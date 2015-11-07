(in-package :cl-bunny)

(defclass librabbitmq-connection (connection)
  ((cl-rabbit-connection :type cl-rabbit::connection
                         :initarg :connection
                         :reader connection-cl-rabbit-connection)))



(defun connection.new (&optional (spec "amqp://") &key shared)
  (let ((connection (make-instance *connection-type* :spec (make-connection-spec spec)
                                                     :connection (cl-rabbit:new-connection))))
    (setup-execute-in-connection-lambda connection)
    connection))

(defun parse-with-connection-params (params)
  (etypecase params
    (string (list params :shared nil))
    (symbol (list params :shared nil))
    (list params)))

(defmacro with-connection (params &body body)
  (destructuring-bind (spec &key shared) (parse-with-connection-params params)
    (with-gensyms (connection-spec-val shared-val)
      `(let* ((,connection-spec-val ,spec)
              (,shared-val ,shared)
              (*connection* (if ,shared-val
                                (find-or-run-new-connection ,connection-spec-val)
                                (run-new-connection ,connection-spec-val))))
         (unwind-protect
              (progn
                ,@body)
           (unless ,shared-val
             (connection.close)))))))

(defun connection.open (&optional (connection *connection*))
  (connection-init connection)
  (setf (slot-value connection 'connection-thread)
        (bt:make-thread (lambda () (connection-loop connection))
                        :name (format nil "CL-BUNNY connection thread. Spec: ~a"
                                      (connection-spec connection))))
  connection)

(defun connection-init (connection)
  (with-slots (cl-rabbit-connection cl-rabbit-socket spec) connection
    (cl-rabbit:socket-open (cl-rabbit:tcp-socket-new cl-rabbit-connection) (connection-spec-host spec) (connection-spec-port spec))
    (handler-bind ((cl-rabbit::rabbitmq-server-error
                     (lambda (error)
                       (when (= 403 (cl-rabbit:rabbitmq-server-error/reply-code error))
                         (error 'authentication-error :connection connection)))))
      (cl-rabbit:login-sasl-plain cl-rabbit-connection
                                  (connection-spec-vhost spec)
                                  (connection-spec-login spec)
                                  (connection-spec-password spec)))))

(defun process-return-method (state method channel)
  (cffi:with-foreign-objects ((message '(:struct cl-rabbit::amqp-message-t)))
    (cl-rabbit::verify-rpc-reply state (channel-id channel) (cl-rabbit::amqp-read-message state (channel-id channel) message 0))
    (let* ((basic-return
             (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                        '(:struct cl-rabbit::amqp-basic-return-t)))
           (message (make-instance 'returned-message
                                   :channel channel
                                   :reply-code (getf basic-return 'cl-rabbit::reply-code)
                                   :reply-text (cl-rabbit::bytes->string (getf basic-return 'cl-rabbit::reply-text))
                                   :exchange (cl-rabbit::bytes->string (getf basic-return 'cl-rabbit::exchange))
                                   :routing-key (cl-rabbit::bytes->string (getf basic-return 'cl-rabbit::routing-key))
                                   :properties (cl-rabbit::load-properties-to-alist
                                                (cffi:foreign-slot-value message
                                                                         '(:struct cl-rabbit::amqp-message-t) 'cl-rabbit::properties))
                                   :body (cl-rabbit::bytes->array
                                          (cffi:foreign-slot-value message
                                                                   '(:struct cl-rabbit::amqp-message-t) 'cl-rabbit::body)))))
      (let* ((exchange (get-registered-exchange channel (message-exchange message)))
             (callback (or (and exchange
                                (exchange-on-return-callback exchange))
                           (exchange-on-return-callback channel))))
        (if callback
            (funcall callback message)
            (log:warn "Got unhandled returned message"))))))

(defun process-ack-method (state method channel)
  (assert (typep channel 'confirm-channel)) ;; TODO: specialize error
  (let ((basic-ack
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-basic-ack-t))))
    (channel.receive channel
                     (make-instance 'amqp-method-basic-ack :delivery-tag (getf basic-ack 'cl-rabbit::delivery-tag)
                                                           :multiple (getf basic-ack 'cl-rabbit::multiple)))))

;;; see https://github.com/alanxz/rabbitmq-c/blob/master/examples/amqp_consumer.c
(defun process-unexpected-frame (connection)
  (let ((cl-rabbit-connection (connection-cl-rabbit-connection connection))
        (channels (connection-channels connection)))
    (cl-rabbit::with-state (state cl-rabbit-connection)
      (cffi:with-foreign-objects ((frame '(:struct cl-rabbit::amqp-frame-t)))
        (let* ((result (cl-rabbit::amqp-simple-wait-frame state frame))
               (result-tag (cl-rabbit::verify-status result)))
          (log:debug "Unexpected frame - channel: ~a, type: ~a"
                     (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::channel)
                     (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::frame-type))
          (when (= cl-rabbit::+amqp-frame-method+
                   (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::frame-type))
            (let* ((channel-id
                     (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::channel))
                   (method
                     (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::payload-method))
                   (method-id
                     (getf method 'cl-rabbit::id)))
              (labels ((warn-nonexistent-channel ()
                         (log:warn "Message received for closed channel: ~a" channel-id)))
                (if-let ((channel (gethash channel-id channels)))
                  (if (channel-open-p channel)
                      (case method-id
                        (#.cl-rabbit::+amqp-basic-ack-method+ (process-ack-method state method channel))
                        (#.cl-rabbit::+amqp-basic-return-method+
                         ;;(cl-rabbit::amqp-put-back-frame state frame)
                         (log:debug "Got return method, reading message")
                         (process-return-method state method channel))
                        (#.cl-rabbit::+amqp-channel-close-method+ (error "Channel close not supported yet"))
                        (#.cl-rabbit::+amqp-connection-close-method+ (error "Connection close not supported yet"))
                        (otherwise (error "Unsupported unexpected method ~a" method-id)))
                      ;; ELSE: We won't deliver messages to a closed channel
                      (log:warn "Incoming message on closed channel: ~s" channel))
                  ;; ELSE: Unused channel
                  (warn-nonexistent-channel)))))
          result-tag)))))

(defun wait-for-frame (connection)
  (log:trace "Waiting for frame on async connection: ~s" connection)
  (with-slots (channels cl-rabbit-connection) connection
    (tagbody
       (handler-bind ((cl-rabbit:rabbitmq-library-error
                        (lambda (condition)
                          (when (eq (cl-rabbit:rabbitmq-library-error/error-code condition)
                                    :amqp-status-unexpected-state)
                            (go unexpected-frame)))))
         ;;
         (let ((envelope (cl-rabbit:consume-message cl-rabbit-connection :timeout 0)))
           (let* ((channel-id (cl-rabbit:envelope/channel envelope)))
             (labels ((warn-nonexistent-channel ()
                        (log:warn "Message received for closed channel: ~a" channel-id)))
               (if-let ((channel (gethash channel-id channels)))
                 (if (channel-open-p channel)
                     (channel-consume-message channel (create-message channel envelope))
                     ;; ELSE: We won't deliver messages to a closed channel
                     (log:warn "Incoming message on closed channel: ~s" channel))
                 ;; ELSE: Unused channel
                 (warn-nonexistent-channel))))
           (go exit)))
     unexpected-frame
       (log:debug "Got unexpected frame")
       (process-unexpected-frame connection)
     exit)))

(defun connection-loop (connection)
  (with-slots (cl-rabbit-connection control-fd control-mailbox event-base) connection
    (iolib:with-event-base (event-base)
      (iolib:set-io-handler event-base
                            control-fd
                            :read (lambda (fd e ex)
                                    (declare (ignorable fd e ex))
                                    (log:debug "Got lambda to execute on connection thread ~a" (eventfd.read control-fd))
                                    (loop for lambda = (dequeue control-mailbox)
                                          while lambda
                                          do (funcall lambda))))
      (iolib:set-io-handler event-base
                            (cl-rabbit::get-sockfd cl-rabbit-connection)
                            :read (lambda (fd e ex)
                                    (declare (ignorable fd e ex))
                                    (wait-for-frame connection)))

      (handler-case
          (loop
            if (or (cl-rabbit::data-in-buffer cl-rabbit-connection)
                   (cl-rabbit::frames-enqueued cl-rabbit-connection))
            do (wait-for-frame connection)
            else
            do (iolib:event-dispatch event-base :one-shot t))
        (stop-connection ()
          (maphash (lambda (id channel) (declare (ignorable id)) (setf (channel-open-p channel) nil)) (connection-channels connection))
          (eventfd.close control-fd)
          (cl-rabbit:destroy-connection cl-rabbit-connection))))))

(defmethod properties->alist ((properties list))
  (properties->alist (apply #'make-instance 'amqp-basic-class-properties properties)))

(defmethod properties->alist ((properties amqp-basic-class-properties))
  (collectors:with-alist-output (add-prop)
    (when (slot-boundp properties 'amqp::content-type)
      (add-prop :content-type (amqp-property-content-type properties)))
    (when (slot-boundp properties 'amqp::content-encoding)
      (add-prop :content-encoding (amqp-property-content-encoding properties)))
    (when (slot-boundp properties 'amqp::headers)
      (add-prop :headers (amqp-property-headers properties)))
    (when (slot-boundp properties 'amqp::delivery-mode)
      (add-prop :delivery-mode (amqp-property-delivery-mode properties)))
    (when (slot-boundp properties 'amqp::priority)
      (add-prop :priority (amqp-property-priority properties)))
    (when (slot-boundp properties 'amqp::correlation-id)
      (add-prop :correlation-id (amqp-property-correlation-id properties)))
    (when (slot-boundp properties 'amqp::reply-to)
      (add-prop :reply-to (amqp-property-reply-to properties)))
    (when (slot-boundp properties 'amqp::expiration)
      (add-prop :expiration (amqp-property-expiration properties)))
    (when (slot-boundp properties 'amqp::message-id)
      (add-prop :message-id (amqp-property-message-id properties)))
    (when (slot-boundp properties 'amqp::timestamp)
      (add-prop :timestamp (amqp-property-timestamp properties)))
    (when (slot-boundp properties 'amqp::type)
      (add-prop :type (amqp-property-type properties)))
    (when (slot-boundp properties 'amqp::user-id)
      (add-prop :user-id (amqp-property-user-id properties)))
    (when (slot-boundp properties 'amqp::app-id)
      (add-prop :app-id (amqp-property-app-id properties)))
    (when (slot-boundp properties 'amqp::cluster-id)
      (add-prop :cluster-id (amqp-property-cluster-id properties)))))

(defmethod connection.send :before ((connection librabbitmq-connection) channel method)
  (when (and (amqp-method-synchronous-p method)
             (amqp-method-field-nowait method))
    (log:warn "Librabbitmq connection: nowait not supported")))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-exchange-declare))
  (cl-rabbit:exchange-declare (connection-cl-rabbit-connection connection)
                              (channel-id channel)
                              (amqp-method-field-exchange method)
                              :type (amqp-method-field-type method)
                              :passive (amqp-method-field-passive method)
                              :durable (amqp-method-field-durable method)
                              :auto-delete (amqp-method-field-auto-delete method)
                              :internal (amqp-method-field-internal method)
                              :arguments (amqp-method-field-arguments method))
  (make-instance 'amqp-method-exchange-declare-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-exchange-delete))
  (cl-rabbit:exchange-delete (connection-cl-rabbit-connection connection)
                             (channel-id channel)
                             (amqp-method-field-exchange method)
                             :if-unused (amqp-method-field-if-unused method))
  (make-instance 'amqp-method-exchange-delete-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-exchange-bind))
  (cl-rabbit:exchange-bind (connection-cl-rabbit-connection connection)
                           (channel-id channel)
                           :destination (amqp-method-field-destination method)
                           :source (amqp-method-field-source method)
                           :routing-key (amqp-method-field-routing-key method)
                           :arguments (amqp-method-field-arguments method))
  (make-instance 'amqp-method-exchange-bind-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-exchange-unbind))
  (cl-rabbit:exchange-unbind (connection-cl-rabbit-connection connection)
                             (channel-id channel)
                             :destination (amqp-method-field-destination method)
                             :source (amqp-method-field-source method)
                             :routing-key (amqp-method-field-routing-key method)
                             :arguments (amqp-method-field-arguments method))
  (make-instance 'amqp-method-exchange-unbind-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-queue-declare))
  (multiple-value-bind (queue message-count consumer-count)
      (cl-rabbit:queue-declare (connection-cl-rabbit-connection connection)
                               (channel-id channel)
                               :queue (amqp-method-field-queue method)
                               :passive (amqp-method-field-passive method)
                               :durable (amqp-method-field-durable method)
                               :exclusive (amqp-method-field-exclusive method)
                               :auto-delete (amqp-method-field-auto-delete method)
                               :arguments (amqp-method-field-arguments method))
    (make-instance 'amqp-method-queue-declare-ok
                   :queue queue
                   :message-count message-count
                   :consumer-count consumer-count)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-queue-bind))
  (cl-rabbit:queue-bind (connection-cl-rabbit-connection connection)
                        (channel-id channel)
                        :queue (amqp-method-field-queue method)
                        :exchange (amqp-method-field-exchange method)
                        :routing-key (amqp-method-field-routing-key method)
                        :arguments (amqp-method-field-arguments method))
  (make-instance 'amqp-method-queue-bind-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-queue-purge))
  (multiple-value-bind (message-count)
      (cl-rabbit:queue-purge (connection-cl-rabbit-connection connection)
                             (channel-id channel)
                             :queue (amqp-method-field-queue method))
    (make-instance 'amqp-method-queue-purge-ok
                   :message-count message-count)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-queue-delete))
  (multiple-value-bind (message-count)
      (cl-rabbit:queue-delete (connection-cl-rabbit-connection connection)
                              (channel-id channel)
                              :queue (amqp-method-field-queue method)
                              :if-unused (amqp-method-field-if-unused method)
                              :if-empty (amqp-method-field-if-empty method))
    (make-instance 'amqp-method-queue-delete-ok
                   :message-count message-count)))


(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-publish))
  (cl-rabbit:basic-publish (connection-cl-rabbit-connection connection)
                           (channel-id channel)
                           :exchange (amqp-method-field-exchange method)
                           :routing-key (amqp-method-field-routing-key method)
                           :mandatory (amqp-method-field-mandatory method)
                           :immediate (amqp-method-field-immediate method)
                           :content (amqp-method-content method)
                           :content-properties (properties->alist (amqp-method-content-properties method))))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-confirm-select))
  (cl-rabbit:confirm-select (connection-cl-rabbit-connection connection)
                            (channel-id channel)))
