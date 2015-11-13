(in-package :cl-bunny)

(defclass librabbitmq-connection (connection)
  ((cl-rabbit-connection :type cl-rabbit::connection
                         :initarg :connection
                         :reader connection-cl-rabbit-connection)))

(defun librabbitmq-error->transport-error (e)
  (let ((code (cl-rabbit:rabbitmq-library-error/error-code e))
        (description (cl-rabbit:rabbitmq-library-error/error-description e)))
    (case code
      (:amqp-status-unknown-class (error 'amqp-unknown-method-class-error))
      (:amqp-status-unknown-method (error 'amqp-unknown-method-error))
      (:amqp-status-hostname-resolution-failed (error 'transport-error))
      (:amqp-status-incompatible-amqp-version (error 'transport-error))
      (:amqp-status-connection-closed (error 'connection-closed-error))
      (:amqp-status-bad-url (error 'transport-error))
      (:amqp-status-socket-error (error 'network-error))
      (:amqp-status-invalid-parameter (error 'transport-error))
      (:amqp-status-table-too-big (error 'transport-error))
      (:amqp-status-wrong-method (error 'transport-error))
      (:amqp-status-timeout (error 'transport-error))
      (:amqp-status-timer-failure (error 'transport-error))
      (:amqp-status-heartbeat-timeout (error 'network-error))
      (:amqp-status-unexpected-state (error 'transport-error))
      (:amqp-status-tcp-error (error 'network-error))
      (:amqp-status-tcp-socketlib-init-error (error 'network-error))
      (:amqp-status-ssl-error (error 'network-error))
      (:amqp-status-ssl-hostname-verify-failed (error 'network-error))
      (:amqp-status-ssl-peer-verify-failed (error 'network-error))
      (:amqp-status-ssl-connection-failed (error 'network-error)))
    (error e)))

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
           (when (and (not ,shared-val))
             (connection.close)))))))

(defun connection.open (&optional (connection *connection*))
  (connection-init connection)
  (setf (slot-value connection 'connection-thread)
        (bt:make-thread (lambda () (connection-loop connection))
                        :name (format nil "CL-BUNNY connection thread. Spec: ~a"
                                      (connection-spec connection))))
  connection)

(defun connection-init (connection)
  (handler-case
      (with-slots (cl-rabbit-connection cl-rabbit-socket spec) connection
        (cl-rabbit:socket-open (cl-rabbit:tcp-socket-new cl-rabbit-connection) (connection-spec-host spec) (connection-spec-port spec))
        (handler-bind ((cl-rabbit::rabbitmq-server-error
                         (lambda (error)
                           (when (= 403 (cl-rabbit:rabbitmq-server-error/reply-code error))
                             (error 'authentication-error :connection connection)))))
          (cl-rabbit:login-sasl-plain cl-rabbit-connection
                                      (connection-spec-vhost spec)
                                      (connection-spec-login spec)
                                      (connection-spec-password spec)
                                      :properties '(("product" . "cl-bunny(cl-rabbit transport)")
                                                    ("version" . "0.1")
                                                    ("copyright" . "Copyright (c) 2015 Ilya Khaprov <ilya.khaprov@publitechs.com> and CONTRIBUTORS <https://github.com/cl-rabbit/cl-bunny/blob/master/CONTRIBUTORS.md>")
                                                    ("information" . "see https://github.com/cl-rabbit/cl-bunny")
                                                    ("capabilities" . (("connection.blocked" . nil)))))
          (setf (slot-value connection 'state) :open)))
    (cl-rabbit:rabbitmq-library-error ()
      (error 'transport-error))))

(defun process-basic-ack-method (connection method channel)
  (declare (ignore connection))
  (assert (typep channel 'confirm-channel)) ;; TODO: specialize error
  (let ((basic-ack
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-basic-ack-t))))
    (channel.receive channel
                     (make-instance 'amqp-method-basic-ack :delivery-tag (getf basic-ack 'cl-rabbit::delivery-tag)
                                                           :multiple (getf basic-ack 'cl-rabbit::multiple)))))

(defun process-basic-return-method (connection method channel)
  (let ((cl-rabbit-connection (connection-cl-rabbit-connection connection)))
    (cl-rabbit::with-state (state cl-rabbit-connection)
      (cffi:with-foreign-objects ((message '(:struct cl-rabbit::amqp-message-t)))
        (log:debug "Got return method, reading message")
        (cl-rabbit::verify-rpc-reply state (channel-id channel) (cl-rabbit::amqp-read-message state (channel-id channel) message 0))
        (let* ((basic-return
                 (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                            '(:struct cl-rabbit::amqp-basic-return-t)))
               (method (make-instance 'amqp-method-basic-return
                                      :reply-code (getf basic-return 'cl-rabbit::reply-code)
                                      :reply-text (cl-rabbit::bytes->string (getf basic-return 'cl-rabbit::reply-text))
                                      :exchange (cl-rabbit::bytes->string (getf basic-return 'cl-rabbit::exchange))
                                      :routing-key (cl-rabbit::bytes->string (getf basic-return 'cl-rabbit::routing-key))
                                      :content-properties (apply #'make-instance 'amqp-basic-class-properties
                                                                 (cl-rabbit::load-properties-to-plist
                                                                  (cffi:foreign-slot-value message
                                                                                           '(:struct cl-rabbit::amqp-message-t) 'cl-rabbit::properties)))
                                      :content (cl-rabbit::bytes->array
                                                (cffi:foreign-slot-value message
                                                                         '(:struct cl-rabbit::amqp-message-t) 'cl-rabbit::body)))))
          (channel.receive channel method))))))

(defun process-channel-close-method (connection method channel)
  (declare (ignore connection))
  (let ((channel-close
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-channel-close-t))))
    (channel.receive channel
                     (make-instance 'amqp-method-channel-close :reply-code (getf channel-close 'cl-rabbit::reply-code)
                                                               :reply-text (getf channel-close 'cl-rabbit::reply-text)
                                                               :class-id (getf channel-close 'cl-rabbit::class-id)
                                                               :method-id (getf channel-close 'cl-rabbit::method-id)))))

(defun process-connection-close-method (connection method)
  (let ((connection-close
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-connection-close-t))))
    (connection.receive connection
                        (make-instance 'amqp-method-connection-close :reply-code (getf connection-close 'cl-rabbit::reply-code)
                                                                     :reply-text (cl-rabbit::bytes->string (getf connection-close 'cl-rabbit::reply-text))
                                                                     :class-id (getf connection-close 'cl-rabbit::class-id)
                                                                     :method-id (getf connection-close 'cl-rabbit::method-id)))))

(defun process-connection-blocked-method (connection method)
  (let ((connection-blocked
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-connection-blocked-t))))
    (connection.receive connection
                        (make-instance 'amqp-method-connection-blocked :reason (cl-rabbit::bytes->string (getf connection-blocked 'cl-rabbit::reason))))))

(defun process-connection-unblocked-method (connection method)
  (declare (ignore method))
  (connection.receive connection
                      (make-instance 'amqp-method-connection-unblocked)))

;;; see https://github.com/alanxz/rabbitmq-c/blob/master/examples/amqp_consumer.c
(defun process-unexpected-frame (connection)
  (let ((cl-rabbit-connection (connection-cl-rabbit-connection connection)))
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
                (case method-id
                  (#.cl-rabbit::+amqp-connection-close-method+ (process-connection-close-method connection method))
                  (#.cl-rabbit::+amqp-connection-blocked-method+ (process-connection-blocked-method connection method))
                  (#.cl-rabbit::+amqp-connection-unblocked-method+ (process-connection-unblocked-method connection method))
                  (otherwise
                   (if-let ((channel (connection.get-channel channel-id :connection connection)))
                     (if (channel-open-p% channel)
                         (case method-id
                           (#.cl-rabbit::+amqp-basic-ack-method+ (process-basic-ack-method connection method channel))
                           (#.cl-rabbit::+amqp-basic-return-method+  (process-basic-return-method connection method channel))
                           (#.cl-rabbit::+amqp-channel-close-method+ (process-channel-close-method connection method channel))
                           (otherwise (error "Unsupported unexpected method ~a" method-id)))
                         ;; ELSE: We won't deliver messages to a closed channel
                         (log:warn "Incoming message on closed channel: ~s" channel))
                     ;; ELSE: Unused channel
                     (warn-nonexistent-channel)))))))
          result-tag)))))


(defun create-deliver-method-from-cl-rabbit-envelope (envelope)
  (make-instance 'amqp-method-basic-deliver
                 :consumer-tag (cl-rabbit:envelope/consumer-tag envelope)
                 :delivery-tag (cl-rabbit:envelope/delivery-tag envelope)
                 :redelivered (cl-rabbit:envelope/redelivered envelope)
                 :exchange (cl-rabbit:envelope/exchange envelope)
                 :routing-key (cl-rabbit:envelope/routing-key envelope)
                 :content-properties (cl-rabbit:message/properties (cl-rabbit:envelope/message envelope))
                 :content (cl-rabbit:message/body (cl-rabbit:envelope/message envelope))))

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
                 (if (channel-open-p% channel)
                     (channel.receive channel (create-deliver-method-from-cl-rabbit-envelope envelope))
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
                                    (eventfd.read control-fd)
                                    (log:debug "Got lambda to execute on connection thread")
                                    (loop for lambda = (safe-queue:dequeue control-mailbox)
                                          while lambda
                                          do (funcall lambda))))
      (iolib:set-io-handler event-base
                            (cl-rabbit::get-sockfd cl-rabbit-connection)
                            :read (lambda (fd e ex)
                                    (declare (ignorable fd e ex))
                                    (wait-for-frame connection)))
      (let ((ret))
        (unwind-protect
             (setf ret
                   (catch 'stop-connection
                     (handler-bind ((cl-rabbit:rabbitmq-library-error
                                      (lambda (e)
                                        (let ((actual-error (librabbitmq-error->transport-error e)))
                                          (log:error "Unhandled transport error: ~a" actual-error)
                                          (throw 'stop-connection actual-error))))
                                    (error
                                      (lambda (e)
                                        (log:error "Unhandled unknown error: ~a" e)
                                        (throw 'stop-connection e))))
                       (loop
                         if (or (cl-rabbit::data-in-buffer cl-rabbit-connection)
                                (cl-rabbit::frames-enqueued cl-rabbit-connection))
                         do (wait-for-frame connection)
                         else
                         do (iolib:event-dispatch event-base :one-shot t)))))
          (setf (slot-value connection 'state) :closing)
          (eventfd.close control-fd)
          (log:debug "Stopping AMQP connection")
          (when (connection-pool connection)
            (remove-connection-from-pool connection))
          (maphash (lambda (id channel) (declare (ignorable id)) (setf (channel-open-p% channel) nil)) (connection-channels connection))
          (log:debug "closed-all-channels")
          ;; drain control mailbox
          (loop for lambda = (safe-queue:dequeue control-mailbox)
                while lambda
                do (funcall lambda))
          (log:debug "queue drained")
          (setf (slot-value connection 'state) :closed)
          (cl-rabbit:destroy-connection cl-rabbit-connection)
          (if (functionp ret)
              (funcall ret)))))))

(defmethod connection.close-ok% ((connection librabbitmq-connection) callback)
  (connection.send connection connection (make-instance 'amqp-method-connection-close-ok))
  (throw 'stop-connection callback)
  t)

(defmethod properties->alist ((properties list))
  (let ((properties (copy-list properties)))
    (when (find :persistent properties)
      (setf (getf properties :delivery-mode) (if (getf properties :persistent)
                                                 2
                                                 1))
      (remf properties :persistent))
    (properties->alist (apply #'make-instance 'amqp-basic-class-properties properties))))

(defgeneric reply-to (queue))

(defmethod properties->alist ((properties amqp-basic-class-properties))
  (collectors:with-alist-output (add-prop)
    (when (slot-boundp properties 'amqp::content-type)
      (add-prop :content-type (amqp-property-content-type properties)))
    (when (slot-boundp properties 'amqp::content-encoding)
      (add-prop :content-encoding (amqp-property-content-encoding properties)))
    (when (slot-boundp properties 'amqp::headers)
      (add-prop :headers (amqp-property-headers properties)))
    (when (slot-boundp properties 'amqp::delivery-mode)
      (add-prop :persistent (ecase (amqp-property-delivery-mode properties)
                              (2 t)
                              (1 nil))))
    (when (slot-boundp properties 'amqp::priority)
      (add-prop :priority (amqp-property-priority properties)))
    (when (slot-boundp properties 'amqp::correlation-id)
      (add-prop :correlation-id (amqp-property-correlation-id properties)))
    (when (slot-boundp properties 'amqp::reply-to)
      (add-prop :reply-to (reply-to (amqp-property-reply-to properties))))
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
             (ignore-errors (amqp-method-field-nowait method)))
    (log:warn "Librabbitmq connection: nowait not supported")))

(defmethod connection.send :around ((connection librabbitmq-connection) channel method)
  (log:trace "connection: ~a, channel: ~a, method: ~a" connection channel method)
  (unless (or (typep method 'amqp-method-connection-close)
              (typep method 'amqp-method-channel-close))
    (assert (connection-open-p (channel-connection channel)) () 'connection-closed-error :connection (channel-connection channel)))
  (unless (or (typep method 'amqp-method-channel-open)
              (typep method 'amqp-method-channel-close))
    (assert (channel-open-p% channel) () 'channel-closed-error :channel channel))
  ;; convert close replies for sync methods
  (handler-case
      (call-next-method)
    (cl-rabbit:rabbitmq-server-error (e)
      (error (amqp-error-type-from-reply-code (cl-rabbit:rabbitmq-server-error/reply-code e))
             :reply-code (cl-rabbit:rabbitmq-server-error/reply-code e)
             :reply-text (cl-rabbit:rabbitmq-server-error/reply-text e)
             :connection connection
             :channel channel
             :class-id (cl-rabbit:rabbitmq-server-error/class-id e)
             :method-id (cl-rabbit:rabbitmq-server-error/method-id e)))
    (cl-rabbit:rabbitmq-library-error (e)
      (librabbitmq-error->transport-error e))))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-connection-close-ok))
  (declare (ignore channel))
  (cl-rabbit:connection-close-ok (connection-cl-rabbit-connection connection)
                                 0))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-open))
  (cl-rabbit:channel-open (connection-cl-rabbit-connection connection)
                          (channel-id channel))
  (make-instance 'amqp-method-channel-open-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-flow))
  (multiple-value-bind (active)
      (cl-rabbit:channel-flow (connection-cl-rabbit-connection connection)
                              (channel-id channel)
                              (amqp-method-field-active method))
    (make-instance 'amqp-method-channel-flow-ok
                   :active active)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-flow-ok))
  (cl-rabbit:channel-flow-ok (connection-cl-rabbit-connection connection)
                             (channel-id channel)
                             (amqp-method-field-active method)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-close))
  (assert (equal (amqp-method-field-reply-text method) "") nil 'error "librabbitmq channel.close only supports reply-code") ;; TODO: specialize error
  (assert (= (amqp-method-field-class-id method) 0) nil 'error "librabbitmq channel.close only supports reply-code") ;; TODO: specialize error
  (assert (= (amqp-method-field-method-id method) 0) nil 'error "librabbitmq channel.close only supports reply-code") ;; TODO: specialize error
  (cl-rabbit:channel-close (connection-cl-rabbit-connection connection)
                           (channel-id channel)
                           :reply-code (amqp-method-field-reply-code method))
  (make-instance 'amqp-method-channel-close-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-close-ok))
  (cl-rabbit:channel-close-ok (connection-cl-rabbit-connection connection)
                              (channel-id channel)))

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

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-qos))
  (cl-rabbit:basic-qos (connection-cl-rabbit-connection connection)
                       (channel-id channel)
                       (amqp-method-field-prefetch-size method)
                       (amqp-method-field-prefetch-count method)
                       :global (amqp-method-field-global method))
  (make-instance 'amqp-method-basic-qos-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-consume))
  (multiple-value-bind (consumer-tag)
      (cl-rabbit:basic-consume (connection-cl-rabbit-connection connection)
                               (channel-id channel)
                               (amqp-method-field-queue method)
                               :consumer-tag (amqp-method-field-consumer-tag method)
                               :no-local (amqp-method-field-no-local method)
                               :no-ack (amqp-method-field-no-ack method)
                               :exclusive (amqp-method-field-exclusive method)
                               :arguments (amqp-method-field-arguments method))
    (make-instance 'amqp-method-basic-consume-ok
                   :consumer-tag consumer-tag)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-cancel))
  (multiple-value-bind (consumer-tag)
      (cl-rabbit:basic-cancel (connection-cl-rabbit-connection connection)
                              (channel-id channel)
                              (amqp-method-field-consumer-tag method))
    (make-instance 'amqp-method-basic-cancel-ok
                   :consumer-tag (amqp-method-field-consumer-tag method);; I'm lazy here because librabbitmq is sync anyway
                   )))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-ack))
  (cl-rabbit:basic-ack (connection-cl-rabbit-connection connection)
                       (channel-id channel)
                       (amqp-method-field-delivery-tag method)
                       :multiple (amqp-method-field-multiple method)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-nack))
  (cl-rabbit:basic-nack (connection-cl-rabbit-connection connection)
                            (channel-id channel)
                            (amqp-method-field-delivery-tag method)
                            :multiple (amqp-method-field-multiple method)
                            :requeue (amqp-method-field-requeue method)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-confirm-select))
  (cl-rabbit:confirm-select (connection-cl-rabbit-connection connection)
                            (channel-id channel))
  (make-instance 'amqp-method-confirm-select-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-tx-select))
  (cl-rabbit:tx-select (connection-cl-rabbit-connection connection)
                       (channel-id channel))
  (make-instance 'amqp-method-tx-select-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-tx-commit))
  (cl-rabbit:tx-commit (connection-cl-rabbit-connection connection)
                       (channel-id channel))
  (make-instance 'amqp-method-tx-commit-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-tx-rollback))
  (cl-rabbit:tx-rollback (connection-cl-rabbit-connection connection)
                         (channel-id channel))
  (make-instance 'amqp-method-tx-rollback-ok))
