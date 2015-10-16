(in-package :cl-bunny)

(defparameter *connections-pool* (make-hash-table :test #'equal))
;; TODO: maybe replace with synchronized hash-table on sbcl?
(defvar *connections-pool-lock* (bt:make-lock "CL-BUNNY connections pool lock"))

(defvar *connection*)

(defun connection-alive-p (connection)
  (and connection
       (slot-boundp connection 'connection-thread)
       (bt:thread-alive-p (connection-thread connection))))

(defun check-connection-alive (connection)
  (when (connection-alive-p connection)
    connection))

(defun get-connection-from-pool (spec)
  (check-connection-alive (gethash spec *connections-pool*)))

(defun add-connection-to-pool (spec connection)
  (setf (gethash spec *connections-pool*)
        connection))

(defun remove-connection-from-pool (connection)
  (bt:with-lock-held (*connections-pool-lock*)
    (remhash (connection-spec connection) *connections-pool*)))

(defun find-or-run-new-connection (spec)
  (bt:with-lock-held (*connections-pool-lock*)
    (or (get-connection-from-pool spec)
        (run-new-connection spec))))

(defun run-new-connection (spec)
  (let ((connection (new-connection spec)))
    (add-connection-to-pool spec connection)
    (connection-start connection)
    connection))

(defclass connection ()
  ((spec :initarg :spec :reader connection-spec)

   (cl-rabbit-connection          :type cl-rabbit::connection
                                  :initarg :connection
                                  :reader connection-cl-rabbit-connection)
   (channel-id-allocator :type channel-id-allocator
                         :initform (new-channel-id-allocator +max-channels+)
                         :reader connection-channel-id-allocator)

   (channels :type hash-table
             :initform (make-hash-table :synchronized t)
             :reader connection-channels)

   (event-base :initform (make-instance 'iolib:event-base) :reader connection-event-base :initarg :event-base)

   (control-fd :initform (eventfd:eventfd.new 0))
   (control-mailbox :initform (make-queue) :reader connection-control-mailbox)
   (execute-in-connection-lambda :initform nil :reader connection-lambda)

   (connection-thread :reader connection-thread)))

(defun setup-execute-in-connection-lambda (connection)
  (with-slots (control-fd control-mailbox execute-in-connection-lambda) connection
    (setf execute-in-connection-lambda
          (lambda (thunk)
            (enqueue thunk control-mailbox)
            (log:debug "Notifying connection thread")
            (eventfd.notify-1 control-fd)))))

(defmacro execute-in-connection-thread ((&optional (connection '*connection*)) &body body)
  `(funcall (connection-lambda ,connection)
            (lambda () ,@body)))

(defmacro execute-in-connection-thread-sync ((&optional (connection '*connection*)) &body body)
  (with-gensyms (lock condition return connection% error)
    `(let ((,lock (bt:make-lock))
           (,condition (bt:make-condition-variable))
           (,return nil)
           (,connection% ,connection)
           (,error))
       (if (connection-alive-p ,connection%)
           (bt:with-lock-held (,lock)
             (funcall (connection-lambda ,connection%)
                      (lambda (&aux (*connection* ,connection%))
                        (bt:with-lock-held (,lock)
                          (handler-case
                              (setf ,return
                                    (multiple-value-list
                                     (unwind-protect
                                          (progn
                                            ,@body)
                                       (bt:condition-notify ,condition))))
                            (cl-rabbit::rabbitmq-server-error (e)
                              (log:error "Server error: ~a" e)
                              (setf ,error e))))))
             (bt:condition-wait ,condition ,lock)
             (if ,error
                 (error (cl-rabbit-error-to-cl-bunny-error ,error))
                 (values-list ,return)))
           (error 'connection-closed :connection ,connection%)))))

(defun new-connection (&optional (spec "amqp://"))
  (let ((connection (make-instance 'connection :spec (make-connection-spec spec)
                                               :connection (cl-rabbit:new-connection))))
    (setup-execute-in-connection-lambda connection)
    connection))

(defun parse-with-connection-params (params)
  (etypecase params
    (string (list params :one-shot nil))
    (symbol (list params :one-shot nil))
    (list params)))

(defmacro with-connection (params &body body)
  (destructuring-bind (spec &key one-shot) (parse-with-connection-params params)
    (with-gensyms (connection-spec-val one-shot-val)
      `(let* ((,connection-spec-val ,spec)
              (,one-shot-val ,one-shot)
              (*connection* (if ,one-shot-val
                                (run-new-connection ,connection-spec-val)
                                (find-or-run-new-connection ,connection-spec-val))))
         (unwind-protect
              (progn
                ,@body)
           (when ,one-shot-val
             (connection-close)))))))

(defun connection-start (connection)
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
                        (#.cl-rabbit::+amqp-basic-ack-method+ (error "Ack not supported yet"))
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
          (eventfd.close control-fd)
          (cl-rabbit:destroy-connection cl-rabbit-connection))))))

(defun connection-close (&optional (connection *connection*))
  (when (connection-alive-p connection)
    (execute-in-connection-thread (connection)
      (error 'stop-connection))
    (bt:join-thread (connection-thread connection)))
  (remove-connection-from-pool connection))

(defun allocate-and-open-new-channel (connection)
  (let ((channel (new-channel connection)))
    (channel-open channel)))

(defun parse-with-channel-params (params)
  (etypecase params
    (string (list params :close t))
    (symbol (list params :close t))
    (list params)))

(defmacro with-channel (params &body body)
  (destructuring-bind (channel &key close) (parse-with-channel-params params)
    (with-gensyms (allocated-p channel-val close-val)
      `(let ((,channel-val ,channel)
             (,close-val ,close))
         (multiple-value-bind (*channel* ,allocated-p) (if ,channel-val
                                                           ,channel-val
                                                           (values
                                                            (allocate-and-open-new-channel *connection*)
                                                            t))
           (unwind-protect
                (progn
                  ,@body)
             (when (and ,close-val ,allocated-p)
               (amqp-channel-close *channel*))))))))
