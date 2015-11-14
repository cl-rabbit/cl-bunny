(in-package :cl-bunny)

(defclass threaded-connection (connection)
  ((event-base :initform (make-instance 'iolib:event-base) :reader connection-event-base :initarg :event-base)
   (control-fd :initform (eventfd:eventfd.new 0))
   (control-mailbox :initform (safe-queue:make-queue) :reader connection-control-mailbox)
   (execute-in-connection-lambda :initform nil :reader connection-lambda)
   (connection-thread :reader connection-thread)))

(defun connection-open-p (connection)
  (and connection
       (slot-boundp connection 'connection-thread)
       (bt:thread-alive-p (connection-thread connection))
       (eq (connection-state connection) :open)))

(defun check-connection-alive (connection)
  (when (connection-open-p connection)
    connection))

(defun setup-execute-in-connection-lambda (connection)
  (with-slots (control-fd control-mailbox execute-in-connection-lambda) connection
    (setf execute-in-connection-lambda
          (lambda (thunk)
            (if (connection-open-p connection)
                (progn (safe-queue:enqueue thunk control-mailbox)
                       (log:debug "Notifying connection thread")
                       (eventfd.notify-1 control-fd))
                (error 'connection-closed-error :connection connection))))))

(defgeneric register-channel (connection channel))

(defmethod register-channel ((connection connection) channel)
  (setf (gethash (channel-id channel) (connection-channels connection)) channel))

(defun connection.register-channel (channel)
  (register-channel (channel-connection channel) channel))

(defgeneric deregister-channel (connection channel))

(defmethod deregister-channel ((connection connection) channel)
  (remhash (channel-id channel) (connection-channels connection))
  (release-channel-id (connection-channel-id-allocator connection) (channel-id channel)))

(defun connection.deregister-channel (channel)
  (deregister-channel (channel-connection channel) channel))

(defgeneric get-channel (connection channel-id))

(defmethod get-channel ((connection connection) channel-id)
  (gethash channel-id (connection-channels connection)))

(defun connection.get-channel (channel-id &key (connection *connection*))
  (get-channel connection channel-id))

(defmacro execute-in-connection-thread ((&optional (connection '*connection*)) &body body)
  `(funcall (connection-lambda ,connection)
            (lambda () ,@body)))

(defmacro execute-in-connection-thread-sync ((&optional (connection '*connection*)) &body body)
  (with-gensyms (lock condition return connection% error)
    `(let ((,lock (bt:make-recursive-lock))
           (,condition (bt:make-condition-variable))
           (,return nil)
           (,connection% ,connection)
           (,error))
       (if (connection-open-p ,connection%)
           (bt:with-recursive-lock-held (,lock)
             (funcall (connection-lambda ,connection%)
                      (lambda (&aux (*connection* ,connection%))
                        (bt:with-recursive-lock-held (,lock)
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
                 (error ,error)
                 (values-list ,return)))
           (error 'connection-closed-error :connection ,connection%)))))

(defun connection.close (&optional (connection *connection*))
  (if (eq (bt:current-thread) (connection-thread connection))
      (throw 'stop-connection (values))
      (progn
        (handler-case
         (progn (execute-in-connection-thread (connection)
                  (connection.close connection))
                (bt:join-thread (connection-thread connection)))
         (connection-closed-error () (log:debug "Closing already closed connection"))))))

(defgeneric connection.send (connection channel method))

(defmethod connection.send :around ((connection connection) channel method)
  (call-next-method))

(defgeneric connection.receive (connection method))

(defmethod connection.receive ((connection connection) (method amqp-method-connection-close))
  (log:debug "Received connection.closed ~a" method)
  (connection.close-ok% connection nil))

(defmethod connection.receive ((connection connection) (method amqp-method-connection-blocked))
  (log:error "Connection blocked ~a" method))

(defmethod connection.receive ((connection connection) (method amqp-method-connection-unblocked))
  (log:error "Connection unblocked ~a" method))
