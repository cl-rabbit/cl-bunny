(in-package :cl-bunny)

(defvar *connection*)

(defparameter *connection-type* 'librabbitmq-connection)

(defclass connection ()
  ((spec :initarg :spec :reader connection-spec)
   (channel-id-allocator :type channel-id-allocator
                         :initform (new-channel-id-allocator +max-channels+)
                         :reader connection-channel-id-allocator)

   (channels :type hash-table
             :initform (make-hash-table :synchronized t)
             :reader connection-channels)

   (pool :initform nil :accessor connection-pool)

   (event-base :initform (make-instance 'iolib:event-base) :reader connection-event-base :initarg :event-base)
   (control-fd :initform (eventfd:eventfd.new 0))
   (control-mailbox :initform (make-queue) :reader connection-control-mailbox)
   (execute-in-connection-lambda :initform nil :reader connection-lambda)
   (connection-thread :reader connection-thread)))

(defun connection-open-p (connection)
  (and connection
       (slot-boundp connection 'connection-thread)
       (bt:thread-alive-p (connection-thread connection))))

(defun check-connection-alive (connection)
  (when (connection-open-p connection)
    connection))

(defun run-new-connection (spec)
  (connection.open (connection.new spec)))

(defun setup-execute-in-connection-lambda (connection)
  (with-slots (control-fd control-mailbox execute-in-connection-lambda) connection
    (setf execute-in-connection-lambda
          (lambda (thunk)
            (enqueue thunk control-mailbox)
            (log:debug "Notifying connection thread")
            (eventfd.notify-1 control-fd)))))

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
  (when (connection-open-p connection)
    (progn
      (execute-in-connection-thread (connection)
        (throw 'stop-connection (values)))
      (bt:join-thread (connection-thread connection)))))

(defun connection.close-ok% (connection callback)
  (connection.send connection connection (make-instance 'amqp-method-connection-close-ok))
  (throw 'stop-connection callback)
  t)

(defgeneric connection.send (connection channel method))

(defmethod connection.send :around ((connection connection) channel method)
  (call-next-method))
