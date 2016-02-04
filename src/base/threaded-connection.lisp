(in-package :cl-bunny)

(defparameter *force-timeout* nil)

(defstruct connection-state-lock
  (rlock (bt:make-lock    "rw-lcok-rlock"))
  (rtrylock (bt:make-lock "rw-lock-rtrylock"))
  (resource (bt:make-lock "rw-lock-resource"))
  (rcount 0 :type (unsigned-byte 32)))

(defun read-lock-begin (connection-state-lock)
  (declare (optimize (speed 3) (debug 0) (safety 0)))
  (bt:acquire-lock (connection-state-lock-rtrylock connection-state-lock))
  (bt:acquire-lock (connection-state-lock-rlock connection-state-lock))
  (when (= 1 (incf (connection-state-lock-rcount connection-state-lock)))
    (bt:acquire-lock (connection-state-lock-resource connection-state-lock)))
  (bt:release-lock (connection-state-lock-rlock connection-state-lock))
  (bt:release-lock (connection-state-lock-rtrylock connection-state-lock)))

(defun read-lock-end (connection-state-lock)
  (declare (optimize (speed 3) (debug 0) (safety 0)))
  (bt:acquire-lock (connection-state-lock-rlock connection-state-lock))
  (when (= 0 (decf (connection-state-lock-rcount connection-state-lock)))
    (bt:release-lock (connection-state-lock-resource connection-state-lock)))
  (bt:release-lock (connection-state-lock-rlock connection-state-lock)))

(defmacro with-read-lock (connection-state-lock &body body)
  (with-gensyms (rwlock%)
    `(let ((,rwlock% ,connection-state-lock))
       (read-lock-begin ,rwlock%)
       (unwind-protect
            (progn ,@body)
         (read-lock-end ,rwlock%)))))

(defun write-lock-begin (connection-state-lock)
  (declare (optimize (speed 3) (debug 0) (safety 0)))
  (bt:acquire-lock (connection-state-lock-rtrylock connection-state-lock))
  (bt:acquire-lock (connection-state-lock-resource connection-state-lock)))

(defun write-lock-end (connection-state-lock)
  (declare (optimize (speed 3) (debug 0) (safety 0)))
  (bt:release-lock (connection-state-lock-resource connection-state-lock))
  (bt:release-lock (connection-state-lock-rtrylock connection-state-lock)))

(defmacro with-write-lock (connection-state-lock &body body)
  (with-gensyms (rwlock%)
    `(let ((,rwlock% ,connection-state-lock))
       (write-lock-begin ,rwlock%)
       (unwind-protect
            (progn ,@body)
         (write-lock-end ,rwlock%)))))

(defclass threaded-connection (connection)
  ((control-fd)
   (control-mailbox :reader connection-control-mailbox)
   (state-lock :initform (bt:make-lock) :reader connection-state-lock)
   (execute-in-connection-lambda :initform nil :reader connection-lambda)
   (connection-thread :reader connection-thread)))

(defmethod connection-open-p% ((connection threaded-connection))
  (and connection
       (slot-boundp connection 'connection-thread)
       (bt:thread-alive-p (connection-thread connection))
       (eq (channel-state connection) :open)))

(defun setup-execute-in-connection-lambda (connection)
  (with-slots (control-fd control-mailbox execute-in-connection-lambda) connection
    (setf execute-in-connection-lambda
          (lambda (thunk)
            (bt:with-lock-held ((connection-state-lock connection))
              (if (connection-open-p connection)
                  (progn (safe-queue:enqueue thunk control-mailbox)
                         (log:debug "Notifying connection thread")
                         (eventfd.notify-1 control-fd))
                  (error 'connection-closed-error :connection connection)))))))

(defmacro execute-in-connection-thread ((&optional (connection '*connection*)) &body body)
  `(funcall (connection-lambda ,connection)
            (lambda () ,@body)))

(defmacro send-to-connection-thread ((&optional (connection '*connection*)) &body body)
  `(funcall (connection-lambda ,connection)
            (progn ,@body)))

(defgeneric connection-init (connection))
(defgeneric connection-loop (connection promise))

(defmethod connection.open% ((connection threaded-connection))
  (connection.init connection)
  (let ((promise (make-sync-promise)))    
    (setf (slot-value connection 'connection-thread)
          (bt:make-thread (lambda () (connection-loop connection promise))
                          :name (format nil "CL-BUNNY connection thread. Spec: ~a"
                                        (connection-spec connection))))
    (promise.force promise :timeout *force-timeout*))
  connection)

(defun execute-on-connection-thread-impl (connection promise lambda channel)
  (execute-in-connection-thread (connection)
    (handler-case
        (promise.resolve promise (funcall lambda))
      (amqp-channel-error (e)
        (log:debug "~a" e)
        (channel.receive channel (make-instance 'amqp-method-channel-close
                                                :method-id (amqp::amqp-error-method e)
                                                :class-id (amqp::amqp-error-class e)
                                                :reply-text (amqp::amqp-error-reply-text e)
                                                :reply-code (amqp::amqp-error-reply-code e)))
        (promise.reject promise e))
      (amqp-connection-error (e)
        (log:debug "~a" e)
        (throw 'stop-connection
          (lambda ()
            (promise.reject promise e))))
      (transport-error (e)
        (log:debug "~a" e)
        (throw 'stop-connection
          (lambda ()
            (promise.reject promise e))))
      (error (e)
        (log:debug "~a" e)
        (promise.reject promise e)))))

(defmethod execute-on-connection-thread ((connection threaded-connection) channel lambda)
  (if (eq (bt:current-thread) (connection-thread connection))
      (call-next-method)
      (progn
        (bt:with-lock-held ((connection-state-lock connection))
          (assert (connection-open-p connection) () 'connection-closed-error :connection connection))
        (if *notification-lambda*
            (let ((promise (make-async-promise *notification-lambda*)))
              (execute-on-connection-thread-impl connection promise lambda channel)
              promise)
            (let ((promise (make-sync-promise)))
              (execute-on-connection-thread-impl connection promise lambda channel)
              (promise.force promise :timeout *force-timeout*))))))


(defmacro connection-execute (connection channel &body body)
  `(execute-on-connection-thread ,connection ,channel
                                 (lambda () ,@body)))

(defmacro execute-in-connection-thread-sync ((&optional (connection '*connection*)) &body body)
  `(execute-on-connection-thread ,connection nil
                                 (lambda () ,@body)))
