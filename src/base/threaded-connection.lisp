(in-package :cl-bunny)

(defclass threaded-connection (connection)
  ((control-fd)
   (control-mailbox :reader connection-control-mailbox)
   (state-lock :initform (bt:make-recursive-lock) :reader connection-state-lock)
   (execute-in-connection-lambda :initform nil :reader connection-lambda)
   (on-connect-promise)
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
            (bt:with-recursive-lock-held ((connection-state-lock connection))
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
(defgeneric connection-loop (connection))

(defmethod connection.open% ((connection threaded-connection))
  (connection.init connection)
  (let ((promise (make-sync-promise)))
    (setf (slot-value connection 'on-connect-promise) promise
     (slot-value connection 'connection-thread)
          (bt:make-thread (lambda () (connection-loop connection))
                          :name (format nil "CL-BUNNY connection thread. Spec: ~a"
                                        (connection-spec connection))))
    (promise.force promise :timeout *force-timeout*))
  connection)
