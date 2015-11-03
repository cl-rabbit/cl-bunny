(in-package :cl-bunny)

(defparameter *connections-pool* (make-hash-table :test #'equal))
;; TODO: maybe replace with synchronized hash-table on sbcl?
(defvar *connections-pool-lock* (bt:make-lock "CL-BUNNY connections pool lock"))

(defvar *connection*)

(defparameter *connection-type* 'librabbitmq-connection)

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
