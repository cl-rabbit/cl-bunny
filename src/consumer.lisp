(in-package #:cl-bunny)

(defvar *consumers*)
(defconstant +consume-receive-timeout+ 1)
(defparameter *ignore-unknown-consumer-tags* nil)

(defmacro with-consumers ((&rest consumers) &body body)
  (assert consumers)
  (let ((consumers (loop for consumer in consumers
                         collect
                            `(setf (gethash (funcall #'amqp-basic-consume ,@(first consumer)) *consumers*) ,(second consumer)))))
    `(let ((*consumers* (make-hash-table :test 'equal)))
       ,@consumers
       ,@body)))

(defun dispatch-consumed-message (message)
  (print message)
  (if-let ((consumer (gethash (message-consumer-tag message) *consumers*)))
    (funcall consumer message)
    (progn
      (log:error "Unknown consumer tag ~a." (message-consumer-tag message))
      (unless *ignore-unknown-consumer-tags*
        (error 'unknown-consumer-error :message message)))))

(defun consume (&optional (channel *channel*))
  ;; what if with-consumers used many channels?
  ;; maybe replace it with flet?
  (let ((mailbox (channel-mailbox channel)))
    (loop for message = (mailbox-receive-message mailbox :timeout +consume-receive-timeout+)
          while message
          do (dispatch-consumed-message message))))
