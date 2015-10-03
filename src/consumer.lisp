(in-package #:cl-bunny)

(defconstant +consume-receive-timeout+ 1)
(defparameter *ignore-unknown-consumer-tags* nil)

(defclass consumer ()
  ((channel :type channel
            :initarg :channel
            :reader consumer-channel)
   (type :type (or :sync :async)
         :initarg :type
         :initform :sync
         :reader consumer-type)
   (tag :type string
        :initarg :tag
        :reader consumer-tag)
   (lambda :type function
      :initarg :lambda
      :reader consumer-lambda)))

(defmacro with-consumers ((&rest consumers) &body body)
  (assert consumers)
  (with-gensyms (new-consumers)
    (let ((consumers (loop for consumer in consumers
                           collect
                              `(apply #'subscribe (list ,@consumer)))))
      `(let ((,new-consumers (list 
                             ,@consumers)))
         (unwind-protect
              (progn ,@body)
           (loop for consumer in ,new-consumers do
                    (unsubscribe consumer)))))))

(defun add-consumer (channel tag type lambda)
  (assert (null (gethash tag (channel-consumers channel))) (tag) 'channel-consumer-already-added channel tag)
  (let ((consumer (make-instance 'consumer :channel channel
                                           :type type
                                           :tag tag
                                           :lambda lambda)))
    (setf (gethash tag (channel-consumers channel)) consumer)))

(defun remove-consumer (channel consumer-tag)
  (remhash consumer-tag (channel-consumers channel)))  

(defun find-message-consumer (channel message)
  (gethash (message-consumer-tag message) (channel-consumers channel)))

(defun execute-consumer (consumer message)
  (funcall (consumer-lambda consumer) message))

(defun dispatch-consumed-message (channel message)
  (if-let ((consumer (find-message-consumer channel message)))
    (execute-consumer consumer message)
    (progn
      (log:error "Unknown consumer tag ~a." (message-consumer-tag message))
      (unless *ignore-unknown-consumer-tags*
        (error 'unknown-consumer-error :message message)))))

(defun consume (&key (channel *channel*) (timeout +consume-receive-timeout+) one-shot)
  ;; what if with-consumers used many channels?
  ;; maybe replace it with flet?
  (let ((mailbox (channel-mailbox channel)))
    (loop for message = (mailbox-receive-message mailbox :timeout timeout)
          while message
          do
             (progn (dispatch-consumed-message channel message)
                    (when one-shot
                      (return))))))

(defun subscribe (queue fn &rest args &key (type :async) consumer-tag no-local no-ack exclusive arguments (channel *channel*))
  (remf args :type)
  (let ((consumer-tag (apply #'amqp-basic-consume (append (list queue) args))))
    (add-consumer channel consumer-tag type fn)))

(defun unsubscribe (consumer)
  (amqp-basic-cancel  (consumer-tag consumer) :no-wait t)
  (remove-consumer (consumer-channel consumer) (consumer-tag consumer)))
