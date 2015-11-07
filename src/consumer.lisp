(in-package #:cl-bunny)

(defconstant +consume-receive-timeout+ 1)
(defparameter *ignore-unknown-consumer-tags* nil)

(defclass consumer ()
  ((channel :type channel
            :initarg :channel
            :reader consumer-channel)
   (queue :type queue
          :initarg :queue
          :reader consumer-queue)
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

(defun add-consumer (channel queue tag type lambda)
  (assert (null (gethash tag (channel-consumers channel))) (tag) 'channel-consumer-already-added channel tag)
  (let ((consumer (make-instance 'consumer :channel channel
                                           :queue queue
                                           :type type
                                           :tag tag
                                           :lambda lambda)))
    (setf (gethash tag (channel-consumers channel)) consumer)))

(defun remove-consumer (channel consumer-tag)
  (remhash consumer-tag (channel-consumers channel)))

(defun find-message-consumer (channel message)
  (gethash (message-consumer-tag message) (channel-consumers channel)))

(defun execute-consumer (consumer message)
  (setf (slot-value message 'consumer) consumer)
  (when (eq :cancel (funcall (consumer-lambda consumer) message))
    (unsubscribe consumer)))

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
          do
             (when message
               (dispatch-consumed-message channel message))
             (when one-shot
               (return)))))

;; maybe just (let ((*channel* (consumer-channel consumer))) ... ) in execute-consumer?
(defun wrap-async-subscribe-with-channel (fn channel)
  (lambda (message)
    (let ((*channel* channel))
      (funcall fn message))))

(defun subscribe (queue fn &rest args &key (type :async) consumer-tag no-local no-ack exclusive arguments (channel *channel*))
  (remf args :type)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (let ((*channel* channel))
      (let ((consumer-tag (apply #'amqp-basic-consume% (append (list (queue-name queue)) args))))
        (add-consumer channel queue consumer-tag type (if (eq type :async)
                                                          (wrap-async-subscribe-with-channel fn channel)
                                                          fn))))))

(defun unsubscribe (consumer)
  (if (eq (consumer-type consumer) :async)
      (amqp-basic-cancel-async (consumer-tag consumer) :no-wait t :channel (consumer-channel consumer))
      (amqp-basic-cancel (consumer-tag consumer) :no-wait t))
  (remove-consumer (consumer-channel consumer) (consumer-tag consumer)))
