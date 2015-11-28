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
      :reader consumer-lambda)
   (on-cancel :type function
              :initarg :on-cancel
              :reader consumer-on-cancel)))

(defmacro with-consumers ((&rest consumers) &body body)
  (assert consumers)
  (with-gensyms (new-consumers)
    (let ((consumers (loop for consumer in consumers
                           if (listp consumer)
                           collect
                              `(subscribe ,@consumer)
                           else
                           collect `(subscribe-sync ,consumer))))
      `(let ((,new-consumers (list
                              ,@consumers)))
         (unwind-protect
              (progn ,@body)
           (ignore-some-conditions (connection-closed-error channel-closed-error)
             (loop for consumer in ,new-consumers do
                      (unsubscribe consumer))))))))

(defun add-consumer (channel queue tag type lambda on-cancel)
  (assert (null (gethash tag (channel-consumers channel))) (tag) 'channel-consumer-already-added channel tag)
  (let ((consumer (make-instance 'consumer :channel channel
                                           :queue queue
                                           :type type
                                           :tag tag
                                           :lambda lambda
                                           :on-cancel on-cancel)))
    (setf (gethash tag (channel-consumers channel)) consumer)))

(defun remove-consumer (channel consumer-tag)
  (remhash consumer-tag (channel-consumers channel)))

(defun find-consumer (channel consumer-tag)
  (gethash consumer-tag (channel-consumers channel)))

(defun find-message-consumer (channel message)
  (find-consumer channel (message-consumer-tag message)))

(defun execute-consumer (consumer message)
  (setf (slot-value message 'consumer) consumer)
  (let ((result (funcall (consumer-lambda consumer) message)))
    (when (eq :cancel result)
      (unsubscribe consumer))
    result))

(defun dispatch-consumed-message (channel message)
  (if-let ((consumer (find-message-consumer channel message)))
    (execute-consumer consumer message)
    (progn
      (log:error "Unknown consumer tag ~a." (message-consumer-tag message))
      (unless *ignore-unknown-consumer-tags*
        (error 'unknown-consumer-error :message message)))))

(defun dispatch-consumer-cancel (channel basic-cancel)
  (if-let ((consumer (find-consumer channel (amqp-method-field-consumer-tag basic-cancel))))
    (progn
      (remove-consumer channel consumer)
      (when (consumer-on-cancel consumer)
        (funcall (consumer-on-cancel consumer) consumer)))
    (progn
      (log:error "Unknown consumer tag ~a." (amqp-method-field-consumer-tag basic-cancel))
      (unless *ignore-unknown-consumer-tags*
        (error 'unknown-consumer-error :message basic-cancel)))))

(defun consume (&key (channel *channel*) (timeout +consume-receive-timeout+) one-shot)
  ;; what if with-consumers used many channels?
  ;; maybe replace it with flet?
  (let ((mailbox (channel-mailbox channel)))
    (loop
      (unless (channel-open-p channel)
        (error 'channel-closed-error :channel channel))
      (multiple-value-bind (message ok)
          (safe-queue:mailbox-receive-message mailbox :timeout timeout)
        (when message
          (etypecase message
            (message (setf message (dispatch-consumed-message channel message)))
            (amqp-method-basic-cancel (dispatch-consumer-cancel channel message))
            (amqp-method-channel-close (error 'channel-closed-error :channel channel))
            (amqp-method-connection-close (error 'connection-closed-error :connection (channel-connection channel)))))
        (when one-shot
          (return (values message ok)))))))

;; maybe just (let ((*channel* (consumer-channel consumer))) ... ) in execute-consumer?
(defun wrap-async-subscribe-with-channel (fn channel)
  (lambda (message)
    (let ((*channel* channel))
      (funcall fn message))))

(defun wrap-async-on-cancel-with-channel (on-cancel channel)
  (lambda (consumer)
    (let ((*channel* channel))
      (funcall on-cancel consumer))))

(defun subscribe (queue fn  &key (type :async) on-cancel consumer-tag no-local no-ack nowait exclusive arguments (channel *channel*))
  (execute-in-connection-thread-sync ((channel-connection channel))
    (let ((reply (channel.send channel (make-instance 'amqp-method-basic-consume
                                                      :queue (queue-name queue)
                                                      :consumer-tag consumer-tag
                                                      :no-local no-local
                                                      :no-ack no-ack
                                                      :exclusive exclusive
                                                      :nowait nowait
                                                      :arguments arguments))))
      (add-consumer channel queue
                    (amqp-method-field-consumer-tag reply) type
                    (if (eq type :async)
                        (wrap-async-subscribe-with-channel fn channel)
                        fn)
                    (when on-cancel
                      (if (eq type :async)
                          (wrap-async-on-cancel-with-channel on-cancel channel)
                          on-cancel))))))

(defun subscribe-sync (queue &key on-cancel consumer-tag no-local no-ack exclusive arguments (channel *channel*))
  (subscribe queue #'identity :type :sync
                              :on-cancel on-cancel
                              :consumer-tag consumer-tag
                              :no-local no-local
                              :no-ack no-ack
                              :exclusive exclusive
                              :arguments arguments
                              :channel channel))

(defun unsubscribe (consumer &key nowait)
  (unwind-protect
       (channel.send% (consumer-channel consumer)
           (make-instance 'amqp-method-basic-cancel
                          :consumer-tag (consumer-tag consumer)
                          :nowait nowait)
         (assert (equal (consumer-tag consumer) (amqp-method-field-consumer-tag reply)))
         (amqp-method-field-consumer-tag reply))
    (remove-consumer (consumer-channel consumer) (consumer-tag consumer))))

(defun channel-consume-message (channel message &key return)
  (if-let ((consumer (find-message-consumer channel message)))
    (if (eq :sync (consumer-type consumer))
        (safe-queue:mailbox-send-message (channel-mailbox channel) message)
        (execute-consumer consumer message))
    (log:error "Unknown consumer tag ~a." (message-consumer-tag message))))

(defmethod channel.receive (channel (method amqp-method-basic-deliver))
  (channel-consume-message channel (make-instance 'message
                                                  :channel channel
                                                  :body (amqp-method-content method)
                                                  :properties (amqp-method-content-properties method)
                                                  :routing-key (amqp-method-field-routing-key method)
                                                  :exchange (amqp-method-field-exchange method)
                                                  :redelivered (amqp-method-field-redelivered method)
                                                  :delivery-tag (amqp-method-field-delivery-tag method)
                                                  :consumer-tag (amqp-method-field-consumer-tag method))))

(defmethod channel.receive (channel (method amqp-method-basic-cancel))
  (if-let ((consumer (find-consumer channel (amqp-method-field-consumer-tag method))))
    (if (eq :sync (consumer-type consumer))
        (safe-queue:mailbox-send-message (channel-mailbox channel) method)
        (progn
          (remove-consumer channel consumer)
          (when (consumer-on-cancel consumer)
            (funcall (consumer-on-cancel consumer) consumer))))
    (log:error "Unknown consumer tag ~a." (amqp-method-field-consumer-tag method))))
