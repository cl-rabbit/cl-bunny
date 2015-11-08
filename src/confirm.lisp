(in-package :cl-bunny)

(defclass confirm-channel (channel)
  ((counter :initform 1 :type fixnum :accessor confirm-channel-counter)
   (unconfirmed-set :initform (make-hash-table :test 'eql) :accessor channel-unconfirmed-set)
   (lock :initform (bt:make-lock) :reader confirm-channel-lock)
   (condition-var :initform (bt:make-condition-variable) :reader confirm-channel-condition-var)))

(defclass confirm-message (message)
  ((nowait :initarg :nowait :reader confirm-message-nowait)
   (promise :initarg :promise :reader confirm-message-promise)))

(defun channel.confirm (&optional (channel *channel*))
  (channel.send% channel (make-instance 'amqp-method-confirm-select)
    (change-class channel 'confirm-channel)))

(defun make-confirm-message (content properties exchange channel delivery-tag nowait)
  (make-instance 'confirm-message :body content
                                  :properties properties
                                  :exchange (exchange-name exchange)
                                  :channel channel
                                  :delivery-tag delivery-tag
                                  :nowait nowait
                                  :promise (unless nowait (lparallel:promise))))

(defmethod channel.publish ((channel confirm-channel) content exchange &key (nowait) (routing-key "") (mandatory nil) (immediate nil) (properties (make-instance 'amqp-basic-class-properties)) &allow-other-keys)
  (assert (channel-open-p% channel) () 'channel-closed-error :channel channel)
  (when (and (eq (bt:current-thread) (connection-thread (channel-connection channel)))
             (not nowait))
    (error "Waiting for confirms on connection thread not supported yet"))
  (let ((unconfirmed-set (channel-unconfirmed-set channel))
        (confirm-message (make-confirm-message content properties exchange channel (confirm-channel-counter channel) nowait)))
    (bt:with-lock-held ((confirm-channel-lock channel))
      (setf (gethash (confirm-channel-counter channel) unconfirmed-set) confirm-message)
      (incf (confirm-channel-counter channel))) ;; check for max counter value hert
    (if nowait
        (call-next-method)
        (progn
          (call-next-method)
          (lparallel:force (confirm-message-promise confirm-message))))))

(defun notify-confirm-message-waiter (message acked)
  (unless (confirm-message-nowait message)
    (lparallel:fulfill (confirm-message-promise message)
      (values message acked))))

(defmethod channel.receive ((channel confirm-channel) (method amqp-method-basic-ack))
  (log:debug "Ack received ~a" method)
  (bt:with-lock-held ((confirm-channel-lock channel))
    (let* ((unconfirmed-set (channel-unconfirmed-set channel))
           (message (gethash (amqp-method-field-delivery-tag method) unconfirmed-set)))
      (flet ((ack-confirm-message (message)
               (remhash (message-delivery-tag message) unconfirmed-set)
               (notify-confirm-message-waiter message t)))
        (unless message
          (error "Unknown delivery-tag ~a" (amqp-method-field-delivery-tag method)))
        (ack-confirm-message message)
        (when (amqp-method-field-multiple method)
          (with-hash-table-iterator (next-entry unconfirmed-set)
            (loop (multiple-value-bind (more delivery-tag message) (next-entry)
                    (unless more (return nil))
                    (when (< delivery-tag (amqp-method-field-delivery-tag method))
                      (ack-confirm-message message))))))))
    (bt:condition-notify (confirm-channel-condition-var channel))))

(defmethod channel.wait-confirms% ((channel confirm-channel) timeout)
  (bt:with-lock-held ((confirm-channel-lock channel))
    (when (= 0 (hash-table-count (channel-unconfirmed-set channel)))
      (return-from channel.wait-confirms% t))
    (if (eq (bt:current-thread) (connection-thread (channel-connection channel)))
        (error "Waiting for confirms on connection thread not supported yet")
        (sb-thread:condition-wait (confirm-channel-condition-var channel) (confirm-channel-lock channel) :timeout timeout))))

(defun channel.wait-confirms (&key (channel *channel*) timeout)
  (channel.wait-confirms% channel timeout))
