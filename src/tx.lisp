(in-package :cl-bunny)

(defclass tx-channel (channel)
  ())

(defun channel.tx (&optional (channel *channel*))
  (assert (eq (type-of channel) 'channel) nil 'error "Can't change channel mode to tx") ;; TODO: specialize error
  (channel.send% channel (make-instance 'amqp-method-tx-select)
    (change-class channel 'tx-channel)
    t))

(defun tx.commit (&optional (channel *channel*))
  (assert (typep channel 'tx-channel) nil 'error "Invalid channel mode") ;; TODO: specialzie error
  (channel.send% channel (make-instance 'amqp-method-tx-commit)
    t))

(defun tx.rollback (&optional (channel *channel*))
  (assert (typep channel 'tx-channel) nil 'error "Invalid channel mode") ;; TODO: specialzie error
  (channel.send% channel (make-instance 'amqp-method-tx-rollback)
    t))
