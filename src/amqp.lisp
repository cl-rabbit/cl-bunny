(in-package :cl-bunny)

(defun amqp-basic-nack (delivery-tag &rest args &key multiple requeue (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (cl-rabbit:basic-nack (connection-cl-rabbit-connection (channel-connection channel))
                         (channel-id channel)
                         delivery-tag
                         :multiple multiple
                         :requeue requeue)))

(defun amqp-basic-nack-async (delivery-tag &rest args &key multiple requeue (channel *channel*))
  (remf args :channel)
  (cl-rabbit:basic-nack (connection-cl-rabbit-connection (channel-connection channel))
                       (channel-id channel)
                       delivery-tag
                       :multiple multiple
                         :requeue requeue))
