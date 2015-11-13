(in-package :cl-bunny)

(defun amqp-basic-consume% (queue &rest args &key consumer-tag no-local no-ack exclusive arguments
                                             (channel *channel*))
  (remf args :channel)
  (apply #'cl-rabbit:basic-consume (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-id channel) queue) args)))

(defun amqp-basic-cancel (consumer-tag &rest args &key no-wait (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (cl-rabbit:basic-cancel (connection-cl-rabbit-connection (channel-connection channel))
                            (channel-id channel)
                            consumer-tag)))

(defun amqp-basic-cancel-async (consumer-tag &rest args &key no-wait (channel *channel*))
  (remf args :channel)
  (cl-rabbit:basic-cancel (connection-cl-rabbit-connection (channel-connection channel))
                          (channel-id channel)
                          consumer-tag))

(defun amqp-basic-ack (delivery-tag &rest args &key multiple (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (cl-rabbit:basic-ack (connection-cl-rabbit-connection (channel-connection channel))
                         (channel-id channel)
                         delivery-tag
                         :multiple multiple)))

(defun amqp-basic-ack-async (delivery-tag &rest args &key multiple (channel *channel*))
  (remf args :channel)
  (cl-rabbit:basic-ack (connection-cl-rabbit-connection (channel-connection channel))
                       (channel-id channel)
                       delivery-tag
                       :multiple multiple))

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
