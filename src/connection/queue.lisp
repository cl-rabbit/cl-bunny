(in-package :cl-bunny)

;; maybe there should be queue object with its own mailbox?

(defun queue.declare (name &key passive durable exclusive auto-delete arguments (channel *channel*))
  (amqp-queue-declare name :passive passive
                           :durable durable
                           :exclusive exclusive
                           :auto-delete auto-delete
                           :arguments arguments
                           :channel channel))

(defun queue.bind (queue exchange &key routing-key arguments (channel *channel*))
  (amqp-queue-bind queue :exchange (exchange-name exchange)
                         :routing-key routing-key
                         :arguments arguments
                         :channel channel)
  queue)

(defun queue.purge (queue &key (channel *channel*))
  (amqp-queue-purge queue :channel channel)
  queue)

(defun queue.delete (queue &key if-unused if-empty (channel *channel*))
  (amqp-queue-delete queue :if-unused if-unused
                           :if-empty if-empty
                           :channel channel))
