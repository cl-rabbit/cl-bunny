(in-package :cl-bunny)

(defvar *connection* nil
  "Current AMQP connection")

(defvar *channel* nil
  "Current AMQP channel")


(defun amqp-channel-open (channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (with-slots (connection open-p channel-id) channel
      (when open-p
        (error 'channel-already-open :channel channel))
      (cl-rabbit:channel-open (connection-cl-rabbit-connection connection)
                              channel-id)
      (setf open-p t)
      (setf (gethash channel-id (connection-channels connection)) channel))))

(defun amqp-channel-close (channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (with-slots (connection open-p channel-id) channel
      (when open-p
        (ignore-errors ;; TODO: Are you sure?
         (cl-rabbit:channel-close (connection-cl-rabbit-connection connection)
                                  channel-id)))
      (setf open-p nil)
      (if (eql channel  (gethash channel-id (connection-channels connection)))
          (remhash channel-id (connection-channels connection)))
      (release-channel-id (connection-channel-id-allocator connection) channel-id))))

(defun amqp-queue-declare (name &rest args &key passive durable exclusive auto-delete arguments (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:queue-declare
           (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-id channel) :queue name)
                   args))))

(defun amqp-queue-delete (name &rest args &key if-unused if-empty (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:queue-delete
           (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-id channel)name)
                   args))))

(defun amqp-queue-bind (name &rest args &key exchange routing-key arguments (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:queue-bind (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-id channel) :queue name) args))))

(defun amqp-queue-purge (queue &key (channel *channel*))
  (execute-in-connection-thread-sync ((channel-connection channel))
    (cl-rabbit:queue-purge (connection-cl-rabbit-connection (channel-connection channel))
                           (channel-id channel)
                           queue)))

(defun amqp-exchange-declare (name &rest args &key (type "direct") (channel *channel*) passive durable auto-delete internal arguments)
  (remf args :channel)
  (remf args :type)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:exchange-declare
           (append (list (connection-cl-rabbit-connection (channel-connection channel))
                         (channel-id channel)
                         name
                         type)
                   args)))
  name)

(defun amqp-exchange-delete (name &rest args &key if-unused (channel *channel*))
  (remf args :channel)
  (remf args :type)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:exchange-delete
           (append (list (connection-cl-rabbit-connection (channel-connection channel))
                         (channel-id channel)
                         name)
                   args))))

(defun amqp-basic-qos (prefetch-count &rest args &key global
                                                  (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:basic-qos (append
                                      (list
                                       (connection-cl-rabbit-connection (channel-connection channel))
                                       (channel-id channel)
                                       0
                                       prefetch-count) args))))

;; TODO: detect string, set encoding/content type appropriately. use this info to decode message body
(defun amqp-basic-publish (body &rest args &key (exchange "") routing-key mandatory immediate properties
                                            (encoding :utf-8)
                                            (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:basic-publish (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-id channel) :body body) args))))


(defun amqp-basic-consume% (queue &rest args &key consumer-tag no-local no-ack exclusive arguments
                                             (channel *channel*))
  (remf args :channel)
  (apply #'cl-rabbit:basic-consume (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-id channel) queue) args)))

(defun amqp-basic-consume (queue &rest args &key consumer-tag no-local no-ack exclusive arguments
                                             (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:basic-consume (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-id channel) queue) args))))

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
