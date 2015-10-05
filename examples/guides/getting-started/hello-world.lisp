(in-package :cl-bunny.examples)

(defun test-send (message)
  (with-connection "amqp://"
    (with-channel ()
      (amqp-queue-declare "test-queue")
      (amqp-basic-publish message :routing-key "test-queue"))))

(defun test-recv ()
  (with-connection "amqp://"
    (with-channel ()
      (amqp-queue-declare "test-queue")
      (with-consumers
          (("test-queue"
            (lambda (message)
              (format t "Got message ~a" (babel:octets-to-string (message-body message))))
             :type :sync))
        (consume :one-shot t)))))
