(in-package :cl-bunny.examples)

(defun test-send (message)
  (with-connection "amqp://"
    (with-channel ()
      (amqp-queue-declare "test-queue")
      (amqp-basic-publish message :routing-key "test-queue"))))

(defun test-recv-sync ()
  (with-connection "amqp://"
    (with-channel ()
      (amqp-queue-declare "test-queue")
      (with-consumers
          (("test-queue"
            (lambda (message)
              (format t "Got message ~a" (babel:octets-to-string (message-body message))))
            :type :sync))
        (consume :one-shot t)))))

(defun hello-world ()
  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let ((x (default-exchange)))
        (->
          (queue.declare "cl-bunny.examples.hello-world" :auto-delete t)
          (subscribe (lambda (message)
                       (log:info "Received ~a"
                                 (babel:octets-to-string (message-body message))))))
        (publish x "Hello world!" :routing-key "cl-bunny.examples.hello-world"))
      (sleep 1))))

(defun hello-world-sync ()
  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let ((x (default-exchange))
            (q "cl-bunny.examples.hello-world"))

        (queue.declare q :auto-delete t)
        (with-consumers
            ((q
              (lambda (message)
                (log:info "Received ~a"
                          (babel:octets-to-string (message-body message))))
              :type :sync))
          (publish x "Hello world!" :routing-key q)
          (consume :one-shot t))))))
