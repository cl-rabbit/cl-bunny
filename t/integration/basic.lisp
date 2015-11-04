(in-package :cl-bunny.test)

(plan 1)

(subtest "Basic publish/consume test"
  (with-connection ("amqp://" :one-shot t)
    (let ((queue))
      (flet ((test-send (message)
               (with-connection ("amqp://" :one-shot t)
                 (with-channel ()
                   (let ((x (default-exchange)))
                     (setf queue (queue.declare :auto-delete t))
                     (publish x message :routing-key queue)))))
             (test-recv-sync ()
               (with-connection ("amqp://" :one-shot t)
                 (with-channel ()
                   (with-consumers
                       ((queue
                         (lambda (message)
                           (message-ack message)
                           (return-from test-recv-sync (babel:octets-to-string (message-body message))))
                         :type :sync))
                     (consume :one-shot t))))))
        (test-send "Hello World!")
        (is (test-recv-sync) "Hello World!")))))


(finalize)
