(in-package :cl-bunny.test)

(plan 1)

(subtest "Basic publish/consume test"
  (with-connection ("amqp://")
    (let ((queue))
      (flet ((test-send (message)
               (with-connection ("amqp://")
                 (with-channel ()
                   (let ((x (exchange.default)))
                     (setf queue (queue.declare :auto-delete t))
                     (publish x message :routing-key queue)))))
             (test-recv-sync ()
               (with-connection ("amqp://")
                 (with-channel ()
                   (with-consumers
                       ((queue
                         (lambda (message)
                           (message-ack message)
                           (return-from test-recv-sync (message-body-string message)))
                         :type :sync))
                     (consume :one-shot t))))))
        (test-send "Hello World!")
        (is (test-recv-sync) "Hello World!")))))


(finalize)
