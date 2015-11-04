(in-package :cl-bunny.test)

(plan 1)

(subtest "fanout-exchange-routing" ()
  (let ((queues (make-hash-table)))
    (with-connection ("amqp://" :one-shot t)
      (with-channel ()
        (let ((x (exchange.fanout "examples.ping")))
          (loop for i from 0 below 10
                as q = (queue.declare :auto-delete t) do
                   (queue.bind q x)
                   (setf (gethash q queues) nil)
                   ((lambda (q)
                      (subscribe q (lambda (message)
                                     (setf (gethash q queues) (message-body-string message)))))
                    q))
          (publish x "Ping")
          (sleep 3)

          (is (hash-table-count queues) 10 "10 queues bound to fanout exchange")

          (is (every (lambda (value)
                       (equal value "Ping")) (hash-table-values queues))
              t
              "Each queue received Ping")

          (exchange.delete "examples.ping"))))))

(finalize)
