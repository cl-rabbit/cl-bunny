(in-package :cl-bunny.test)

(plan 1)

(subtest "Queue.unbind tests"
  (with-connection ()
    (with-channel ()
      (let ((x (exchange.fanout))
            (q (queue.declare-temp)))
        (queue.bind q x)

        (publish x "")

        (sleep 1)
        (is (queue.message-count q) 1)

        (queue.unbind q x)

        (publish x "")

        (sleep 1)
        (is (queue.message-count q) 1)))))

(finalize)
