(in-package :cl-bunny.test)

(plan 1)

(subtest "Queue.delete tests"
  (with-connection ()
    (with-channel ()
      (let ((q (queue.declare-temp)))

        (queue.delete q)
        (queue.delete q)
        (ok "No error on second delete")

        ;; TODO: (is (length (channel-queues)) 0 "Channel has no registered queues")
        ))))

(finalize)
