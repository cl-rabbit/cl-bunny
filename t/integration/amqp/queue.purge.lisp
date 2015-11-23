(in-package :cl-bunny.test)

(plan 1)

(subtest "Queue.purge tests"
  (with-connection ()
    (with-channel ()
      (let ((q (queue.declare-temp)))
        (queue.put q "message 1")
        (queue.put q "message 2")
        (queue.put q "message 3")
        (sleep 1)
        (is (queue.message-count q) 3)
        (queue.purge q)
        (is (queue.message-count q) 0)))))

(finalize)
