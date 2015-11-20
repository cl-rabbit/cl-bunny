(in-package :cl-bunny.test)

(plan 1)

(subtest "Basic.ack tests"
  (subtest "Acknowledges a message with a valid (known) delivery tag"
    (with-connection ()
      (with-channel ()
        (let ((q (queue.declare :name "cl-bunny.basic.ack.manual-acks" :exclusive t))
              (x (exchange.default)))
          (publish x "bunneth" :routing-key q)
          (sleep 0.5)
          (is (queue.message-count q) 1)
          (let ((message (queue.get)))
            (isnt message nil)
            (message.ack message))))
      (with-channel ()
        (let ((q (queue.declare :name "cl-bunny.basic.ack.manual-acks" :exclusive t)))
          (is (queue.message-count q) 0))))))

(finalize)
