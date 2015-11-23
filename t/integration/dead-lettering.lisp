(in-package :cl-bunny.test)

(plan 1)

(subtest "Dead lettering test"
  (subtest "Nacked message"
    (with-connection ()
      (with-channel ()
        (let ((dlx))
          (unwind-protect
               (progn
                 (setf dlx (exchange.fanout "cl-bunny.tests.dlx.exchange"))
                 (let* ((x (exchange.fanout))
                        (q (->
                             (queue.declare-temp :arguments '(("x-dead-letter-exchange" . "cl-bunny.tests.dlx.exchange")))
                             (queue.bind x)))
                        (dlq (-> (queue.declare-temp)
                               (queue.bind dlx))))
                   (publish x "")
                   (sleep 0.2)
                   (let ((message (queue.get q)))
                     (is (queue.message-count dlq) 0)
                     (message.nack message))
                   (sleep 0.2)
                   (is (queue.message-count q) 0)
                   (let* ((message (queue.get dlq))
                          (ds (message-header-value message "x-death")))
                     (isnt ds nil)
                     (is (header-value (aref ds 0) "reason") "rejected"))))
            (when dlx
              (exchange.delete dlx)))))))

  (subtest "Expired message"
    (with-connection ()
      (with-channel ()
        (let ((dlx))
          (unwind-protect
               (progn
                 (setf dlx (exchange.fanout "cl-bunny.tests.dlx.exchange"))
                 (let* ((x (exchange.fanout))
                        (q (->
                             (queue.declare-temp :arguments '(("x-dead-letter-exchange" . "cl-bunny.tests.dlx.exchange")
                                                              ("x-message-ttl" . 100)))
                             (queue.bind x)))
                        (dlq (-> (queue.declare-temp)
                               (queue.bind dlx))))
                   (publish x "")
                   (sleep 0.5)
                   (is (queue.message-count q) 0)
                   (is (queue.message-count dlq) 1)))
            (when dlx
              (exchange.delete dlx))))))))

(finalize)
