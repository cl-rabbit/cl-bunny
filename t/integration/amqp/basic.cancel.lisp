(in-package :cl-bunny.test)

(plan 1)

(subtest "Basic.cancel tests"
  (subtest "Server-sent cancellation for async consumer"
    (with-connection ()
        (with-channel ()
          (let* ((q (queue.declare :name "basic.consume1" :auto-delete t))
                 (cancelled)
                 (consumer (subscribe q #'identity
                                      :on-cancel (lambda (consumer) (setf cancelled consumer)))))

            (queue.put q "abc")

            (sleep 1)

            (queue.delete q)
            (sleep 1)

            (is cancelled consumer)))))

  (subtest "Server-sent cancellation for sync consumer"
    (with-connection ()
      (with-channel ()
        (let* ((q (queue.declare :name "basic.consume1" :auto-delete t))
               (cancelled)
               (consumer (subscribe-sync q :on-cancel (lambda (consumer) (setf cancelled consumer)))))

          (queue.put q "abc")
          (is (message-body-string (consume :one-shot t)) "abc")
          (queue.delete q)
          (consume :one-shot t)
          (is cancelled consumer)))))

  (subtest "Client-sent cancellation"
    (with-connection ()
      (with-channel ()
        (let* ((q (queue.declare :name "basic.consume1" :auto-delete t))
               (consumer (subscribe-sync q)))
          
          (queue.put q "abc")
          (is (message-body-string (consume :one-shot t)) "abc")
          (unsubscribe consumer)
          (queue.put q "abc")
          (is (consume :one-shot t) nil "No message consumed during timeout window"))))))

(finalize)
