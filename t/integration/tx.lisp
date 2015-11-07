(in-package :cl-bunny.test)

(plan 1)

(subtest "Test channel mode"
  (with-connection ("amqp://")
    (with-channel ()
      (channel.confirm)
      (is-error (channel.tx) 'error "Can't change confirm mode to tx"))
    (with-channel ()
      (is-error (tx.commit) 'error "Can't tx.commit on non-tx channel"))
    (with-channel ()
      (is-error (tx.rollback) 'error "Can't tx.rollback on non-tx channel"))
    (with-channel ()
      (is (channel.tx) t)
      (ok (tx.commit))
      (ok (tx.rollback)))))

(subtest "Test tx.commit"
  (with-connection ("amqp://")
    (let ((queue))
      (flet ((test-send (message)
               (with-channel ()
                 (let ((x (exchange.default)))
                   (channel.tx)
                   (setf queue (queue.declare :auto-delete t))
                   (publish x message :routing-key queue)
                   (tx.commit))))
             (test-recv-sync ()
               (with-channel ()
                 (with-consumers
                     ((queue
                       (lambda (message)
                         (message-ack message)
                         (return-from test-recv-sync (message-body-string message)))
                       :type :sync))
                   (consume :one-shot t)))))
        (test-send "Hello World!")
        (is (test-recv-sync) "Hello World!")))))

(subtest "Test tx.rollback"
  (with-connection ("amqp://")
    (let ((queue))
      (flet ((test-send (message)
               (with-channel ()
                 (let ((x (exchange.default)))
                   (channel.tx)
                   (setf queue (queue.declare :auto-delete t))
                   (publish x message :routing-key queue)
                   (tx.rollback))))
             (test-recv-sync ()
               (with-channel ()
                 (with-consumers
                     ((queue
                       (lambda (message)
                         (message-ack message)
                         (return-from test-recv-sync (message-body-string message)))
                       :type :sync))
                   (consume :one-shot t :timeout 3)))))
        (test-send "Hello World!")
        (is (test-recv-sync) nil "Publish canceled with tx.rollback")))))

(finalize)
