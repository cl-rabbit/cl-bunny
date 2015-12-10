(in-package :cl-bunny.test)

(plan 1)

(subtest "Basic publish/consume test"

  (subtest "Sync with-consumers"
    (with-connection ()
      (let ((queue))
        (labels ((test-send (message)
                   (with-channel ()
                     (let ((x (exchange.default)))
                       (setf queue (queue.declare :auto-delete t))
                       (publish x message :routing-key queue))))
                 (test-recv-sync ()
                   (let ((string))
                     (with-channel ()
                       (with-consumers
                           ((queue
                             (lambda (message)
                               (message.ack message)
                               (setf string (message-body-string message)))
                             :type :sync))
                         (is (message-body-string (consume :one-shot t)) "Hello World!" "Sync consumer didn't timed out"))
                       string))))
          (test-send "Hello World!")
          (is (test-recv-sync) "Hello World!")))))

  (subtest "Timed out one-shot consume"
    (with-connection ("amqp://")
      (with-channel ()
        (with-consumers
            (((queue.declare :auto-delete t)
              (lambda (message)
                (message.ack message)
                (message-body-string message))
              :type :sync))
          (is (consume :one-shot t) nil "Sync consumer timed out")))))

  (subtest "Sync with-consumers short"
    (with-connection ()
      (let ((queue)
            (consumer))
        (labels ((test-send (message)
                   (with-channel ()
                     (setf queue (queue.declare :auto-delete t))
                     (publish (exchange.default) message :routing-key queue)))
                 (test-recv-sync ()
                   (with-channel ()
                     (setf consumer (subscribe-sync queue))
                     (consume :one-shot t))))
          (test-send "Hello World!")
          (multiple-value-bind (message ok)
              (test-recv-sync)
            (is ok t)
            (is (message-body-string message) "Hello World!")
            (is (message-consumer message) consumer "Message consumer set"))))))

  (subtest "Async consumer"
    (let ((string))
      (with-connection ()
        (with-channel ()
          (let ((x (exchange.default)))
            (subscribe
             (queue.declare :name "cl-bunny.test.hello-world" :auto-delete t)
             (lambda (message)
               (setf string
                     (message-body-string message))))
            (publish x "Hello world!" :routing-key "cl-bunny.test.hello-world"))
          (sleep 1)
          (is string "Hello world!")))))

  (subtest "Shared connection"
    (with-connection ("amqp://" :shared t)
      (let ((queue))
        (labels ((test-send (message)
                   (with-channel ()
                     (let ((x (exchange.default)))
                       (setf queue (queue.declare :auto-delete t))
                       (publish x message :routing-key queue))))
                 (test-recv-sync ()
                   (let ((string))
                     (with-channel ()
                       (with-consumers
                           ((queue
                             (lambda (message)
                               (message.ack message)
                               (setf string (message-body-string message)))
                             :type :sync))
                         (is (message-body-string (consume :one-shot t)) "Hello World!" "Sync consumer didn't timed out"))
                       string))))
          (test-send "Hello World!")
          (is (test-recv-sync) "Hello World!")
          (connection.close))))))

(finalize)
