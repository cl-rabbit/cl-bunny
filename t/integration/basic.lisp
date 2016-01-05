(in-package :cl-bunny.test)

(plan 1)

(subtest "Basic publish/consume test"

  (subtest "Same-thread connection"
    (subtest "Async consumer"
      (let ((text))
        (with-connection ("amqp://" :type 'bunny::librabbitmq-connection)
          (with-channel ()
            (let ((x (exchange.default))
                  (q "cl-bunny.examples.hello-world"))

              (queue.declare :name q :auto-delete t)
              (with-consumers
                  ((q
                    (lambda (message)
                      (setf text (message-body-string message)))))
                (publish x "Hello world!" :routing-key q)
                (is (message-body-string (connection.consume :one-shot t)) "Hello world!")
                (sleep 0.5)
                (is text "Hello world!")))))))

    (subtest "Sync consumer"
      (let ((text))
        (with-connection ("amqp://" :type 'bunny::librabbitmq-connection)
          (with-channel ()
            (let ((x (exchange.default))
                  (q "cl-bunny.examples.hello-world"))

              (queue.declare :name q :auto-delete t)
              (with-consumers
                  ((q
                    (lambda (message)
                      (setf text (message-body-string message)))
                    :type :sync))
                (publish x "Hello world!" :routing-key q)
                (is (message-body-string (connection.consume :one-shot t)) "Hello world!")
                (is text "Hello world!"))))))))

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
          (connection.close)))))

  (subtest "Using shared connection from non-blocking thread"
    (with-connection ()
      (with-channel ()
        (let* ((control-fd (eventfd:eventfd.new 0))
               (control-mailbox (safe-queue:make-queue))
               (notify-lambda (lambda (thunk)
                                (safe-queue:enqueue thunk control-mailbox)
                                (eventfd:eventfd.notify-1 control-fd)))
               (*notification-lambda* notify-lambda)
               (x)
               (message))
          (funcall notify-lambda (lambda ()
                                   (bb:alet ((x (bunny:exchange.declare "hmm2" :auto-delete t))
                                             (q (bunny:queue.declare-temp))
                                             (text "Hello World!"))
                                     (bb:chain
                                         (bunny:queue.bind q x :routing-key q)
                                       (:then () (bunny:subscribe-sync q))
                                       (:then () (bunny:publish x text :routing-key q :properties '(:content-type "text/plain")))
                                       (:then () (bunny:consume :one-shot t))
                                       (:then (m) (setf message (message-body-string m)))
                                       (:then () (queue.unbind q x))
                                       (:then () (exchange.delete x))
                                       (:finally () (throw 'stop nil))))))
          (funcall notify-lambda (lambda () (setf x 1)))
          (unwind-protect
               (catch 'stop
                 (iolib:with-event-base (ev)
                   (iolib:set-io-handler ev control-fd
                                         :read (lambda (fd e ex)
                                                 (declare (ignorable fd e ex))
                                                 (eventfd:eventfd.read control-fd)
                                                 (log:debug "Got lambda to execute on connection thread")
                                                 (loop for lambda = (safe-queue:dequeue control-mailbox)
                                                       while lambda
                                                       do (funcall lambda))))
                   (iolib:event-dispatch ev)))
            (eventfd:eventfd.close control-fd)
            (is x 1 "Event loop iterated more than once")
            (is message "Hello World!" "Message consumed")))))))

(finalize)
