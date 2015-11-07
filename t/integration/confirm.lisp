(in-package :cl-bunny.test)

(plan 1)

(subtest "Batch Publisher Acknowledgements test"
  (with-connection ("amqp://")
    (let ((send-channel (channel.new.open))
          (recv-channel (channel.new.open)))
      (let ((queue))
        (flet ((test-send (message)
                 (with-channel (send-channel :close nil)
                   (channel.confirm)
                   (let ((x (exchange.default)))
                     (setf queue (queue.declare :auto-delete t))
                     (loop for i from 0 below 1000 do
                              (publish x message :routing-key queue :nowait t))
                     ;; "Waiting for confirms"
                     (is (channel.wait-confirms :timeout 3) t "All messages confirmed"))))
               (test-recv-sync ()
                 (let ((recv "Hello World!"))
                   (with-channel (recv-channel :close nil)
                     (with-consumers
                         ((queue
                           (lambda (message)
                             (message-ack message)
                             (assert (equal recv (message-body-string message))))
                           :type :sync))
                       (loop for i from 0 below 1000 do
                                (consume :one-shot t))))
                   recv)))
          (test-send "Hello World!")
          (is (test-recv-sync) "Hello World!"))))))

(subtest "Publisher Acknowledgements test"
  (with-connection ("amqp://")
    (let ((send-channel (channel.new.open))
          (recv-channel (channel.new.open)))
      (let ((queue))
        (flet ((test-send (message)
                 (with-channel (send-channel :close nil)
                   (channel.confirm)
                   (let ((x (exchange.default)))
                     (setf queue (queue.declare :auto-delete t))
                     (multiple-value-bind (message acked)
                         (publish x message :routing-key queue)
                       (declare (ignorable message))
                       ;; "Waiting for confirms"
                       (is acked t "Message confirmed")))))
               (test-recv-sync ()
                 (let ((recv "Hello World!"))
                   (with-channel (recv-channel :close nil)
                     (with-consumers
                         ((queue
                           (lambda (message)
                             (message-ack message)
                             (assert (equal recv (message-body-string message))))
                           :type :sync))
                       (consume :one-shot t)))
                   recv)))
          (test-send "Hello World!")
          (is (test-recv-sync) "Hello World!"))))))



(finalize)
