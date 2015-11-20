(ql:quickload :cl-bunny.examples)

(in-package :cl-bunny.examples)

(with-connection ()
  (with-channel ()
    (let ((q (queue.declare :name "task_queue" :auto-delete t :durable t)))
      (format t " [*] Waiting for messages in queue 'task_queue'.~%")
      (qos :prefetch-count 1)
      (subscribe q (lambda (message)
                     (let ((body (message-body-string message)))
                       (format t " [x] Received ~a~%" body)
                       ;; imitate some work
                       (sleep (count #\. body))
                       (message.ack message)
                       (print " [x] Done")))
                 :type :sync)
      (consume :one-shot t :timeout nil))))
