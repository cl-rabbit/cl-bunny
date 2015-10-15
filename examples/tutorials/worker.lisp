(ql:quickload :cl-bunny.examples)

(in-package :cl-bunny.examples)

(with-connection ("amqp://")
  (with-channel ()
    (let ((n 1)
          (q (queue.declare "task_queue" :auto-delete t :durable t)))
      (format t " [*] Waiting for messages in queue 'task_queue'. To exit type (exit)~%")
      (setf (channel-prefetch cl-bunny::*channel*) n)
      (subscribe q (lambda (message)
                     (let ((body (babel:octets-to-string (message-body message))))
                       (format t " [x] Received ~a~%" body)
                       ;; imitate some work
                       (sleep (count #\. body))
                       (print " [x] Done~%")
                       (message-ack message)))                       
                 :no-ack t
                 :type :sync)
      (consume :one-shot t))))
