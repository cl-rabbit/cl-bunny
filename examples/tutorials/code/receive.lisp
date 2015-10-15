(ql:quickload :cl-bunny.examples)

(in-package :cl-bunny.examples)

(with-connection ("amqp://")
  (with-channel ()
    (let ((q (queue.declare "hello" :auto-delete t)))
      (format t " [*] Waiting for messages in queue 'hello'. To exit type (exit)~%")
      (subscribe q (lambda (message)
                     (let ((body (babel:octets-to-string (message-body message))))
                       (format t " [x] Received ~a~%" body)))
                 :type :sync)
      (consume :one-shot t))))
