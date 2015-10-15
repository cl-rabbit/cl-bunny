(ql:quickload :cl-bunny.examples)

(in-package :cl-bunny.examples)

(with-connection ("amqp://")
  (with-channel ()
    (let ((msg (format nil "~{~a ~}" (cdr sb-ext:*posix-argv*)))
          (x (amqp-exchange-declare "logs" :type "fanout")))
      (publish x msg)
      (format t " [x] Sent '~a'~%" msg)
      (sleep 1))))
