(ql:quickload :cl-bunny.examples)

(in-package :cl-bunny.examples)

(with-connection ("amqp://")
  (with-channel ()
    (let* ((args (cdr sb-ext:*posix-argv*))
           (severity (if (car args) (car args) "info"))
           (msg (format nil "~{~a ~}" (cdr args)))
           (x (amqp-exchange-declare "direct_logs" :type "direct")))
      (publish x msg :routing-key severity)
      (format t " [x] Sent '~a'" msg)
      (sleep 1))))
