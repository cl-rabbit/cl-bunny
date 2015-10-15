(ql:quickload :cl-bunny.examples)

(in-package :cl-bunny.examples)

(with-connection ("amqp://")
  (with-channel ()
    (let ((x (default-exchange))
          (msg (format nil "~{~a ~}" (cdr sb-ext:*posix-argv*))))
      (publish x msg :routing-key "task_queue"
                     :properties `((:persistent . ,t)))
      (format t " [x] Sent '~a'~%" msg)
      (sleep 1))))
