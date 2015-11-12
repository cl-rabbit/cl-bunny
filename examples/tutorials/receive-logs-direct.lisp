(ql:quickload :cl-bunny.examples)

(in-package :cl-bunny.examples)

(if-let ((args (rest sb-ext:*posix-argv*)))
  (with-connection ()
    (with-channel ()
      (let ((q (queue.declare :auto-delete t))
            (x (exchange.direct "direct_logs")))
        (loop for severity in args do
                 (queue.bind q x :routing-key severity))
        (format t " [*] Waiting for logs. To exit type (exit)~%")
        (subscribe q (lambda (message)
                       (let ((body (message-body-string message)))
                         (format t " [x] #~a~%" body)))
                   :type :sync)
        (consume))))
  (error "Usage: #{$0} [info] [warning] [error]")) ;; TODO: wtf is #{$0}?
