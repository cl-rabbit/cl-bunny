(in-package :cl-bunny.examples)

(defun connection-blocked ()
  (with-connection ()
    (with-channel ()
      (let ((q (queue.declare-temp)))
        (event+ (connection-on-blocked)
                (lambda (connection reason)
                  (log:info "Connection ~a blocked with reason ~a"
                            connection reason)))
        (event+ (connection-on-unblocked)
                (lambda (connection)
                  (log:info "Connection ~a unlblocked"
                            connection)))
        (loop for i from 0 to 9 do
                 (queue.put q "Hello World!"))))))
