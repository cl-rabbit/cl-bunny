(in-package :cl-bunny.examples)

(defun mandatory-messages ()
  (log:info "=> Publishing messages as mandatory")

  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let* ((x (exchange.default))
             (q (queue.declare :exclusive t)))

        (setf (exchange-on-return-callback x)
              (lambda (returned-message)
                (log:info "Got returned message ~a" (message-body-string returned-message))))

        (subscribe q (lambda (message)                       
                       (log:info "~a received ~a" q (message-body-string message))))

        (publish x "This will NOT be returned" :mandatory t :routing-key q)
        (publish x "This will be returned" :mandatory t :routing-key (format nil "wefvvtrw~a" (random 10)))        

        (log:info "Waiting...")
        (sleep 3)
        (log:info "Disconnecting")))))
