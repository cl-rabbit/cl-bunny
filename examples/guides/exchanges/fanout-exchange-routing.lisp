(in-package :cl-bunny.examples)

(defun fanout-exchange-routing ()
  (log:info "=> Fanout exchange routing")

  (with-connection ("amqp://")
    (with-channel ()
      (let ((x (exchange.declare "examples.ping" :type "fanout")))
        (loop for i from 0 below 10
              as q = (queue.declare :auto-delete t) do
                 (queue.bind q x)
                 ((lambda (q)
                    (subscribe q (lambda (message)
                                   (log:info "~a received ~a"
                                             q (message-body-string message)))))
                  q))

        (log:info "Sending ping")
        (publish x "Ping")

        (log:info "Waiting...")
        (sleep 3)
        
        (exchange.delete "examples.ping")
        (log:info "Disconnecting")))))
