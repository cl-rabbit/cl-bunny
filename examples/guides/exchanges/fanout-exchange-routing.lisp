(in-package :cl-bunny.examples)

(defun fanout-exchange-routing ()
  (log:info "=> Fanout exchange routing")

  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let ((x (amqp-exchange-declare "examples.ping" :type "fanout")))
        (loop for i from 0 below 10
              as q = (amqp-queue-declare "" :auto-delete t) do
                 (amqp-queue-bind q :exchange x)
                 ((lambda (q)
                    (subscribe q (lambda (message)
                                   (log:info "~a received ~a"
                                             q (babel:octets-to-string (message-body message))))))
                  q))

        (log:info "Sending ping")
        (amqp-basic-publish "Ping" :exchange x)

        (log:info "Waiting...")
        (sleep 3)
        
        (amqp-exchange-delete "examples.ping")
        (log:info "Disconnecting")))))
