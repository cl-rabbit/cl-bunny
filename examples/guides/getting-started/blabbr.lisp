(in-package :cl-bunny.examples)

(defun blabbr ()
  (log:info "=> blabbr example")

  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let ((x (fanout-exchange "nba.scores" :auto-delete t)))
        (->
          (queue.declare "joe" :auto-delete t)
          (queue.bind x)
          (subscribe (lambda (message)
                       (log:info "~a => joe"
                                 (babel:octets-to-string (message-body message))))))
        (->
          (queue.declare "aaron" :auto-delete t)
          (queue.bind x)
          (subscribe (lambda (message)
                       (log:info "~a => aaron"
                                 (babel:octets-to-string (message-body message))))))
        (->
          (queue.declare "bob" :auto-delete t)
          (queue.bind x)
          (subscribe (lambda (message)
                       (log:info "~a => bob"
                                 (babel:octets-to-string (message-body message))))))

        (log:info "Publishing")        
        (->
          x
          (publish "BOS 101, NYK 89")
          (publish "ORL 85, ALT 88"))
        
        (log:info "Waiting...")
        (sleep 3)
        (log:info "Disconnecting")))))


