(in-package :cl-bunny.examples)

(defun blabbr ()
  (log:info "=> blabbr example")

  (with-connection ("amqp://")
    (with-channel ()
      (let ((x (exchange.fanout "nba.scores" :auto-delete t)))
        (->
          (queue.declare :name "joe" :auto-delete t)
          (queue.bind x)
          (subscribe (lambda (message)
                       (log:info "~a => joe"
                                 (babel:octets-to-string (message-body message))))))
        (->
          (queue.declare :name "aaron" :auto-delete t)
          (queue.bind x)
          (subscribe (lambda (message)
                       (log:info "~a => aaron"
                                 (babel:octets-to-string (message-body message))))))
        (->
          (queue.declare :name "bob" :auto-delete t)
          (queue.bind x)
          (subscribe (lambda (message)
                       (log:info "~a => bob"
                                 (babel:octets-to-string (message-body message))))))

        (log:info "Publishing")        
        (publish x "BOS 101, NYK 89")
        (publish x "ORL 85, ALT 88")
        
        (log:info "Waiting...")
        (sleep 3)
        (log:info "Disconnecting")))))


