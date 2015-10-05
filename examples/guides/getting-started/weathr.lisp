(in-package :cl-bunny.examples)

(defun weathr ()
  (log:info "=> weathr example")

  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let ((x (topic-exchange "weathr" :auto-delete t)))
        (->
          (queue.declare "" :exclusive t)
          (queue.bind x :routing-key "americas.north.#")
          (subscribe (lambda (message)
                       (log:info "An update for North America: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare "americas.south" :exclusive t)
          (queue.bind x :routing-key "americas.south.#")
          (subscribe (lambda (message)
                       (log:info "An update for South America: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare "us.california" :exclusive t)
          (queue.bind x :routing-key "americas.north.us.ca.*")
          (subscribe (lambda (message)
                       (log:info "An update for US/California: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare "us.tx.austin" :exclusive t)
          (queue.bind x :routing-key "#.tx.austin")
          (subscribe (lambda (message)
                       (log:info "An update for Austin, TX: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare "it.rome" :exclusive t)
          (queue.bind x :routing-key "europe.italy.rome")
          (subscribe (lambda (message)
                       (log:info "An update for Rome, Italy: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare "asia.hk" :exclusive t)
          (queue.bind x :routing-key "asia.southeast.hk.#")
          (subscribe (lambda (message)
                       (log:info "An update for Hong Kong: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))

        (log:info "Publishing")        
        (->
          x
          (publish "San Diego update"      :routing-key "americas.north.us.ca.sandiego")
          (publish "Berkeley update"       :routing-key "americas.north.us.ca.berkeley")
          (publish "San Francisco update"  :routing-key "americas.north.us.ca.sanfrancisco")
          (publish "New York update"       :routing-key "americas.north.us.ny.newyork")
          (publish "São Paolo update"      :routing-key "americas.south.brazil.saopaolo")
          (publish "Hong Kong update"      :routing-key "asia.southeast.hk.hongkong")
          (publish "Kyoto update"          :routing-key "asia.southeast.japan.kyoto")
          (publish "Shanghai update"       :routing-key "asia.southeast.prc.shanghai")
          (publish "Rome update"           :routing-key "europe.italy.roma")
          (publish "Paris update"          :routing-key "europe.france.paris"))

        (log:info "Waiting...")
        (sleep 3)
        (log:info "Disconnecting")))))


