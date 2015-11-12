(in-package :cl-bunny.examples)

(defun weathr ()
  (log:info "=> weathr example")

  (with-connection ("amqp://")
    (with-channel ()
      (let ((x (exchange.topic "weathr" :auto-delete t)))
        (->
          (queue.declare :exclusive t)
          (queue.bind x :routing-key "americas.north.#")
          (subscribe (lambda (message)
                       (log:info "An update for North America: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare :name "americas.south" :exclusive t)
          (queue.bind x :routing-key "americas.south.#")
          (subscribe (lambda (message)
                       (log:info "An update for South America: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare :name "us.california" :exclusive t)
          (queue.bind x :routing-key "americas.north.us.ca.*")
          (subscribe (lambda (message)
                       (log:info "An update for US/California: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare :name "us.tx.austin" :exclusive t)
          (queue.bind x :routing-key "#.tx.austin")
          (subscribe (lambda (message)
                       (log:info "An update for Austin, TX: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare :name "it.rome" :exclusive t)
          (queue.bind x :routing-key "europe.italy.rome")
          (subscribe (lambda (message)
                       (log:info "An update for Rome, Italy: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))
        (->
          (queue.declare :name "asia.hk" :exclusive t)
          (queue.bind x :routing-key "asia.southeast.hk.#")
          (subscribe (lambda (message)
                       (log:info "An update for Hong Kong: ~a, routing key is ~a"
                                 (babel:octets-to-string (message-body message))
                                 (message-routing-key message)))))

        (log:info "Publishing")        
        (publish x "San Diego update"      :routing-key "americas.north.us.ca.sandiego")
        (publish x "Berkeley update"       :routing-key "americas.north.us.ca.berkeley")
        (publish x "San Francisco update"  :routing-key "americas.north.us.ca.sanfrancisco")
        (publish x "New York update"       :routing-key "americas.north.us.ny.newyork")
        (publish x "São Paolo update"      :routing-key "americas.south.brazil.saopaolo")
        (publish x "Hong Kong update"      :routing-key "asia.southeast.hk.hongkong")
        (publish x "Kyoto update"          :routing-key "asia.southeast.japan.kyoto")
        (publish x "Shanghai update"       :routing-key "asia.southeast.prc.shanghai")
        (publish x "Rome update"           :routing-key "europe.italy.roma")
        (publish x "Paris update"          :routing-key "europe.france.paris")

        (log:info "Waiting...")
        (sleep 3)
        (log:info "Disconnecting")))))


