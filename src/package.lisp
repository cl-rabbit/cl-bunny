(defpackage :cl-bunny
  (:use :cl :alexandria :eventfd :string-case :amqp)
  (:nicknames :bunny)
  (:export #:with-connection
           #:with-channel
           #:with-consumers

           #:*force-timeout*

           #:message-channel
           #:message-consumer-tag
           #:message-consumer
           #:message-delivery-tag
           #:message-redelivered-p
           #:message-exchange
           #:message-routing-key
           #:message-properties
           #:message-body
           #:message-body-string
           #:message.ack
           #:message.nack
           #:message-header-value
           #:message-property-value

           ;; conditions
           #:authentication-error
           #:error-connection
           #:connection-closed-error
           #:channel-closed-error
           #:threaded-promise-timeout
           #:transport-error
           #:network-error

           ;; high-level interfaces
           #:message-content-type
           #:message-content-encoding
           #:message-headers
           #:message-persistent-p
           #:message-priority
           #:message-correlation-id
           #:message-reply-to
           #:message-expiration
           #:message-message-id
           #:message-timestamp
           #:message-type
           #:message-user-id
           #:message-app-id
           #:message-cluster-id
           #:message-header-value
           #:header-value

           ;; connections pool
           #:*connections-pool*
           #:connections-pool-base
           #:eq-connections-pool
           #:connections-pool.get
           #:connections-pool.add
           #:connections-pool.remove
           #:connections-pool.find-or-run

           #:*connection*
           #:connection.new
           #:connection.open
           #:connection.close

           #:connection-spec
           #:connection-spec-login
           #:connection-spec-password
           #:connection-spec-host
           #:connection-spec-port
           #:connection-spec-vhost
           #:connection-spec-use-tls-p
           #:connection-spec-use-ipv6-p
           #:connection-open-p
           #:connection-channel-max
           #:connection-frame-max
           #:connection-heartbeat
           #:connection-server-properties

           #:*channel*
           #:channel-id
           #:channel-connection
           #:channel-open-p
           #:channel-on-error-callback
           #:channel.new
           #:channel.open
           #:channel.flow
           #:channel.new.open
           #:channel.send
           #:channel.confirm
           #:channel.tx
           #:channel.wait-confirms
           #:channel.close

           #:tx.commit
           #:tx.rollback

           #:exchange-on-return-callback

           #:queue-name
           #:queue-server-named-p
           #:queue-durable-p
           #:queue-exclusive-p
           #:queue-auto-delete-p
           #:queue-arguments
           #:queue.exists-p
           #:queue.declare
           #:queue.declare-temp
           #:queue.bind
           #:queue.purge
           #:queue.unbind
           #:queue.delete
           #:queue.put
           #:queue.peek
           #:queue.get
           #:queue.state
           #:queue.message-count
           #:queue.consumer-count

           #:exchange.exists-p
           #:exchange.default
           #:exchange.topic
           #:exchange.fanout
           #:exchange.direct
           #:exchange.headers
           #:exchange.match
           #:exchange.declare
           #:exchange.delete
           #:exchange.bind
           #:exchange.unbind

           #:publish
           #:consume
           #:subscribe
           #:subscribe-sync
           #:unsubscribe
           #:qos

           #:->

           #:execute-callback
           #:*callback-executor*))
