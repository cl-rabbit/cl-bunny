(defpackage :cl-bunny
  (:use :cl :alexandria :eventfd :string-case :amqp :cl-events)
  (:nicknames :bunny)
  (:export #:with-connection
           #:with-channel
           #:with-consumers

           #:*force-timeout*

           ;; conditions
           #:authentication-error
           #:error-connection
           #:connection-closed-error
           #:channel-closed-error
           #:sync-promise-timeout
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

           ;; connections pool
           #:*connections-pool*
           #:connections-pool-base
           #:eq-connections-pool
           #:connections-pool.get
           #:connections-pool.add
           #:connections-pool.remove
           #:connections-pool.find-or-run

           #:*connection*
           #:*connection-type*
           #:*notification-lambda*
           #:connection.new
           #:connection.open
           #:connection.close
           #:connection.consume

           #:connection-spec
           #:connection-spec-login
           #:connection-spec-password
           #:connection-spec-host
           #:connection-spec-port
           #:connection-spec-vhost
           #:connection-spec-use-tls-p
           #:connection-spec-use-ipv6-p
           #:connection-spec-channel-max
           #:connection-spec-frame-max
           #:connection-spec-heartbeat-interval
           #:connection-open-p
           #:connection-channel-max
           #:connection-frame-max
           #:connection-heartbeat
           #:connection-server-properties
           #:connection-on-close
           #:connection-on-blocked
           #:connection-on-unblocked

           #:*channel*
           #:channel-id
           #:channel-connection
           #:channel-open-p
           #:channel-on-error
           #:channel-on-return
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

           #:exchange-channel
           #:exchange-name
           #:exchange-type
           #:exchange-durable-p
           #:exchange-auto-delete-p
           #:exchange-internal-p
           #:exchange-arguments
           #:exchange-on-return
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

           #:*event-executor*))
