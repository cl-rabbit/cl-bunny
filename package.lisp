(defpackage :cl-bunny
  (:use :cl :alexandria :safe-queue :eventfd :amqp)
  (:nicknames :bunny)
  (:export #:with-connection
           #:with-channel
           #:with-consumers

           #:message-channel
           #:message-consumer-tag
           #:message-delivery-tag
           #:message-redelivered-p
           #:message-exchange
           #:message-routing-key
           #:message-properties
           #:message-body
           #:message-ack
           #:message-header-value

           #:amqp-queue-declare
           #:amqp-queue-bind
           #:amqp-queue-delete

           #:amqp-exchange-declare
           #:amqp-exchange-delete

           #:amqp-basic-publish
           #:amqp-basic-consume

           ;; conditions
           #:authentication-error
           #:error-connection

           ;; high-level interfaces
           #:properties-headers
           #:header-value

           #:new-connection
           #:connection-start
           #:connection-close

           #:connection-spec
           #:connection-spec-login
           #:connection-spec-password
           #:connection-spec-host
           #:connection-spec-port
           #:connection-spec-vhost

           #:new-channel
           #:channel-open
           #:channel-prefetch
           #:channel-send

           #:exchange-on-return-callback

           #:queue.declare
           #:queue.bind
           #:queue.purge
           #:queue.unbind
           #:queue.delete

           #:+default-exchange+
           #:exchange.default
           #:exchange.topic
           #:exchange.fanout
           #:exchagne.direct
           #:exchange.headers
           #:exchange.declare
           #:exchange.delete
           #:exchange.bind
           #:exchange.unbind

           #:publish
           #:consume
           #:subscribe))
