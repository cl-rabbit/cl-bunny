(defpackage :cl-bunny
  (:use :cl :alexandria :safe-queue :eventfd)
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

           ;; high-level interfaces
           #:properties-headers
           #:header-value
           
           #:new-connection
           #:connection-start
           #:connection-close

           #:new-channel
           #:channel-open
           #:channel-prefetch
           
           #:exchange-on-return-callback

           #:queue.declare
           #:queue.bind
           #:queue.purge
           
           #:default-exchange
           #:topic-exchange
           #:fanout-exchange
           #:direct-exchange
           #:headers-exchange
           #:publish          
           #:consume
           #:subscribe))

