(defpackage :cl-bunny
  (:use :cl :alexandria :safe-queue :eventfd)
  (:nicknames :bunny)
  (:export #:with-connection
           #:with-channel
           #:with-consumers

           #:message-channel
           #:message-consumer-tag
           #:message-delivery-tag
           #:message-redelivered
           #:message-exchange
           #:message-routing-key
           #:message-properties
           #:message-body

           #:amqp-queue-declare
           #:amqp-queue-bind
           #:amqp-queue-delete

           #:amqp-exchange-declare
           #:amqp-exchange-delete
           
           #:amqp-basic-publish
           #:amqp-basic-consume

           ;; high-level interfaces
           #:exchange-on-return-callback

           #:queue.declare
           #:queue.bind
           
           #:default-exchange
           #:topic-exchange
           #:fanout-exchange
           #:publish          
           #:consume
           #:subscribe))

