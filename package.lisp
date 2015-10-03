(defpackage :cl-bunny
  (:use :cl :alexandria :safe-queue :eventfd)
  (:nicknames :bunny)
  (:export #:with-connection
           #:with-channel
           #:with-consumers

           #:message-body

           #:amqp-queue-declare
           #:amqp-basic-publish
           #:amqp-basic-consume
           
           #:consume))

