(in-package :cl-user)

(defpackage :cl-bunny.examples
  (:use :cl :cl-bunny :amqp :alexandria)
  (:nicknames :bunny.examples)
  (:export #:direct-exchange-routing
           #:direct-exchange-routing-sync
           #:fanout-exchange-routing
           #:headers-exchange-routing
           #:mandatory-messages

           #:blabbr
           #:weathr
           #:hello-world
           #:hello-world-sync
           #:hello-world-raw

           #:redeliveries

           #:authentication-error))
