(in-package :cl-user)

(defpackage :cl-bunny.examples
  (:use :cl :cl-bunny :amqp :alexandria :blackbird)
  (:nicknames :bunny.examples)
  (:export #:fanout-exchange-routing
           #:headers-exchange-routing
           #:direct-exchange-routing
           #:direct-exchange-routing-sync

           #:blabbr
           #:weathr
           #:hello-world
           #:hello-world-sync))
