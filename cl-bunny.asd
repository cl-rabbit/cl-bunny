(in-package :cl-user)

(defpackage :cl-bunny.system
  (:use :cl :asdf))

(in-package :cl-bunny.system)

(defsystem :cl-bunny
  :version "0.2.1"
  :description "High-level interface for cl-rabbit"
  :maintainer "Ilya Khaprov <ilya.khaprov@publitechs.com>"
  :author "Ilya Khaprov <ilya.khaprov@publitechs.com>"
  :licence "MIT"
  :depends-on ("alexandria"
               "string-case"
               "cl-amqp"
               "iolib"
               "cl-rabbit"
               "quri"
               "lparallel"
               "safe-queue"
               "eventfd"
               "cl-events"
               "log4cl")
  :components ((:module "src"
                :serial t
                :components
                ((:file "package")
                 (:module "support"
                  :serial t
                  :components
                  ((:file "pipe")
                   (:file "int-allocator")
                   (:file "channel-id-allocator")
                   (:file "threaded-promise")
                   (:file "bunny-event")))
                 (:file "conditions")
                 (:file "properties-and-headers")
                 (:module "connection"
                  :serial t
                  :components
                  ((:file "spec")
                   (:file "pool")
                   (:file "connection-base")
                   (:file "shared-connection")
                   (:file "librabbitmq")
                   (:file "channel")))
                 (:file "message")
                 (:file "queue")
                 (:file "exchange")
                 (:file "consumer")
                 (:file "basic")
                 (:file "confirm")
                 (:file "tx")
                 (:file "printer")))))
