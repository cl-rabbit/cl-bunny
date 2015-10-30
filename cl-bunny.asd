(in-package :cl-user)

(defpackage :cl-bunny.system
  (:use :cl :asdf))

(in-package :cl-bunny.system)

(defsystem :cl-bunny
  :version "0.1"
  :description "High-level interface for cl-rabbit"  
  :maintainer "Ilya Khaprov <ilya.khaprov@publitechs.com>"
  :author "Ilya Khaprov <ilya.khaprov@publitechs.com>"
  :licence "MIT"
  :depends-on ("alexandria"
               ;; "cl-amqp"
               "iolib"
               "cl-rabbit"
               "quri"
               "safe-queue"
               "eventfd"
               "log4cl")
  :serial t
  :components ((:file "package")
               (:file "src/support/int-allocator")
               (:file "src/support/channel-id-allocator")
               (:file "src/conditions")
               (:file "src/properties-and-headers")
               (:file "src/message")
               (:file "src/connection/spec")
               (:file "src/connection/exchange")
               (:file "src/queue")
               (:file "src/connection/channel")
               (:file "src/connection/connection")
               (:file "src/consumer")
               (:file "src/amqp")))
