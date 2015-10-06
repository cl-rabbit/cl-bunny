(in-package :cl-user)

(defpackage :cl-bunny.examples.system
  (:use :cl :asdf))

(in-package :cl-bunny.examples.system)

(defsystem :cl-bunny.examples
  :version "0.1"
  :description "CL-BUNNY examples"  
  :maintainer "Ilya Khaprov <ilya.khaprov@publitechs.com>"
  :author "Ilya Khaprov <ilya.khaprov@publitechs.com>"
  :licence "MIT"
  :depends-on ("cl-bunny"
               "log4cl")
  :serial t
  :components ((:file "examples/package")
               (:file "examples/guides/getting-started/weathr")
               (:file "examples/guides/getting-started/blabbr")
               (:file "examples/guides/getting-started/hello-world")
               (:file "examples/guides/exchanges/direct-exchange-routing")
               (:file "examples/guides/exchanges/fanout-exchange-routing")
               (:file "examples/guides/exchanges/headers-exchange-routing")
               (:file "examples/guides/exchanges/mandatory-messages")
               (:file "examples/guides/queues/redeliveries")))
