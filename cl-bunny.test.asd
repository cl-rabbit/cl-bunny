(in-package :cl-user)

(defpackage :cl-bunny.test.system
  (:use :cl :asdf))

(in-package :cl-bunny.test.system)

(defsystem :cl-bunny.test
  :version "0.1"
  :description "Tests for cl-bunny"
  :maintainer "Ilya Khaprov <ilya.khaprov@publitechs.com>"
  :author "Ilya Khaprov <ilya.khaprov@publitechs.com> and CONTRIBUTORS"
  :licence "MIT"
  :depends-on ("cl-bunny"
               "prove"
               "log4cl"
               "cl-interpol")
  :serial t
  :components ((:module "t"
                :serial t
                :components
                ((:file "package")
                 (:test-file "dummy")
                 (:module "utils"
                  :serial t
                  :components
                  ((:file "printer")))
                 (:module "unit"
                  :serial t
                  :components
                  ((:test-file "channel-send")
                   (:test-file "connection-spec")
                   (:test-file "connection-pool")))
                 (:module "integration"
                  :serial t
                  :components
                  ((:test-file "errors")
                   (:test-file "basic")
                   (:test-file "channel")
                   (:test-file "connection")
                   (:test-file "confirm")
                   (:test-file "tx")
                   (:module "exchanges"
                    :serial t
                    :components
                    ((:test-file "fanout"))))))))
  :defsystem-depends-on (:prove-asdf)
  :perform (test-op :after (op c)
                    (funcall (intern #.(string :run-test-system) :prove-asdf) c)))
