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
  :components ((:module "test"
                :serial t
                :components
                ((:file "package"))))
  :defsystem-depends-on (:prove-asdf)
  :perform (test-op :after (op c)
                    (funcall (intern #.(string :run-test-system) :prove-asdf) c)))
