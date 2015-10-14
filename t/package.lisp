(in-package :cl-user)

(defpackage :cl-bunny.test
  (:use :cl
        :alexandria
        :5am))

(in-package :cl-bunny.test)

(def-suite :cl-bunny
  :description "Main test suite for CL-BUNNY")

(def-suite :cl-bunny.support
  :description "CL-BUNNY support stuff tests")
