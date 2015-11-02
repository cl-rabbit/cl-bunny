(in-package :cl-user)

(defpackage :cl-bunny.test
  (:use :cl
        :alexandria
        :prove
        :cl-bunny
        :blackbird)
  (:shadowing-import-from :prove
                          :*debug-on-error*))

(in-package :cl-bunny.test)
