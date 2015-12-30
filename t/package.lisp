(in-package :cl-user)

(defpackage :cl-bunny.test
  (:use :cl
        :alexandria
        :prove
        :cl-events
        :cl-bunny)
  (:shadowing-import-from :prove
                          :*debug-on-error*))

(in-package :cl-bunny.test)

(setf *connection-type* 'bunny::shared-librabbitmq-connection)
