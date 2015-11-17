(in-package :cl-bunny.test)

(defun print-amqp-object-to-string (object)
  (with-output-to-string (stream)
    (bunny::print-amqp-object object stream)))
