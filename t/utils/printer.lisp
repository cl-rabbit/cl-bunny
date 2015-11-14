(in-package :cl-bunny.test)

(defun actually-print-connection-spec (spec)
  (with-output-to-string (stream)
    (bunny::actually-print-connection-spec spec stream)))
