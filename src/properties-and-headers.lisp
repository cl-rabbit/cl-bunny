(in-package :cl-bunny)

(defun properties-headers (properties)
  (amqp-property-headers properties))


(defun header-value (headers name)
  (assoc-value headers name :test #'equal))
