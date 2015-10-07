(in-package :cl-bunny)

(defun properties-headers (properties)
  (assoc-value properties :headers))


(defun header-value (headers name)
  (assoc-value headers name :test #'equal))
