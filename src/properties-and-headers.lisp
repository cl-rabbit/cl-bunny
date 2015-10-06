(in-package :cl-bunny)

(defun properties-headers (properties)
  (assoc-value properties :headers))


(defun header-value (headers name)
  (error "Not implemented"))
