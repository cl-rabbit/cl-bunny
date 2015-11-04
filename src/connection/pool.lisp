(in-package :cl-bunny)

(defparameter *connections-pool* (make-hash-table :test #'equal))
;; TODO: maybe replace with synchronized hash-table on sbcl?
(defvar *connections-pool-lock* (bt:make-lock "CL-BUNNY connections pool lock"))

(defun get-connection-from-pool (spec)
  (check-connection-alive (gethash spec *connections-pool*)))

(defun add-connection-to-pool (spec connection)
  (setf (gethash spec *connections-pool*)
        connection))

(defun remove-connection-from-pool (connection)
  (bt:with-lock-held (*connections-pool-lock*)
    (remhash (connection-spec connection) *connections-pool*)))

(defun find-or-run-new-connection (spec)
  (bt:with-lock-held (*connections-pool-lock*)
    (or (get-connection-from-pool spec)
        (add-connection-to-pool spec (run-new-connection spec)))))
