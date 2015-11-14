(in-package :cl-bunny)

(defclass connections-pool ()
  ((storage :initform (make-hash-table :test #'equal):reader connections-pool-storage)
   (lock :initform (bt:make-recursive-lock "CL-BUNNY connections pool lock") :reader connections-pool-lock)
   ;; events
   #|
   on-connection-added
   on-connection-removed
   on-connection-open
   on-connection-close
   |#
   ))

(defmethod print-object ((object connections-pool) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (let ((connections (alexandria:hash-table-values (connections-pool-storage *connections-pool*))))
      (format stream "Connections: ~a, Open: ~a"
              (length connections)
              (count-if 'connection-open-p connections)))))

(defparameter *connections-pool* (make-instance 'connections-pool))

(defun get-connection-from-pool (spec)
  (check-connection-alive (gethash spec (connections-pool-storage *connections-pool*))))

(defun add-connection-to-pool (spec connection)
  (bt:with-recursive-lock-held ((connections-pool-lock *connections-pool*))
    (setf (gethash spec (connections-pool-storage *connections-pool*))
          connection
          (connection-pool connection)
          *connections-pool*))
  connection)

(defun remove-connection-from-pool (connection)
  (when (connection-pool connection)
    (bt:with-recursive-lock-held ((connections-pool-lock (connection-pool connection)))
      (remhash (connection-spec connection) (connections-pool-storage (connection-pool connection)))))) 

(defun find-or-run-new-connection (spec)
  (bt:with-recursive-lock-held ((connections-pool-lock *connections-pool*))
    (or (get-connection-from-pool spec)
        (add-connection-to-pool spec (run-new-connection spec)))))
