(in-package :cl-bunny)

(defclass connection-in-pool ()
  ((pool-tag :initarg :pool-tag :accessor connection-pool-tag)
   (pool :initform nil :accessor connection-pool)))

(defclass connections-pool-base ()
  ())

(defgeneric connections-pool.get% (pool spec))

(defgeneric connections-pool.add% (pool connection))

(defgeneric connections-pool.remove% (pool connection))

(defgeneric connections-pool.find-or-run% (pool spec))

(defclass eq-connections-pool (connections-pool-base)
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

(defmethod connections-pool.get% ((pool eq-connections-pool) spec)
  (gethash spec (connections-pool-storage pool)))

(defmethod connections-pool.add% ((pool eq-connections-pool) connection)
  (assert (null (gethash (connection-pool-tag connection) (connections-pool-storage pool))) nil
          "Connection with spec ~a already added to pool ~a" connection pool) ;; TODO: specialize error
  (bt:with-recursive-lock-held ((connections-pool-lock pool))
    (setf (gethash (connection-pool-tag connection) (connections-pool-storage pool))
          connection
          (connection-pool connection)
          pool)
    connection))

(defmethod connections-pool.remove% ((pool eq-connections-pool) (connection connection-in-pool))
  (assert (connection-pool connection) nil "Connection pool is nil") ;; TODO: specialize error
  (bt:with-recursive-lock-held ((connections-pool-lock (connection-pool connection)))
    (multiple-value-prog1
        (remhash (connection-pool-tag connection) (connections-pool-storage (connection-pool connection)))
    (setf (connection-pool connection) nil))))

(defmethod connections-pool.remove% ((pool eq-connections-pool) tag)
  (bt:with-recursive-lock-held ((connections-pool-lock pool))
    (remhash tag (connections-pool-storage pool))))

(defmethod connections-pool.find-or-run% ((pool eq-connections-pool) spec)
  (bt:with-recursive-lock-held ((connections-pool-lock pool))
    (or (connections-pool.get% pool spec)
        (connections-pool.add% pool (connection.open (connection.new spec))))))


(defparameter *connections-pool* (make-instance 'eq-connections-pool))

(defmethod print-amqp-object ((pool eq-connections-pool) stream)
  (let ((connections (alexandria:hash-table-values (connections-pool-storage pool))))
      (format stream "Connections: ~a, Open: ~a"
              (length connections)
              (count-if 'connection-open-p connections))))

(defmethod print-object ((object eq-connections-pool) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (print-amqp-object object stream)))

(defun connections-pool.get (spec &key (pool *connections-pool*))
  (connections-pool.get% pool spec))

(defun connections-pool.add (connection &key (pool *connections-pool*))
  (connections-pool.add% pool connection))

(defun connections-pool.remove (connection-or-tag &key (pool *connections-pool*))
  (connections-pool.remove% pool connection-or-tag))

(defun connections-pool.find-or-run (spec &key (pool *connections-pool*))
  (connections-pool.find-or-run% pool spec))
