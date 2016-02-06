(in-package :cl-bunny)

;; operations
(defgeneric promise.reject (promise error))
(defgeneric promise.resolve% (promise values))
(defgeneric promise-finished-p (promise))

;; "normal" async promise e.g. to be used inside async thread with private connection
(defclass promise (bb:promise)
  ())

(defun make-promise ()
  (bb::make-promise))

(defmethod promise.reject ((promise promise) error)
  (bb:signal-error promise error))

(defmethod promise.resolve% ((promise promise) values)
  (apply #'bb::finish promise values))

(defmacro promise.resolve (promise &body values)
  `(promise.resolve% ,promise (multiple-value-list (progn ,@values))))

(defmethod promise-finished-p (promise)
  (bb:promise-finished-p promise))
