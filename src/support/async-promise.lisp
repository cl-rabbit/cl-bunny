(in-package :cl-bunny)

;; represents shared connection call result for async caller
(defclass async-promise (promise)
  ((notify-lambda :initarg :notify-lambda :reader async-promise-notify-lambda)
   (private-resolved :initform nil)))

(defun make-async-promise (notify-lambda)
  (make-instance 'async-promise :notify-lambda notify-lambda))

;; this should be called only by shared connection thread
(defmethod promise.reject ((promise async-promise) error)
  (assert (not (bb:promise-finished-p promise)))
  (funcall (async-promise-notify-lambda promise) (lambda () (apply #'bb::signal-error promise error))))

;; this should be called only by shared connection thread
(defmethod promise.resolve% ((promise async-promise) values)
  (assert (not (bb:promise-finished-p promise)))
  (funcall (async-promise-notify-lambda promise) (lambda () (apply #'bb::finish promise values))))
