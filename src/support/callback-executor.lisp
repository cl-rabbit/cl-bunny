(in-package :cl-bunny)

(defparameter *callback-executor* nil)

(defgeneric execute-callback (callback-executor callback args))

(defun execute-callback% (callback args)
  (assert *callback-executor* (*callback-executor*) "callback-executor is not set") ;; TODO: specialize error
  (execute-callback *callback-executor* callback args))

(defun maybe-execute-callback (callback &rest args)
  (when callback
    (execute-callback% callback args)))

;; lparallel

(defun create-lparallel-callback-executor ()
  (setf *callback-executor* (lparallel:make-kernel 5)))

(defmethod execute-callback ((kernel lparallel:kernel) callback args)
  (lparallel.kernel::submit-raw-task (lparallel.kernel::make-task-instance
                                      (lambda ()
                                        (apply callback args))
                                      :cl-bunny-callback)
                                     kernel))
