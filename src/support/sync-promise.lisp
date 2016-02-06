(in-package :cl-bunny)

(unless (boundp '+sync-promise-no-values+)
  (define-constant +sync-promise-no-values+ (gensym)))


;; represents sharec connection call for regular sync threads
(defclass sync-promise ()
  ((mutex :initform (bt:make-lock "cl-bunny threaded promise mutex") :reader snyc-promise-mutex)
   (condition-var :initform (bt:make-condition-variable :name "cl-bunny threaded condition variable") :reader sync-promise-condition-var)
   (values :accessor sync-promise-values)
   (error :accessor sync-promise-error)
   (forced-p :accessor sync-promise-forced-p)))

(defun make-sync-promise ()
  (make-instance 'sync-promise))

(defun sync-promise-resolved-or-rejected-p (promise)
  (or (slot-boundp promise 'values)
      (slot-boundp promise 'error)))

(defun sync-promise-resolved-p (promise)
  (slot-boundp promise 'values))

(defun sync-promise-rejected-p (promise)
  (slot-boundp promise 'error))

(defmacro with-sync-promise-lock (promise &body body)
  `(bt:with-lock-held ((snyc-promise-mutex ,promise))
     ,@body))

(defun notify-sync-promise-resolved (promise)
  (bt:condition-notify (sync-promise-condition-var promise)))

(defmethod promise.resolve% ((promise sync-promise) values)
  (with-sync-promise-lock promise
    (unless (sync-promise-resolved-or-rejected-p promise)
      (setf (sync-promise-values promise) values)
      (notify-sync-promise-resolved promise))))

(defmethod promise.reject ((promise sync-promise) error)
  (with-sync-promise-lock promise
    (unless (sync-promise-resolved-or-rejected-p promise)
      (setf (sync-promise-error promise) error)
      (notify-sync-promise-resolved promise))))

(defmethod promise-finished-p ((promise sync-promise))
  (sync-promise-resolved-or-rejected-p promise))

(define-condition sync-promise-timeout (error)
  ())

(define-condition sync-promise-timeout-not-supported (error)
  ())

(defun promise.force% (promise)
  "Actually return/throw"
  (cond
    ((sync-promise-resolved-p promise)
     (values-list (sync-promise-values promise)))
    ((sync-promise-rejected-p promise)
     (error (sync-promise-error promise)))))

(defun promise.force (promise &key timeout)
  "Block until promise rejected/resolved or timeout expired.
If promise rejected respective error will be thrown in calling thread.
If promise resolved returns respective values
If timeout expires throws THREADED-PROMISE-TIMEOUT"
  (with-sync-promise-lock promise
    (unless (sync-promise-resolved-or-rejected-p promise)
      ;; it can wakeup before timout elapsed or even before promise actually fulfilled
      ;; probably reimplement using %decrement-semapthore idea from sbcl
      (let ((time (get-universal-time))) ;; just illustration
        (loop
          as ret = (bt:condition-wait (sync-promise-condition-var promise)
                                      (snyc-promise-mutex promise)
                                      :timeout timeout)
          do ;; promise can be in default state because of
             ;; timeout or just spurious wakeup
             (when (sync-promise-resolved-or-rejected-p promise)
               (return))
             (unless ret
               ;; timeout
               (when (>= (get-universal-time) (+ time timeout))
                 (error 'sync-promise-timeout))
               (unless (sync-promise-resolved-or-rejected-p promise))))))
    (promise.force% promise)))
