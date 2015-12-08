(in-package :cl-bunny)

(unless (boundp '+threaded-promise-no-values+)
  (define-constant +threaded-promise-no-values+ (gensym)))

(defclass threaded-promise ()
  ((mutex :initform (bt:make-lock "cl-bunny threaded promise mutex") :reader threaded-promise-mutex)
   (condition-var :initform (bt:make-condition-variable :name "cl-bunny threaded condition variable") :reader threaded-promise-condition-var)
   (values :accessor threaded-promise-values)
   (error :accessor threaded-promise-error)
   (forced-p :accessor threaded-promise-forced-p)))

(defun threaded-promise ()
  (make-instance 'threaded-promise))

(defun threaded-promise-resolved-or-rejected-p (promise)
  (or (slot-boundp promise 'values)
      (slot-boundp promise 'error)))

(defun threaded-promise-resolved-p (promise)
  (slot-boundp promise 'values))

(defun threaded-promise-rejected-p (promise)
  (slot-boundp promise 'error))

(defmacro with-threaded-promise-lock (promise &body body)
  `(bt:with-lock-held ((threaded-promise-mutex ,promise))
     ,@body))

(defun notify-threaded-promise-resolved (promise)
  (bt:condition-notify (threaded-promise-condition-var promise)))

(defmacro promise.resolve (promise &body values)
  (with-gensyms (%promise)
    `(let ((,%promise ,promise))
       (with-threaded-promise-lock ,%promise
         (unless (threaded-promise-resolved-or-rejected-p ,%promise)
           (setf (threaded-promise-values ,%promise)
                 (multiple-value-list (progn ,@values)))
           (notify-threaded-promise-resolved ,%promise))))))

(defun promise.reject (promise error)
  (with-threaded-promise-lock promise
    (unless (threaded-promise-resolved-or-rejected-p promise)
      (setf (threaded-promise-error promise) error)
      (notify-threaded-promise-resolved promise))))

(define-condition threaded-promise-timeout (error)
  ())

(define-condition threaded-promise-timeout-not-supported (error)
  ())

(defun promise.force% (promise)
  "Actually return/throw"
  (cond
    ((threaded-promise-resolved-p promise)
     (values-list (threaded-promise-values promise)))
    ((threaded-promise-rejected-p promise)
     (error (threaded-promise-error promise)))))

(defun promise.force (promise &key timeout)
  "Block until promise rejected/resolved or timeout expired.
If promise rejected respective error will be thrown in calling thread.
If promise resolved returns respective values
If timeout expires throws THREADED-PROMISE-TIMEOUT"
  (with-threaded-promise-lock promise
    (unless (threaded-promise-resolved-or-rejected-p promise)
      ;; it can wakeup before timout elapsed or even before promise actually fulfilled
      ;; probably reimplement using %decrement-semapthore idea from sbcl
      (let ((time (get-universal-time))) ;; just illustration
        (loop
          as ret = (bt:condition-wait (threaded-promise-condition-var promise)
                                      (threaded-promise-mutex promise)
                                      :timeout timeout)
          do ;; promise can be in default state because of
             ;; timeout or just spurious wakeup
             (when (threaded-promise-resolved-or-rejected-p promise)
               (return))
             (unless ret
               ;; timeout
               (when (>= (get-universal-time) (+ time timeout))
                 (error 'threaded-promise-timeout))
               (unless (threaded-promise-resolved-or-rejected-p promise))))))
    (promise.force% promise)))
