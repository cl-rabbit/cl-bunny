(ql:quickload :cl-bunny.examples)
(ql:quickload :nibbles)

(in-package :cl-bunny.examples)

(defun int32-to-octet(val)
  (declare (type (signed-byte 32) val)
           (optimize (sb-c::insert-debug-catch 0)  
                     (speed 3) 
                     (compilation-speed 0)
                     (safety 0)))
  (let ((array (make-array 4 :element-type '(unsigned-byte 8))))
    (setf (aref array 0) (ldb (byte 8 0) val))
    (setf (aref array 1) (ldb (byte 8 8) val))
    (setf (aref array 2) (ldb (byte 8 16) val))
    (setf (aref array 3) (ldb (byte 8 24) val))
    array))

(defun fibonacci (n)
  (if (< n 3)
      1
      (+ (fibonacci (- n 1)) (fibonacci (- n 2)))))

(with-connection ("amqp://")
  (with-channel ()
    (let ((x (default-exchange))
          (q (queue.declare "rpc_queue" :auto-delete t)))
      (format t " [x] Awaiting RPC requests~%")
      (subscribe q (lambda (message)                     
                     (let ((n (nibbles:sb32ref/le (coerce (message-body message) '(vector (unsigned-byte 8))) 0)))
                       (publish x
                                (int32-to-octet (fibonacci n ))
                                :routing-key (alexandria::assoc-value (message-properties message) :reply-to)
                                :properties `((:correlation-id . ,(alexandria::assoc-value (message-properties message) :correlation-id))))))
                 :type :sync)
      (consume))))
                                

