(ql:quickload :cl-bunny.examples)
(ql:quickload :nibbles)

(in-package :cl-bunny.examples)

(defun int64-to-octets(val)
  (let ((obuffer (fast-io:make-output-buffer)))
    (fast-io:write64-be val obuffer)
    (fast-io:finish-output-buffer obuffer)))

;; http://www.cliki.net/fibonacci
(defun fibonacci (n)
  "Successive squaring method from SICP"
  (check-type n (integer 0 *))
  (labels ((fib-aux (a b p q count)
             (cond ((= count 0) b)
                   ((evenp count)
                    (fib-aux a
                             b
                             (+ (* p p) (* q q))
                             (+ (* q q) (* 2 p q))
                             (/ count 2)))
                   (t (fib-aux (+ (* b q) (* a q) (* a p))
                               (+ (* b p) (* a q))
                               p
                               q
                               (- count 1))))))
    (fib-aux 1 0 0 1 n)))

(with-connection ()
  (with-channel ()
    (let ((x (exchange.default))
          (q (queue.declare :name "rpc_queue" :auto-delete t)))
      (format t " [x] Awaiting RPC requests~%")
      (subscribe q (lambda (message)
                     (let ((n (nibbles:sb64ref/be (message-body message) 0)))
                       (publish x
                                (int64-to-octets (fibonacci n))
                                :routing-key (message-reply-to message)
                                :properties `(:correlation-id ,(message-correlation-id message)))))
                 :type :sync)
      (consume))))
