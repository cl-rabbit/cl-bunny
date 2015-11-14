(in-package :cl-bunny.examples)

(defun connection-blocked ()
  (with-connection ()
    (with-channel ()
      (let ((x (exchange.fanout "amq.fanout"))
            (content (nibbles:make-octet-vector (* 1024 1024 16))))
        (publish x content)))))
