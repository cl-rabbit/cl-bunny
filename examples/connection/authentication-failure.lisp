(in-package :cl-bunny.examples)

(defun authentication-failure ()
  (handler-case
      (with-connection "amqp://guest8we78w7e8:guest2378278@127.0.0.1")
    (authentication-error (e)
      (log:error "Could not authenticate as ~a" (connection-spec-login (connection-spec (error-connection e)))))))
