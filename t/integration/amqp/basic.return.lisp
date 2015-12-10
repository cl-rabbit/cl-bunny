(in-package :cl-bunny.test)

(plan 1)

(subtest "Basic.return tests"
  (with-connection ()
    (with-channel ()
      (let ((x (exchange.default))
            (returned))

        (queue.declare-temp)

        (event+ (exchange-on-return x)
                (lambda (returned-message)
                  (setf returned returned-message)))

        (publish x "This will be returned" :mandatory t :routing-key (format nil "wefvvtrw~a" (random 10)))

        (sleep 1)
        (isnt returned nil "Message returned")
        (is (message-body-string returned) "This will be returned")))))

(finalize)
