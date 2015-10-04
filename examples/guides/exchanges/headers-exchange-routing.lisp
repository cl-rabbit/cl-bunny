(in-package :cl-bunny.examples)

(defun headers-exchange-routing ()
  (log:info "=> Headers exchange routing")

  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let ((x (amqp-exchange-declare "headers" :type "headers"))
            (q1 (amqp-queue-declare "" :exclusive t))
            (q2 (amqp-queue-declare "" :exclusive t)))
        (amqp-queue-bind q1 :exchange x :arguments '(("os" . "linux")
                                                     ("cores" . 8)
                                                     ("x-match" . "all")))
        (amqp-queue-bind q2 :exchange x :arguments '(("os" . "osx")
                                                     ("cores" . 4)
                                                     ("x-match" . "any")))
        (subscribe q1 (lambda (message)
                        (log:info "~a received ~a" q1 (babel:octets-to-string (message-body message)))))
        (subscribe q2 (lambda (message)
                        (log:info "~a received ~a" q2 (babel:octets-to-string (message-body message)))))

        (amqp-basic-publish "8 cores/Linux" :exchange x
                                            :properties '((:headers . (("os" . "linux")
                                                                       ("cores" . 8)))))
        (amqp-basic-publish "8 cores/OS X" :exchange x
                                           :properties '((:headers . (("os" . "osx")
                                                                      ("cores" . 8)))))
        (amqp-basic-publish "4 cores/Linux" :exchange x
                                            :properties '((:headers . (("os" . "linux")
                                                                       ("cores" . 4)))))

        (log:info "Waiting...")
        (sleep 3)
        (log:info "Disconnecting")))))
