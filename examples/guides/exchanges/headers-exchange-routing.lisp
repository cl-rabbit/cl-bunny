(in-package :cl-bunny.examples)

(defun headers-exchange-routing ()
  (log:info "=> Headers exchange routing")

  (with-connection ("amqp://")
    (with-channel ()
      (let* ((x (exchange.headers "headers" :auto-delete t))
             (q1 (->
                   (queue.declare :exclusive t)
                   (queue.bind x :arguments '(("os" . "linux")
                                              ("cores" . 8)
                                              ("x-match" . "all")))))
             (q2 (->
                   (queue.declare :exclusive t)
                   (queue.bind x :arguments '(("os" . "osx")
                                              ("cores" . 4)
                                              ("x-match" . "any"))))))
        (subscribe q1 (lambda (message)
                        (log:info "~a received ~a" q1 (message-body-string message))))


        (subscribe q2 (lambda (message)
                        (log:info "~a received ~a" q2 (message-body-string message))))

        (publish x "8 cores/Linux" :properties '(:headers (("os" . "linux")
                                                         ("cores" . 8))))
        (publish x "8 cores/Linux" :properties '(:headers (("os" . "linux")
                                                         ("cores" . 8))))
        (publish x "8 cores/OS X"  :properties '(:headers (("os" . "osx")
                                                         ("cores" . 8))))
        (publish x "4 cores/Linux" :properties '(:headers (("os" . "linux")
                                                         ("cores" . 4))))

        (log:info "Waiting...")
        (sleep 3)
        (log:info "Disconnecting")))))
