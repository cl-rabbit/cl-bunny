(in-package :cl-bunny.test)

(plan 1)

(subtest "Connection errors"
  (is-error (with-connection ("amqp://localost")) 'transport-error)
  (is-error (with-connection ("amqp://localhost/ewgfrmiogtiogwr")) 'amqp:amqp-error-not-allowed)

  (with-connection ("amqp://")
    (with-channel ()
      (iolib.syscalls:close (cl-rabbit::get-sockfd (slot-value bunny:*connection* 'bunny::cl-rabbit-connection)))
      (is-error (queue.declare) 'bunny:network-error "Network error raised when operation performed on closed socket"))))

(subtest "Channel errors"
  (subtest "Sync channel error"
    (with-connection ()
      (let ((error))
        (with-channel (:on-error (lambda (e)
                                   (setf error e)))
          (ignore-errors (queue.bind "xwewf" "x")))
        (sleep 1)
        (is-type error 'amqp:amqp-error-not-found))))

  (subtest "Async channel error"
    (with-connection ()
      (with-channel ()
        (let ((error))
          (setf (channel-on-error-callback)
                (lambda (e)
                  (setf error e)))
          (message.ack 12312)
          (sleep 1)
          (is-type error 'amqp:amqp-error-precondition-failed))))))

(finalize)
