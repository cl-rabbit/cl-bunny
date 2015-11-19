(in-package :cl-bunny.test)

(plan 1)

(subtest "Connection errors"
  (is-error (with-connection ("amqp://localhost/ewgfrmiogtiogwr")) 'transport-error)

  (with-connection ("amqp://")
    (with-channel ()
      (iolib.syscalls:close (cl-rabbit::get-sockfd (slot-value bunny:*connection* 'bunny::cl-rabbit-connection)))
      (is-error (queue.declare) 'bunny:network-error "Network error raised when operation performed on closed socket"))))

(finalize)
