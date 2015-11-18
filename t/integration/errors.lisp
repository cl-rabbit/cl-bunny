(in-package :cl-bunny.test)

(plan 1)

(subtest "Connection errors"
  (is-error (with-connection ("amqp://localhost/ewgfrmiogtiogwr")) 'transport-error))

(finalize)
