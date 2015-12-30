(in-package :cl-bunny.test)

(plan 1)

(subtest "Connection errors"
  (subtest "General"
    (is-error (with-connection ("amqp://localost")) 'transport-error)
    ;; it theory it should throw amqp:amqp-error-not-allowed
    ;; since authentication_failure_close is set
    ;; however on my local pc it throws transport-error i.e. authentication_failure_close seems to be ignored
    ;; with rabbitmq built from sources it throws amqp:amqp-error-not-allowed as expected
    ;; on TravisCI it throws connnection-closed.
    (is-error (with-connection ("amqp://localhost/ewgfrmiogtiogwr")) 'error)

    (with-connection ("amqp://")
      (with-channel ()
        (iolib.syscalls:close (cl-rabbit::get-sockfd (slot-value bunny:*connection* 'bunny::cl-rabbit-connection)))
        (is-error (queue.declare) 'bunny:network-error "Network error raised when operation performed on closed socket"))))
  (subtest "Connection.consume and closed connection"
    (let ((text))
      (with-connection ("amqp://" :type 'bunny::librabbitmq-connection)
        (with-channel ()
          (let ((x (exchange.default))
                (q "cl-bunny.examples.hello-world"))

            (queue.declare :name q :auto-delete t)
            (with-consumers
                ((q
                  (lambda (message)
                     (setf text (message-body-string message))
                     (connection.close :connection (channel-connection (message-channel message))))
                  :type :sync))
              (publish x "Hello world!" :routing-key q)
              (is-error (message-body-string (connection.consume)) 'connection-closed-error)
              (is text "Hello world!"))))))))

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
          (event+ (channel-on-error)
                  (lambda (e)
                    (setf error e)))
          (message.ack 12312)
          (sleep 1)
          (is-type error 'amqp:amqp-error-precondition-failed))))))

(finalize)
