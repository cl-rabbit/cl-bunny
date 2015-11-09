(in-package :cl-bunny.test)

(plan 1)

(subtest "Connection termination corner cases"
  (with-connection ()
    (with-channel ()
      (let ((bunny::*force-timeout* 1))
        (sb-thread:make-thread (lambda (connection)
                                 (bunny::execute-in-connection-thread (connection)
                                   (sleep 5)
                                   (connection.close connection)))
                               :arguments (list *connection*))
        (sleep 2)
        (is-error (queue.declare) 'bunny:threaded-promise-timeout
                  "if connection unexpectedly closed or threaded call made after queue draining call will block until *force-timout* expired"))))

  (with-connection ()
    (with-channel ()
      (sb-thread:make-thread (lambda (connection)
                               (bunny::execute-in-connection-thread (connection)
                                 (sleep 5)
                                 (connection.close connection)))
                             :arguments (list *connection*))
      (sleep 1)
      (is-error (queue.declare) 'bunny:connection-closed-error
                "while closing connection control mailbox drained and all functions called. connection is closed at this time")))

  (with-connection ()
    (with-channel ()
      (let ((bunny::*force-timeout* 1))
        (sb-thread:make-thread (lambda (connection)
                                 (bunny::execute-in-connection-thread ((channel-connection connection))
                                   (sleep 2)
                                   (channel.close 201 0 0 :channel  connection)))
                               :arguments (list *channel*))
        (sleep 5)
        (is-error (queue.declare) 'bunny:channel-closed-error "Can't use closed channel")
        (connection.close *connection*))))
  (ok "Can safely ignore closed channel and/or connection in with-channel cleanup"))

(finalize)
