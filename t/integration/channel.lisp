(in-package :cl-bunny.test)

(plan 5)

(subtest "Channel tests"
  (subtest "New channel should be closed and registered"
    (let* ((*connection* (connection.new))
           (*channel* (channel.new)))
      (is (gethash (channel-id *channel*) (bunny::connection-channels *connection*)) *channel*)
      (is (channel-open-p) nil "Newly created channel is closed")))

  (with-connection ()
    (with-channel ()
      (is-error (channel.open) 'amqp:amqp-error-command-invalid "Can't open already opened channel")))

  (with-connection ()
    (with-channel ()
      (is-error (channel.flow nil) 'amqp:amqp-error-not-implemented "Inactive channels not supported")
      (is (connection-open-p *connection*) nil "Not supported error closes connection"))))

(finalize)
