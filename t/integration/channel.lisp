(in-package :cl-bunny.test)

(plan 5)

(subtest "Channel tests"
  (is-error (channel.new :connection (connection.new)) 'connection-closed-error "Can't create channel for closed connection")

  (subtest "New channel should be closed and registered"
    (with-connection ()
      (let ((channel (channel.new)))
        (is (gethash (channel-id channel) (bunny::connection-channels *connection*)) channel)
        (is (channel-open-p channel) nil "Newly created channel is closed"))))

  (with-connection ()
    (with-channel ()
      (is-error (channel.open) 'amqp:amqp-error-command-invalid "Can't open already opened channel")))

  (with-connection ()
    (with-channel ()
      (is-error (channel.flow nil) 'amqp:amqp-error-not-implemented "Inactive channels not supported")
      (is (connection-open-p *connection*) nil "Not supported error closes connection")))

  (progn
    (with-connection ()
      (with-channel ()
        (channel.close 200 0 0))
      (with-channel ()))
    (pass "Can quietly close closed channel")))

(finalize)
