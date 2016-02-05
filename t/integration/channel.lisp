(in-package :cl-bunny.test)

(plan 1)

(subtest "Channel tests"
  (is-error (channel.new :connection (connection.new)) 'connection-closed-error "Can't create channel for closed connection")

  (subtest "New channel should be closed and registered"
    (with-connection ()
      (let ((channel (channel.new)))
        (is (gethash (channel-id channel) (bunny::connection-channels *connection*)) channel)
        (is (channel-open-p channel) nil "Newly created channel is closed"))))

  (with-connection ()
    (with-channel ()
      ;; note on error type: it should be channel-error but old rabbitmq versios return command-invalid
      (is-error (channel.open) 'connection-closed-error "Can't open already opened channel")))

  (with-connection ()
    (with-channel ()
      (is-error (channel.flow nil) 'connection-closed-error "Inactive channels not supported")
      (is (connection-open-p *connection*) nil "Not supported error closes connection")))

  (progn
    (with-connection ()
      (with-channel ()
        (channel.close 200 0 0))
      (with-channel ()))
    (pass "Can quietly close closed channel"))

  (subtest "Channel on-return event"
    (with-connection ()
      (with-channel ()
        (let ((x (exchange.default))
              (returned))

          (queue.declare-temp)

          (event+ (channel-on-return)
                  (lambda (returned-message)
                    (setf returned returned-message)))

          (publish x "This will be returned" :mandatory t :routing-key (format nil "wefvvtrw~a" (random 10)))

          (sleep 1)
          (isnt returned nil "Message returned")
          (is (message-body-string returned) "This will be returned"))))))

(finalize)
