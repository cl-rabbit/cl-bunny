(in-package :cl-bunny.test)

(plan nil)

(subtest "Channel tests"
  (subtest "New channel should be closed and registered"
    (let* ((*connection* (connection.new))
           (*channel* (channel.new)))
      (is (gethash (channel-id *channel*) (bunny::connection-channels *connection*)) *channel*)
      (is (channel-open-p) nil "Newly created channel is closed")))

  (with-connection ()
    (with-channel ()
      (is-error (channel.open) 'error "Can't open already opened channel"))) ;; TODO: specialize error

  (with-connection ()
    (with-channel ()
      (is-error (channel.flow nil) 'error "Inactive channels not supported")
      (is (connection-open-p *connection*) nil "Not supported error closes connection"))))

(finalize)
