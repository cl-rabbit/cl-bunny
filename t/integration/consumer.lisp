(in-package :cl-bunny.test)

(plan 1)

(subtest "Sync consumer test"
  (subtest "Consume on closed channel should signal channel-closed-error"
    (with-connection ()
        (with-channel ()
          (channel.close 200 0 0)
          (is-error (consume) 'channel-closed-error))))

  (subtest "Consume should signal channel-closed-error"
    (with-connection ()
      (with-channel ()
        (bunny:subscribe-sync (queue.declare-temp))
        (sb-thread:make-thread (lambda (channel)
                                 (bunny::execute-in-connection-thread ((channel-connection channel))
                                   (sleep 5)
                                   (channel.close 200 0 0 :channel channel)))
                               :arguments (list *channel*))
        (sleep 1)
        (is-error (bunny:consume) 'channel-closed-error))))

  (subtest "Consume should signal connection-closed-error"
    (with-connection ()
      (with-channel ()
        (bunny:subscribe-sync (queue.declare-temp))
        (sb-thread:make-thread (lambda (connection)
                                 (bunny::execute-in-connection-thread (connection)
                                   (sleep 5)
                                   (connection.close :connection connection)))
                               :arguments (list *connection*))
        (sleep 1)
        (is-error (bunny:consume) 'connection-closed-error)))))

(finalize)
