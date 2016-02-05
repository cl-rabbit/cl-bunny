(in-package :cl-bunny.test)

(plan 1)

(subtest "Sync consumer test"
  (subtest "Consume on closed channel should signal channel-closed-error"
    (with-connection ()
      (with-channel ()
        (channel.close 200 0 0)
        (is-error (consume) 'channel-closed-error))))

  (subtest "Consume should signal amqp-error-not-found when subscribing to unknown queue"
    (with-connection ()
      (with-channel ()
        (is-error (subscribe "qweqweqwe" #'identity) 'amqp:amqp-error-not-found))))

  (subtest "Consume should signal channel-closed-error"
    (with-connection ()
      (with-channel ()
        (bunny:subscribe-sync (queue.declare-temp))
        (channel.close 200 0 0)
        (is-error (bunny:consume) 'channel-closed-error))))

  (subtest "Consume should signal channel-closed-error when connection closed"
    (with-connection ()
      (with-channel ()
        (bunny:subscribe-sync (queue.declare-temp))
        (connection.close)
        (is-error (bunny:consume) 'channel-closed-error))) "CHANNEL-CLOSED-ERROR  signaled"))

(finalize)
