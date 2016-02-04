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
        (sb-thread:make-thread (lambda (channel)
                                 (bunny::execute-in-connection-thread ((channel-connection channel))
                                   (sleep 5)
                                   (channel.close 200 0 0 :channel channel)))
                               :arguments (list *channel*))
        (sleep 1)
        (is-error (bunny:consume) 'channel-closed-error))))

  (subtest "Consume should signal channel-closed-error when connection closed"
    (with-connection ()
      (with-channel ()
        (bunny:subscribe-sync (queue.declare-temp))
        (sb-thread:make-thread (lambda (connection)
                                 (bunny::execute-in-connection-thread (connection)
                                   (sleep 5)
                                   (connection.close :connection connection)))
                               :arguments (list *connection*))
        (sleep 2)
        (let ((error))
          (handler-case
              (bunny:consume)
            (error (e)
              (setf error e)))
          (ok (or (typep error 'channel-closed-error)
                  (typep error 'connection-closed-error)))))) "CHANNEL-CLOSED-ERROR or CONNECTION-CLOSED-ERROR signaled"))

(finalize)
