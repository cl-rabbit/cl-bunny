(in-package :cl-bunny.test)

(plan 3)

(subtest "Connection parameters"
  (with-connection "amqp://localhost?frame-max=131070&heartbeat-interval=30&channel-max=256"
    (is (connection-frame-max) 131070)
    (is (connection-heartbeat) 30)
    (is (connection-channel-max) 256)))

(subtest "Threaded Connection interaction corner cases"
  (progn (with-connection ()
           (with-channel ()
             (let ((bunny::*force-timeout* 1))
               (sb-thread:make-thread (lambda (connection)
                                        (bunny::execute-in-connection-thread (connection)
                                          (sleep 5)))
                                      :arguments (list *connection*))
               (sleep 2)
               (is-error (queue.declare) 'bunny:sync-promise-timeout
                         "If connection thread is unresponsive sync operation errors with timeout")))))

  (progn (with-connection ()
           (with-channel ()
             (sb-thread:make-thread (lambda (connection)
                                      (sleep 2)
                                      (connection.close :connection connection))
                                    :arguments (list *connection*))
             (sleep 6)
             (is-error (queue.declare) 'bunny:channel-closed-error
                       "Closing connection also closes channel"))))

  (with-connection ()
    (with-channel ()
      (let ((bunny::*force-timeout* 5))
        (sb-thread:make-thread (lambda (connection)
                                 (bunny::execute-in-connection-thread (connection)
                                   (sleep 2)
                                   (throw 'bunny::stop-connection nil)))
                               :arguments (list *connection*))
        (sleep 1)
        (is-error (queue.declare) 'connection-closed-error
                  "If connection closed before receiving sync reply connection-closed-error signaled"))))

  (progn (loop for i from 1 to 1000 do ;; <- actually there should be 10000 but Travis CI can't handle that
                  (with-connection ()
                    (with-channel ())))
         (pass "Resources are properly deallocated, no races")))

(subtest "Heartbeat tests"
  (subtest "When client skips more than two heartbeats server should  close connection"
    (is-error
     (with-connection ("amqp://?heartbeat=2")
       (bunny::execute-in-connection-thread ()
         (sleep 10))
       (sleep 15)
       (with-channel ()))
     'connection-closed-error))

  (subtest "We actually send heartbeats to the server"
    (ok
     (with-connection ("amqp://?heartbeat=6")
       (sleep 20)
       (with-channel ())
       t)))

  (subtest "Heartbeats help detect closed/aborted connections"
    (let ((closed))
      (with-connection ("amqp://?heartbeat=2")
        (event+ (connection-on-close)
                (lambda (connection)
                  (setf closed connection)))
        (with-channel ()
          (close (bunny::connection-socket *connection*))
          (sleep 7)
          (unless closed
            (with-channel ()))
          (is closed bunny:*connection*)))))

  (subtest "Aborted connection without hearbeat"
    (is-error (let ((closed))
                (with-connection ("amqp://")
                  (event+ (connection-on-close)
                          (lambda (connection)
                            (setf closed connection)))
                  (with-channel ()
                    (close (bunny::connection-socket *connection*))
                    (sleep 7)
                    (unless closed
                      (with-channel ()))
                    (is closed bunny:*connection*))))
              connection-closed-error)))

(finalize)
