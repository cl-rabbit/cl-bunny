(in-package :cl-bunny.test)

(plan 3)

(subtest "Connection parameters"
  (with-connection "amqp://localhost?frame-max=131070&heartbeat-interval=60&channel-max=256"
    (is (connection-frame-max) 131070)
    (is (connection-heartbeat) 60)
    (is (connection-channel-max) 256)))

(subtest "Connection termination corner cases"
  (progn (with-connection ()
           (with-channel ()
             (let ((bunny::*force-timeout* 1))
               (sb-thread:make-thread (lambda (connection)
                                        (bunny::execute-in-connection-thread (connection)
                                          (sleep 5)
                                          (connection.close :connection connection)))
                                      :arguments (list *connection*))
               (sleep 2)
               (is-error (queue.declare) 'bunny:threaded-promise-timeout
                         "if connection unexpectedly closed or threaded call made after queue draining call will block until *force-timout* expired")))))

  (progn (with-connection ()
           (with-channel ()
             (sb-thread:make-thread (lambda (connection)
                                      (bunny::execute-in-connection-thread (connection)
                                        (sleep 5)
                                        (connection.close :connection connection)))
                                    :arguments (list *connection*))
             (sleep 1)
             (is-error (queue.declare) 'bunny:connection-closed-error
                       "while closing connection control mailbox drained and all functions called. connection is closed at this time")
             ))
         (pass "passed"))

  (progn (with-connection ()
           (with-channel ()
             (queue.declare :name "qwe" :auto-delete t)
             (with-consumers
                 (("qwe"
                   (lambda (m) (declare (ignore m)) (print "never called"))))
               (let ((bunny::*force-timeout* 1))
                 (sb-thread:make-thread (lambda (connection)
                                          (bunny::execute-in-connection-thread ((channel-connection connection))
                                            (sleep 2)
                                            (channel.close 201 0 0 :channel  connection)))
                                        :arguments (list *channel*))
                 (sleep 5)
                 (is-error (queue.declare) 'bunny:channel-closed-error "Can't use closed channel")
                 (connection.close)))))
         (pass "Can safely ignore closed channel and/or connection in with-channel/consumers cleanup"))


  (progn (with-connection ()
           (with-channel ()
             (queue.declare :name "qwe" :auto-delete t)
             (with-consumers
                 (("qwe"
                   (lambda (m) (declare (ignore m)) (print "never called"))))
               (let ((bunny::*force-timeout* 1))
                 (sb-thread:make-thread (lambda (channel)
                                          (bunny::execute-in-connection-thread ((channel-connection channel))
                                            (with-channel channel
                                              (sleep 10)
                                              (connection.close :connection (channel-connection channel)))))
                                        :arguments (list *channel*))
                 (sleep 5)
                 (connection.close)))))
         (pass "Can safely abort stalled connection"))

  (progn (with-connection ()
           (with-channel ()
             (queue.declare :name "qwe" :auto-delete t)
             (with-consumers
                 (("qwe"
                   (lambda (m) (declare (ignore m)) (print "never called"))))
               (let ((bunny::*force-timeout* 1))
                 (sb-thread:make-thread (lambda (channel)
                                          (bunny::execute-in-connection-thread ((channel-connection channel))
                                            (with-channel channel
                                              (sleep 2)
                                              (connection.close :connection (channel-connection channel)))))
                                        :arguments (list *channel*))
                 (sleep 5)
                 (connection.close)))))
         (pass "Do not block when closing closed connection"))

  (progn (loop for i from 1 to 1000 do ;; <- actually there should be 10000 but Travis CI can't handle that
                  (with-connection ()
                    (with-channel ())))
         (pass "Resources are properly deallocated, no races")))

(subtest "Heartbeat tests"
  (subtest "When client skips more than two heartbeats server should  close connection"
    (is-error
     (with-connection ("amqp://" :heartbeat 6)
       (bunny::execute-in-connection-thread-sync ()
         (sleep 20))
       (with-channel ()))
     'connection-closed-error))

  (subtest "We actually send heartbeats to the server"
    (ok
     (with-connection ("amqp://" :heartbeat 6)
       (sleep 20)
       (with-channel ())
       t)))

  (subtest "Heartbeats help detect closed/aborted connections"
    (let ((closed))
        (with-connection ("amqp://" :heartbeat 6)
          (setf (bunny:connection-on-close-callback) (lambda (connection)
                                                       (setf closed connection)))
          (with-channel ()
            (sleep 3)
            (iolib.syscalls:close (cl-rabbit::get-sockfd (slot-value bunny:*connection* 'bunny::cl-rabbit-connection)))
            (sleep 7)
            (unless closed
              (with-channel ()))
            (is closed bunny:*connection*)))))

  (subtest "Aborted connection without hearbeat"
    (is-error (with-connection ("amqp://")
                (sleep 3)
                (iolib.syscalls:close (cl-rabbit::get-sockfd (slot-value bunny:*connection* 'bunny::cl-rabbit-connection)))
                (sleep 7)
                (with-channel ()))
              network-error)))

(finalize)
