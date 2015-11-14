(in-package :cl-bunny.test)

(plan 1)

(subtest "EQ Connections pool"
  (let* ((spec "amqp://")
         (*connections-pool* (make-instance 'eq-connections-pool))
         (connection1 (connection.new spec)))
    (connections-pool.add connection1)
    (is (bunny::connection-pool connection1) *connections-pool* "Connection pool is set")
    (is (connections-pool.get spec) connection1 "Connection added to pool")
    (is (connections-pool.find-or-run spec) connection1 "Don't create new connection for known tag")
    (connections-pool.remove connection1)
    (is (connections-pool.get spec) nil "Connection removed from pool")
    (is (bunny::connection-pool connection1) nil "Connection pool is nil")))

(finalize)
