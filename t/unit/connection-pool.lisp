(in-package :cl-bunny.test)

(plan 1)

(subtest "EQ Connections pool"
  (subtest "Functional test"
    (let* ((spec "amqp://")
           (*connections-pool* (make-instance 'eq-connections-pool))
           (connection1 (connection.new spec)))
      (connections-pool.add connection1)
      (is (bunny::connection-pool connection1) *connections-pool* "Connection pool is set")
      (is (connections-pool.get spec) connection1 "Connection added to pool")
      (is (connections-pool.find-or-run spec spec) connection1 "Don't create new connection for known tag")
      (connections-pool.remove connection1)
      (is (connections-pool.get spec) nil "Connection removed from pool")
      (is (bunny::connection-pool connection1) nil "Connection pool is nil")))

  (subtest "Printer test"
    (let ((pool (make-instance 'eq-connections-pool)))
      (is (cl-bunny.test::print-amqp-object-to-string pool) "Connections: 0, Open: 0")
      (let ((connection1 (connection.new "amqp://localhost"))
            (connection2 (connection.new "amqp://h2"))
            (connection3 (connection.new "amqp://h3")))
        (is (connections-pool.add connection1 :pool pool) connection1)
        (connections-pool.add connection2 :pool pool)
        (connections-pool.add connection3 :pool pool)
        (is (cl-bunny.test::print-amqp-object-to-string pool) "Connections: 3, Open: 0")
        (unwind-protect
             (progn
               (connection.open connection1)
               (is (cl-bunny.test::print-amqp-object-to-string pool) "Connections: 3, Open: 1"))
          (connection.close :connection connection1))
        (is (cl-bunny.test::print-amqp-object-to-string pool) "Connections: 2, Open: 0")))

    (subtest "With-connection shared tag"
      (let ((connection))
        (unwind-protect
             (progn
               (with-connection ("amqp://" :shared :qwe)
                 (setf connection *connection*))
               (with-connection (:shared :qwe)
                 (is connection *connection*)))
          (when connection
            (connection.close :connection connection)))))))

(finalize)
