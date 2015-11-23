(in-package :cl-bunny.test)

(plan 1)

(subtest "Exchange class/interface tests"
  (subtest "Test exchange.exists-p"
    (with-connection ()
      (is (exchange.exists-p "qweewrweqerggrewgre") nil "Exchange `qweewrweqerggrewgre` doesn't exist")
      (with-channel ()
        (unwind-protect
             (progn
               (ok (exchange.declare "qweewrweqerggrewgre") "Exchange `qweewrweqerggrewgre` successfully declared")
               (ok (exchange.exists-p "qweewrweqerggrewgre") "Exchange `qweewrweqerggrewgre` exists"))
          (ignore-errors
           (exchange.delete "qweewrweqerggrewgre"))))))

  (subtest "Predefined Exchanges"
    (with-connection ()
      (with-channel ()
        (is (exchange.default) (exchange.default))
        (is (exchange.direct) (exchange.direct))
        (is (exchange.fanout) (exchange.fanout))
        (is (exchange.headers) (exchange.headers))
        (is (exchange.topic) (exchange.topic))
        (is (exchange.match) (exchange.match))))))

(finalize)
