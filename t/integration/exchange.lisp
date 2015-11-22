(in-package :cl-bunny.test)

(plan 1)

(subtest "Exchange class/interface tests"
  (subtest "Test exchange.exists-p")
  (with-connection ()
    (is (exchange.exists-p "qweewrweqerggrewgre") nil "Exchange `qweewrweqerggrewgre` doesn't exist")
    (with-channel ()
      (unwind-protect
           (progn
             (ok (exchange.declare "qweewrweqerggrewgre") "Exchange `qweewrweqerggrewgre` successfully declared")
             (ok (exchange.exists-p "qweewrweqerggrewgre") "Exchange `qweewrweqerggrewgre` exists"))
        (ignore-errors
         (exchange.delete "qweewrweqerggrewgre"))))))

(finalize)
