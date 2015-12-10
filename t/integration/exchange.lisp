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
        (is (exchange.match) (exchange.match)))))

  (subtest "Exchange on return event"
    (with-connection ()
    (with-channel ()
      (let* ((x (exchange.default))
             (q (queue.declare :exclusive t))
             (consumed)
             (returned))

        (event+ (exchange-on-return x)
                (lambda (returned-message)
                  (setf returned (message-body-string returned-message))))

        (subscribe q (lambda (message)
                       (setf consumed (message-body-string message))))

        (publish x "This will NOT be returned" :mandatory t :routing-key q)
        (publish x "This will be returned" :mandatory t :routing-key (format nil "wefvvtrw~a" (random 10)))

        (sleep 1)
        (is consumed "This will NOT be returned")
        (is returned "This will be returned"))))))

(finalize)
