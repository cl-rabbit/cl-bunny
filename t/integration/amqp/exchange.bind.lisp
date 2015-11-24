(in-package :cl-bunny.test)

(plan 1)

(subtest "Exchange.bind tests"
  (with-connection ()
    (with-channel ()
      (let ((source) ;; TODO: maybe it's time for with-exchange?
            (destination))
        (unwind-protect
             (let ((queue (queue.declare-temp)))
               (setf source (exchange.fanout "cl-bunny.exchange.source" :auto-delete t)
                     destination (exchange.fanout "cl-bunny.exchange.destination" :auto-delete t))
               (queue.bind queue destination)
               (exchange.bind destination source)

               (publish source "")
               (sleep 1)

               (is (queue.message-count queue) 1))
          (when source
            (exchange.delete source))
          (when destination
            (exchange.delete destination)))))))

(finalize)
