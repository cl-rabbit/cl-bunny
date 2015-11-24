(in-package :cl-bunny.test)

(plan 1)

(subtest "Exchange.unbind tests"
  (with-connection ()
    (with-channel ()
      (let ((source) ;; TODO: maybe it's time for with-exchange?
            (destination))
        (unwind-protect
             (let ((queue (queue.declare-temp)))
               (setf source (exchange.fanout "cl-bunny.exchange.source")
                     destination (exchange.fanout "cl-bunny.exchange.destination"))
               (queue.bind queue destination)
               (exchange.bind destination source)

               (publish source "")
               (sleep 1)

               (is (queue.message-count queue) 1)

               (queue.get queue :no-ack t)

               (exchange.unbind destination source)

               (publish source "")
               (sleep 1)

               (is (queue.message-count queue) 0))
          (when source
            (exchange.delete source))
          (when destination
            (exchange.delete destination)))))))

(finalize)
