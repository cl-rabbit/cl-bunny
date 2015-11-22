(in-package :cl-bunny.test)

(plan 1)

(subtest "Queue class/interface tests"
  (subtest "Test queue-server-named-p"
    (with-connection ()
      (with-channel ()
        (let ((server-named-queue (queue.declare :exclusive t))
              (client-named-queue (queue.declare :name "qwe" :exclusive t)))
          (is (queue-server-named-p server-named-queue) t "Server-named queue is server-named")
          (is (queue-server-named-p client-named-queue) nil "Client-named queue isn't server-named")))))

  (subtest "Temp queue shortcut"
    (with-connection ()
      (with-channel ()
        (let ((q (queue.declare-temp)))
          (is (queue-server-named-p q) t "Temp queue is server-named")
          (is (queue-exclusive-p q) t "Temp queue is exclusive")
          (isnt (queue-durable-p q) t "Temp queue isn't durable")))))

  (subtest "Test queue.exists-p"
    (with-connection ()
      (is (queue.exists-p "qweewrweqerggrewgre") nil "Queue `qweewrweqerggrewgre` doesn't exist")
      (with-channel ()
        (ok (queue.declare :name "qweewrweqerggrewgre" :exclusive t) "Queue `qweewrweqerggrewgre` successfully declared"))
      (ok (queue.exists-p "qweewrweqerggrewgre") "Queue `qweewrweqerggrewgre` exists")))

  (subtest "Queue put/peek/get operations"
    (with-connection ()
      (with-channel ()
        (let ((q (queue.declare-temp)))
          (queue.put q "qwe")
          (ok (queue.peek q))
          (ok (queue.get :no-ack t)))))))

(finalize)
