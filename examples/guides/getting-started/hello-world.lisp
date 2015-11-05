(in-package :cl-bunny.examples)

(defun test-send (message)
  (with-connection "amqp://"
    (with-channel ()
      (queue.declare :name "test-queue")
      (publish (exchange.default) message :routing-key "test-queue"))))

(defun test-recv-sync ()
  (with-connection "amqp://"
    (with-channel ()
      (queue.declare :name "test-queue")
      (with-consumers
          (("test-queue"
            (lambda (message)
              (format t "Got message ~a" (message-body-string message)))
            :type :sync))
        (consume :one-shot t)))))

(defun hello-world ()
  (with-connection ("amqp://")
    (with-channel ()
      (let ((x (exchange.default)))
        (->
          (queue.declare :name "cl-bunny.examples.hello-world" :auto-delete t)
          (subscribe (lambda (message)
                       (log:info "Received ~a"
                                 (message-body-string message)))))
        (publish x "Hello world!" :routing-key "cl-bunny.examples.hello-world"))
      (sleep 1))))

(defun hello-world-sync ()
  (with-connection ("amqp://")
    (with-channel ()
      (let ((x (exchange.default))
            (q "cl-bunny.examples.hello-world"))

        (queue.declare :name q :auto-delete t)
        (with-consumers
            ((q
              (lambda (message)
                (log:info "Received ~a"
                          (message-body-string message)))
              :type :sync))
          (publish x "Hello world!" :routing-key q)
          (consume :one-shot t))))))

(defun hello-world-raw ()
  (let ((*connection* (connection.new)))
    (unwind-protect
         (progn
           (connection.open)
           (let* ((*channel* (channel.open (channel.new))) ;; channel.new.open also works
                  (x (exchange.default))
                  (queue-name "cl-bunny.examples.hello-world"))
             (unwind-protect
                  (progn
                    (->
                      (queue.declare :name queue-name :auto-delete t)
                      (subscribe (lambda (message)
                                   (log:info "Received ~a"
                                             (message-body-string message)))
                                 :type :sync))
                    (publish x "Hello world!" :routing-key queue-name)
                    (consume :one-shot t))
               (when *channel* ;; btw, channel will be closed with connection anyway
                 (channel.close)))))
      (when *connection*
        (connection.close)))))
