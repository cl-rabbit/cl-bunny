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
                 (channel.close amqp:+amqp-reply-success+ 0 0)))))
      (when *connection*
        (connection.close)))))




;; (defun hello-world-async ()
;;   (with-conneciton ("amqp://" :async t)
;;     (with-channel ()
;;       (alet ((x (exchange.default)))
;;         (chain (queue.declare :name "cl-bunny.examples.hello-world" :auto-delete t)
;;           (:attach (q)
;;                    (subscribe q (lambda (message)
;;                                   (log:info "Received ~a"
;;                                             (message-body-string message)))))
;;           (:attach ()
;;                    (publish x "Hello world!" :routing-key "cl-bunny.examples.hello-world")))))))

;; (defun hello-world-async-raw ()
;;   (let ((bunny::*connection* (connection.new "amqp://"))
;;         (channel))
;;     (finally
;;         (catcher
;;          (chain (connection.start)
;;            (:attach (_)
;;              (channel.open))
;;            (:attach (c &out (x (exchange.default c)))
;;              (setf channel c))
;;            (:attach (_)
;;              (queue.declare :name "cl-bunny.examples.hello-world" :auto-delete t :channel channel))
;;            (:attach (q)
;;              (subscribe q (lambda (message)
;;                             (log:info "Received ~a"
;;                                       (message-body-string message))
;;                             (connection.close))))
;;            (:attach (_)
;;               (publish "" "Hello world!" :routing-key "cl-bunny.examples.hello-world" :channel channel)))
;;          (t (e)
;;             (log:error "~A" e)))
;;       (when bunny::*connection*
;;         (connection.close)))))

