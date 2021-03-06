(in-package :cl-bunny.examples)


(defun direct-exchange-routing ()
  (log:info "=> Direct exchange routing")

  (with-connection ("amqp://")
    (with-channel ()
      (let ((x (exchange.declare "examples.imaging" :auto-delete t))
            (q1 (queue.declare :auto-delete t))
            (q2 (queue.declare :auto-delete t)))
        (queue.bind q1 x :routing-key "resize")

        (subscribe q1 (lambda (message)
                        (log:info "[~a][consumer] ~a received a 'resize' message: ~a"
                                  (bt:current-thread) q1 (message-body-string message))
                        (queue.bind q2 x :routing-key "watermark")
                        (publish x (format nil "~a" (random 15)) :routing-key "watermark")))
        (subscribe q2 (lambda (message)
                        (log:info "[~a][consumer] ~a received a 'watermark' message: ~a"
                                  (bt:current-thread) q2 (message-body-string message))))

        (log:info "[~a] Publishing resize message" (bt:current-thread))
        (publish x (format nil "~a" (random 10)) :routing-key "resize")

        (log:info "Waiting...")
        (sleep 5)
        (log:info "Disconnecting")))))

(defun direct-exchange-routing-sync ()
  (log:info "=> Direct exchange routing")

  (with-connection ("amqp://")
    (with-channel ()
      (let ((x (exchange.declare "examples.imaging" :auto-delete t))
            (q1 (queue.declare :auto-delete t))
            (q2 (queue.declare :auto-delete t)))
        (queue.bind q1 x :routing-key "resize")
        (queue.bind q2 x :routing-key "watermark")

        (with-consumers
            ((q1
              (lambda (message)
                (log:info "[consumer] ~a received a 'resize' message: ~a" q1 (message-body-string message)))
              :type :sync)
             (q2
              (lambda (message)
                (log:info "[consumer] ~a received a 'watermark' message: ~a" q2 (message-body-string message)))
              :type :sync))


          (log:info "Publishing resize message")
          (publish x (format nil "~a" (random 10)) :routing-key "resize")
          (consume :one-shot t)

          (log:info "Publishing watermark message")
          (publish x (format nil "~a" (random 15)) :routing-key "watermark")
          (consume :one-shot t)

          (log:info "Unsubscribing consumers"))

        (log:info "Disconnecting")))))
