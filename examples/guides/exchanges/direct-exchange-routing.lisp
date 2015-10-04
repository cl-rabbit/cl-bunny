(in-package :cl-bunny.examples)


(defun direct-exchange-routing ()
  (log:info "=> Direct exchange routing")

  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let ((x (amqp-exchange-declare "examples.imaging" :type "direct"))
            (q1 (amqp-queue-declare "" :auto-delete t))
            (q2 (amqp-queue-declare "" :auto-delete t)))
        (amqp-queue-bind q1 :exchange x :routing-key "resize")
        (amqp-queue-bind q2 :exchange x :routing-key "watermark")

        (subscribe q1 (lambda (message)
                        (log:info "[consumer] ~a received a 'resize' message: ~a"
                                  q1 (babel:octets-to-string (message-body message)))))
        (subscribe q2 (lambda (message)
                        (log:info "[consumer] ~a received a 'watermark' message: ~a"
                                  q2 (babel:octets-to-string (message-body message)))))

        (amqp-basic-publish (format nil "~a" (random 10)) :exchange x
                                                          :routing-key "resize")
        (amqp-basic-publish (format nil "~a" (random 15)) :exchange x
                                                          :routing-key "watermark")
        
        (log:info "Waiting...")
        (sleep 5)
        
        (amqp-exchange-delete "examples.imaging")
        (amqp-queue-delete q1)
        (amqp-queue-delete q2)
        (log:info "Disconnecting")))))

(defun direct-exchange-routing-sync ()
  (log:info "=> Direct exchange routing")

  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let ((x (amqp-exchange-declare "examples.imaging" :type "direct"))
            (q1 (amqp-queue-declare "" :auto-delete t))
            (q2 (amqp-queue-declare "" :auto-delete t)))
        (amqp-queue-bind q1 :exchange x :routing-key "resize")
        (amqp-queue-bind q2 :exchange x :routing-key "watermark")

        (with-consumers
            ((q1
              (lambda (message)
                (log:info "[consumer] ~a received a 'resize' message: ~a" q1 (babel:octets-to-string (message-body message))))
              :type :sync)
             (q2
              (lambda (message)
                (log:info "[consumer] ~a received a 'watermark' message: ~a" q2 (babel:octets-to-string (message-body message))))
              :type :sync))
          

          (log:info "Publishing resize message")
          (amqp-basic-publish (format nil "~a" (random 10)) :exchange x
                                                            :routing-key "resize")
          (consume :one-shot t)
          (log:info "Publishing watermark message")
          (amqp-basic-publish (format nil "~a" (random 15)) :exchange x
                                                            :routing-key "watermark")
          (consume :one-shot t)

          (log:info "Unsubscribing consumers"))
        
        (amqp-exchange-delete "examples.imaging")
        (amqp-queue-delete q1)
        (amqp-queue-delete q2)
        
        (log:info "Disconnecting")))))
