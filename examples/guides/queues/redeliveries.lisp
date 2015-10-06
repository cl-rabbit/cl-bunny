(in-package :cl-bunny.examples)

(defun redeliveries ()
  (log:info "=> Subscribing for messages using explicit acknowledgements model")

  (let ((connection1 (new-connection))
        (connection2 (new-connection))
        (connection3 (new-connection)))

    (log:info "Starting connections")
    (connection-start connection1)
    (connection-start connection2)
    (connection-start connection3)

    (log:info "Setting-up channels")
    (let ((ch1 (new-channel connection1))
          (ch2 (new-channel connection1))
          (ch3 (new-channel connection1)))
      
      (log:info "Opening channels")
      (channel-open ch1)
      (channel-open ch2)
      (channel-open ch3)

      (log:info "Setting prefetch size")
      (setf (channel-prefetch ch1) 1
            (channel-prefetch ch2) 1
            (channel-prefetch ch3) 1)

      (log:info "Declaring direct exchange")
      (let ((x (direct-exchange "amq.direct" :Channel ch3)))

        (log:info "Declaring and subscribing to queue 1")
        (->
          (queue.declare "cl-bunny.examples.acknowledgements.explicit" :auto-delete nil
                                                                       :channel ch1)
          (queue.purge)
          (queue.bind x)
          (subscribe (lambda (message)
                       ;; do some work
                       (sleep 0.2)
                       ;; acknowledge some messages, they will be removed from the queue
                       (cond
                         ((> (random 10) 5)
                          (message-ack message) ;; (message-ack message :multiple nil)
                          (log:info "[consumer1] Got message #~a, redelivered?: ~a, ack-ed"
                                    (header-value (properties-headers (message-properties message)) "i")
                                    (message-redelivered-p message)))
                         (t                          
                          ;; some messages are not ack-ed and will remain in the queue for redelivery
                          ;; when app #1 connection is closed (either properly or due to a crash)
                          (log:info "[consumer1] Got message #~a, SKIPPED"                                    
                                    (message-header-value message "i"))))))) ;; <- short form

        (log:info "Declaring and subscribing to queue 2")
        (->
          (queue.declare "cl-bunny.examples.acknowledgements.explicit" :auto-delete nil
                                                                       :channel ch2)
          (queue.bind x)
          (subscribe (lambda (message)
                       ;; do some work
                       (sleep 0.2)

                       (message-ack message)
                       (log:info "[consumer1] Got message #~a, redelivered?: ~a, ack-ed"
                                    (message-header-value message "i")
                                    (message-redelivered-p message)))))
        
        (bt:make-thread (lambda ()
                          (loop
                            for i from 0 do
                               (sleep 0)
                               (publish x (format nil "Message #~a" i)
                                        :properties `((:headers . (("i" . ,i))))))))

        (bt:make-thread (lambda ()
                          (sleep 4)

                          (connection-close connection1)
                          (log:info "----- Connection 1 is now closed (we pretend that it has crashed) -----")))

        (sleep 7)

        (log:info "Closing connections 2 & 3")
        (connection-close connection2)
        (connection-close connection3)))))
