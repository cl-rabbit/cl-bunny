(in-package :cl-bunny)

(defun make-connection (amqp-connection)
  (make-instance 'connection
                 :amqp-connection-state amqp-connection
                 :channel-id-allocator (new-channel-id-allocator (amqp-get-channel-max (cl-rabbit::connection/native-connection amqp-connection)))))

(defun allocate-and-open-new-channel ()
  (let ((channel-id (next-channel-id (slot-value *connection* 'channel-id-allocator))))
    (amqp-channel-open channel-id)
    channel-id))

(defmacro with-channel (channel-id &body body) ;;TODO: add id auto-generation
  (with-gensyms (allocated-p)
    `(multiple-value-bind (*channel* ,allocated-p) (if ,channel-id
                                                       ,channel-id
                                                       (values
                                                        (allocate-and-open-new-channel)
                                                        t))
       (unwind-protect
            (progn
              ,@body)
         (when ,allocated-p
           (amqp-channel-close *channel*))))))

(defmacro with-connection (spec &body body)
  (with-gensyms (amqp-connection socket)
    `(cl-rabbit:with-connection (,amqp-connection)
       (let ((,socket (cl-rabbit:tcp-socket-new ,amqp-connection)))
         (cl-rabbit:socket-open ,socket
                                (or (getf ',spec :host)
                                    "localhost")
                                (or (getf ',spec :port)
                                    5672))
         (cl-rabbit:login-sasl-plain ,amqp-connection
                                     (or (getf ',spec :vhost)
                                         "/")
                                     (or (getf ',spec :login)
                                         "guest")
                                     (or (getf ',spec :password)
                                         "guest"))
         (let ((*connection* (make-connection ,amqp-connection)))
           (with-channel ,(getf spec :channel-id)
             ,@body))))))

(defun test-send1 (message)
  "Default Exchange, No need to bind queue"
  (with-connection ()
    (amqp-queue-declare "xx")
    (amqp-publish message
                  :routing-key "xx")))

(defun subscribe (queue callback)
  (cl-rabbit:basic-consume *connection* *channel* queue)
  (loop
    as )
  )

(defun test-recv1 ()
  (with-connection ()
    (amqp-queue-declare "xx")
    ;; (queue-subscribe "xx" (lambda (envelope)
    ;;                          (format t "Got message: ~s~%content: ~s~%props: ~s"
    ;;                                  result (babel:octets-to-string (message/body message) :encoding :utf-8)
    ;;                                  (message/properties message))
    ;;                          nil))
    ;;(cl-rabbit:queue-bind conn 1 :queue queue-name :exchange "test-ex" :routing-key "xx")
    (with-amqp-connection (amqp-connection *connection*)
      (cl-rabbit:basic-consume amqp-connection *channel* "xx")
      (loop
        (let* ((result (cl-rabbit:consume-message amqp-connection))
               (message (cl-rabbit:envelope/message result))
               (message-string (babel:octets-to-string (cl-rabbit:message/body message) :encoding :utf-8)))
          (format t "Got message: ~s~%content: ~s~%props: ~s"
                  result message-string
                  (cl-rabbit:message/properties message))
          (cl-rabbit:basic-ack amqp-connection *channel* (cl-rabbit:envelope/delivery-tag result))
          (when (equal message-string "exit")
            (return)))))))


(defun test-recv ()
  (with-connection (conn)
    (let ((socket (tcp-socket-new conn)))
      (socket-open socket "localhost" 5672)
      (login-sasl-plain conn "/" "guest" "guest")
      (channel-open conn 1)
      (exchange-declare conn 1 "test-ex" "topic")
      (let ((queue-name "foo3"))
        (queue-declare conn 1 :queue queue-name)
        (queue-bind conn 1 :queue queue-name :exchange "test-ex" :routing-key "xx")
        (basic-consume conn 1 "xx")
        (let* ((result (consume-message conn))
               (message (envelope/message result)))
          (format t "Got message: ~s~%content: ~s~%props: ~s"
                  result (babel:octets-to-string (message/body message) :encoding :utf-8)
                  (message/properties message))
          (cl-rabbit:basic-ack conn 1 (envelope/delivery-tag result)))))))

(test-send1)

(test-recv1)
