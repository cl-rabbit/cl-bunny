(in-package :cl-bunny)

(defclass message ()
  ((channel      :type channel
                 :initarg :channel
                 :reader message-channel)
   (consumer-tag :type string
                 :initarg :consumer-tag
                 :reader message-consumer-tag)
   (consumer     :type consumer
                 :initarg :consumer
                 :reader message-consumer)
   (delivery-tag :type integer
                 :initarg :delivery-tag
                 :reader message-delivery-tag)
   (redelivered  :type boolean
                 :initarg :redelivered
                 :reader message-redelivered-p)
   (exchange     :type string
                 :initarg :exchange
                 :reader message-exchange)
   (routing-key  :type string
                 :initarg :routing-key
                 :reader message-routing-key)
   (properties   :type list
                 :initarg :properties
                 :reader message-properties)
   (body         :type (simple-array (unsigned-byte 8) (*))
                 :initarg :body
                 :reader message-body)))

(defclass returned-message (message)
  ((reply-code :type fixnum
               :initarg :reply-code
               :reader returned-message-reply-code)
   (reply-text :type string
               :initarg :reply-text
               :reader returned-message-reply-text)))

(defun create-message (channel envelope)
  (make-instance 'message
                 :channel channel
                 :consumer-tag (cl-rabbit:envelope/consumer-tag envelope)
                 :delivery-tag (cl-rabbit:envelope/delivery-tag envelope)
                 :redelivered (cl-rabbit:envelope/redelivered envelope)
                 :exchange (cl-rabbit:envelope/exchange envelope)
                 :routing-key (cl-rabbit:envelope/routing-key envelope)
                 :properties (cl-rabbit:message/properties (cl-rabbit:envelope/message envelope))
                 :body (cl-rabbit:message/body (cl-rabbit:envelope/message envelope))))

(defun message-ack (message &key multiple (channel (message-channel message)))
  (let ((consumer (message-consumer message)))
    (if (eq :sync (consumer-type consumer))
        (amqp-basic-ack (message-delivery-tag message) :multiple multiple :channel channel)
        (amqp-basic-ack-async (message-delivery-tag message) :multiple multiple :channel channel))))

(defun message-nack (message &key multiple requeue (channel (message-channel message)))
  (let ((consumer (message-consumer message)))
    (if (eq :sync (consumer-type consumer))
        (amqp-basic-nack (message-delivery-tag message) :multiple multiple :requeue requeue :channel channel)
        (amqp-basic-nack-async (message-delivery-tag message) :multiple multiple :requeue requeue :channel channel))))

(defun message-header-value (message name)
  (header-value
   (properties-headers
    (message-properties message))
   name))

(defun message-property-value (message name)
  (assoc-value (print (message-properties message)) name))
