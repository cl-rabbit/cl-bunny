(in-package :cl-bunny)

(defclass message ()
  ((channel      :type channel
                 :initarg :channel
                 :reader message-channel)
   (consumer-tag :type string
                 :initform nil
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

(defmethod message-delivery-tag ((tag integer))
  tag)

(defmethod message-channel ((tag integer))
  nil)

(defun message.ack (message &key multiple (channel (or (message-channel message) *channel*)))
  (channel.send% channel
      (make-instance 'amqp-method-basic-ack
                     :delivery-tag (message-delivery-tag message)
                     :multiple multiple)))

(defun message.nack (message &key multiple requeue (channel (or (message-channel message) *channel*)))
  (channel.send% channel
                 (make-instance 'amqp-method-basic-nack
                   :delivery-tag (message-delivery-tag message)
                   :multiple multiple
                   :requeue requeue)))

(defun message-header-value (message name)
  (header-value
   (properties-headers
    (message-properties message))
   name))

(defun message-property-value (message name)
  (assoc-value (message-properties message) name))

(defun message-body-string (message &optional (encoding :utf-8))
  (babel:octets-to-string (message-body message) :encoding encoding))

(defmethod print-amqp-object ((message message) stream)
  (format stream "~@[consumer-tag=~a ~]delivery-tag=~a" (message-consumer-tag message) (message-delivery-tag message)))

(defmethod print-object ((message message) stream)
  (print-unreadable-object (message stream :type t :identity t)
    (print-amqp-object message stream)))
