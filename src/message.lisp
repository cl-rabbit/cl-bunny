(in-package :cl-bunny)

(defclass message ()
  ((channel      :type channel
                 :initarg :channel
                 :reader :message-channel)
   (consumer-tag :type string
                 :initarg :consumer-tag
                 :reader message-consumer-tag)
   (delivery-tag :type integer
                 :initarg :delivery-tag
                 :reader message-delivery-tag)
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

(defun create-message (channel envelope)
  (make-instance 'message
                 :channel channel
                 :consumer-tag (cl-rabbit:envelope/consumer-tag envelope)
                 :delivery-tag (cl-rabbit:envelope/delivery-tag envelope)
                 :exchange (cl-rabbit:envelope/exchange envelope)
                 :routing-key (cl-rabbit:envelope/routing-key envelope)
                 :properties (cl-rabbit:message/properties (cl-rabbit:envelope/message envelope))
                 :body (cl-rabbit:message/body (cl-rabbit:envelope/message envelope))))
