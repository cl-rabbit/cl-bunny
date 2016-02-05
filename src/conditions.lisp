(in-package :cl-bunny)

(define-condition stop-connection (error)
  ())

(define-condition error-base (error)
  ())

(define-condition transport-error (amqp-protocol-error)
  ((description :initform "" :initarg :description :reader transport-error-description)))

(define-condition network-error (transport-error)
  ())

(define-condition unknown-consumer-error (error-base)
  ((message :initarg :message
            :reader error-message
            :type message)))

(define-condition channel-already-open (error-base)
  ((channel :initarg :channel
            :reader error-channel
            :type channel)))

(define-condition channel-consumer-already-added (error-base)
  ((channel :initarg :channel
            :reader error-channel
            :type channel)
   (consumer-tag :initarg :consumer-tag
                 :reader error-consumer-tag
                 :type string)))

(define-condition connection-closed-error (error-base)
  ((connection :initarg :connection
               :reader error-connection
               :type connection)))

(define-condition channel-closed-error (error-base)
  ((channel :initarg :channel
            :reader error-channel
            :type channel)))

(define-condition server-error (error-base)
  ((connection :initarg :connection
               :reader error-connection
               :type connection)))

(define-condition authentication-error (server-error)
  ())

(defun close-method-to-error (channel method)
  (let ((error-type (amqp-error-type-from-reply-code (amqp-method-field-reply-code method))))
    (make-condition error-type
                    :reply-code (amqp-method-field-reply-code method)
                    :reply-text (amqp-method-field-reply-text method)
                    :connection (channel-connection channel)
                    :channel channel
                    :class-id (amqp-method-field-class-id method)
                    :method-id (amqp-method-field-method-id method))))
