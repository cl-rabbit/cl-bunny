(in-package :cl-bunny)


(defun message-content-type (message)
  (amqp-property-content-type (message-properties message)))

(defun message-content-encoding (message)
  (amqp-property-content-encoding (message-properties message)))

(defun message-headers (message)
  (amqp-property-headers (message-properties message)))

(defun message-persistent-p (message)
  (ecase
      (amqp-property-delivery-mode (message-properties message))
    (1 nil)
    (2 t)))

(defun message-priority (message)
  (amqp-property-priority (message-properties message)))

(defun message-correlation-id (message)
  (amqp-property-correlation-id (message-properties message)))

(defun message-reply-to (message)
  (amqp-property-reply-to (message-properties message)))

(defun message-expiration (message)
  (amqp-property-priority (message-expiration message)))

(defun message-message-id (message)
  (amqp-property-message-id (message-properties message)))

(defun message-timestamp (message)
  (amqp-property-timestamp (message-properties message)))

(defun message-type (message)
  (amqp-property-type (message-properties message)))

(defun message-user-id (message)
  (amqp-property-user-id (message-properties message)))

(defun message-app-id (message)
  (amqp-property-app-id (message-properties message)))

(defun message-cluster-id (message)
  (amqp-property-cluster-id (message-properties message)))

(defun header-value (headers name)
  (assoc-value headers name :test #'equal))

(defun message-header-value (message name)
  (header-value
   (message-headers message)
   name))
