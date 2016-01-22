(in-package :cl-bunny)

(defun message-content-type (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-content-type (message-properties message))))

(defun message-content-encoding (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-content-encoding (message-properties message))))

(defun message-headers (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-headers (message-properties message))))

(defun message-persistent-p (message)
  (ecase
      (ignore-some-conditions (unbound-slot)
        (amqp-property-delivery-mode (message-properties message)))
    (nil nil)
    (1 nil)
    (2 t)))

(defun message-priority (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-priority (message-properties message))))

(defun message-correlation-id (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-correlation-id (message-properties message))))

(defun message-reply-to (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-reply-to (message-properties message))))

(defun message-expiration (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-expiration (message-properties message))))

(defun message-message-id (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-message-id (message-properties message))))

(defun message-timestamp (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-timestamp (message-properties message))))

(defun message-type (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-type (message-properties message))))

(defun message-user-id (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-user-id (message-properties message))))

(defun message-app-id (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-app-id (message-properties message))))

(defun message-cluster-id (message)
  (ignore-some-conditions (unbound-slot)
    (amqp-property-cluster-id (message-properties message))))

(defun header-value (headers name)
  (assoc-value headers name :test #'equal))

(defun message-header-value (message name)
  (header-value
   (message-headers message)
   name))
