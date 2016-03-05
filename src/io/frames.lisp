(in-package :cl-bunny)

(amqp:enable-binary-string-syntax)

(defclass cached-frame ()
  ((bytes :reader get-frame-bytes)))

(defclass cached-heartbeat-frame (cached-frame amqp:heartbeat-frame)
  ((bytes :initform #b"\x08\x00\x00\x00\x00\x00\x00\xce")))

(unless (boundp '+heartbeat-frame+)
  (defconstant +heartbeat-frame+ (make-instance 'cached-heartbeat-frame)))

(defclass amqp-frame (cached-frame)
  ((bytes :initform #b"AMQP\x0\x0\x9\x1")))

(unless (boundp '+amqp-frame+)
  (defconstant +amqp-frame+ (make-instance 'amqp-frame)))
