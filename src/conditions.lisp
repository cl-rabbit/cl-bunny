(in-package :cl-bunny)

(define-condition stop-connection (error)
  ())

(define-condition error-base (error)
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
