(in-package :cl-bunny)

(defclass exchange ()
  ((channel :type channel
            :initarg :channel
            :reader exchange-channel)
   (name :type string
         :initarg :name
         :reader exchange-name)
   (on-return :type function
              :initform nil
              :accessor exchange-on-return-callback)))

(defun publish (exchange payload &key routing-key mandatory immediate properties
                                  (encoding :utf-8))
  (amqp-basic-publish payload :exchange (exchange-name exchange)
                              :routing-key routing-key
                              :mandatory mandatory
                              :immediate immediate
                              :properties properties
                              :encoding encoding
                              :channel (exchange-channel exchange)))









