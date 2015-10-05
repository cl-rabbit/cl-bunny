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

(defmethod exchange-name ((exchange string))
  exchange)

(defmethod exchange-channel ((exchange string))
  nil)

(defun publish (exchange payload &key routing-key mandatory immediate properties
                                  (encoding :utf-8)
                                  (channel *channel*))
  (amqp-basic-publish payload :exchange (exchange-name exchange)
                              :routing-key routing-key
                              :mandatory mandatory
                              :immediate immediate
                              :properties properties
                              :encoding encoding
                              :channel (or (exchange-channel exchange)
                                           channel))
  exchange)







