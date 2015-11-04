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

(defun get-registered-exchange (channel name)
  (gethash name (channel-exchanges channel)))

(defun register-exchange (channel exchange)
  (setf (gethash (exchange-name exchange) (channel-exchanges channel))
        exchange))

(defun default-exchange (&optional (channel *channel*))
  (or
   (get-registered-exchange channel "")
   (register-exchange channel (make-instance 'exchange :channel channel
                                                       :name ""))))

(defun direct-exchange (name &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'amqp-exchange-declare
         (append (list  name
                        :type "direct")
                 args))
  (make-instance 'exchange :name name :channel channel))

(defun topic-exchange (name &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'amqp-exchange-declare
         (append (list  name
                        :type "topic")
                 args))
  (make-instance 'exchange :name name :channel channel))

(defun fanout-exchange (name &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'amqp-exchange-declare
         (append (list  name
                        :type "fanout")
                 args))
  (make-instance 'exchange :name name :channel channel))

(defun headers-exchange (name &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'amqp-exchange-declare
         (append (list  name
                        :type "headers")
                 args))
  (make-instance 'exchange :name name :channel channel))

(defmethod routing-key ((routing-key string))
  routing-key)

(defmethod routing-key ((routing-key queue))
  (queue-name routing-key))

(defun publish (exchange payload &key routing-key mandatory immediate properties
                                  (encoding :utf-8)
                                  (channel *channel*))
  (amqp-basic-publish payload :exchange (exchange-name exchange)
                              :routing-key (routing-key routing-key)
                              :mandatory mandatory
                              :immediate immediate
                              :content-properties properties
                              :encoding encoding
                              :channel (or (exchange-channel exchange)
                                           channel))
  exchange)
