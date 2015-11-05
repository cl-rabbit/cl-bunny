(in-package :cl-bunny)

(defmethod routing-key ((routing-key string))
  routing-key)

(defmethod routing-key ((routing-key queue))
  (queue-name routing-key))

(defun publish (exchange content &key (routing-key "") (mandatory nil) (immediate nil) (channel *channel*) (properties (make-instance 'amqp-basic-class-properties)))
  (channel.send% (or channel (exchange-channel exchange))
      (make-instance 'amqp-method-basic-publish
                     :exchange (exchange-name exchange)
                     :routing-key (routing-key routing-key)
                     :mandatory mandatory
                     :immediate immediate
                     :content content
                     :content-properties properties)
    exchange))

