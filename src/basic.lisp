(in-package :cl-bunny)

(defmethod routing-key ((routing-key string))
  routing-key)

(defmethod routing-key ((routing-key queue))
  (queue-name routing-key))

(defun publish (exchange content &key (routing-key "") (mandatory nil) (immediate nil) (channel *channel*) (properties (make-instance 'amqp-basic-class-properties)))
  (channel.publish (or (exchange-channel exchange) channel)
                   content
                   (exchange-name exchange)
                   :routing-key (routing-key routing-key)
                   :mandatory mandatory
                   :immediate immediate
                   :properties properties))