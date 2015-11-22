(in-package :cl-bunny)

(defclass exchange ()
  ((channel :type channel
            :initform nil
            :initarg :channel
            :reader exchange-channel)
   (name :type string
         :initarg :name
         :reader exchange-name)
   (type :type string
         :initform "direct"
         :initarg :type
         :reader exchange-type)
   (durable :initarg :durable
            :reader exchange-durable-p)
   (auto-delete :initarg :auto-delete
                :reader exchange-auto-delete-p)
   (internal :initarg :internal
                :reader exchange-internal-p)
   (arguments :initarg :arguments
              :reader exchange-arguments)
   (on-return :type function
              :initform nil
              :accessor exchange-on-return-callback)))

(defmethod print-object ((exchange exchange) s)
  (print-unreadable-object (exchange s :type t :identity t)
    (format s "~s" (exchange-name exchange))))

(defmethod exchange-name ((exchange string))
  exchange)

(defmethod exchange-channel ((exchange string))
  nil)

(defun exchange.exists-p (name)
  (ignore-some-conditions (amqp-error-not-found)
    (with-channel ()
      (exchange.declare name
                        :passive t))))

(defun exchange.default (&optional (channel *channel*))
  (or
   (get-registered-exchange channel "")
   (register-exchange channel (make-instance 'exchange :name ""
                                                       :durable t
                                                       :channel channel))))

(defun exchange.declare (exchange &key (type "direct") (passive nil) (durable nil) (auto-delete nil) (internal nil) (nowait nil) (arguments nil) (channel *channel*))
  (channel.send% channel
      (make-instance 'amqp-method-exchange-declare
                     :exchange (exchange-name exchange)
                     :type type
                     :passive passive
                     :durable durable
                     :auto-delete auto-delete
                     :internal internal
                     :nowait nowait
                     :arguments arguments)
    (or (get-registered-exchange channel exchange)
        (register-exchange channel (make-instance 'exchange
                                                  :channel channel
                                                  :name (exchange-name exchange)
                                                  :type type
                                                  :durable durable
                                                  :auto-delete auto-delete
                                                  :internal internal
                                                  :arguments arguments)))))

(defun exchange.topic (exchange &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'exchange.declare
         exchange
         (append (list :type "topic")
                 args)))

(defun exchange.fanout (exchange &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'exchange.declare
         exchange
         (append (list :type "fanout")
                 args)))

(defun exchange.direct (exchange &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'exchange.declare
         exchange
         (append (list :type "direct")
                 args)))

(defun exchange.headers (exchange &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'exchange.declare
         exchange
         (append (list :type "headers")
                 args)))

(defun exchange.delete (exchange &key (if-unused nil) (nowait nil) (channel *channel*))
  ;; TODO: deregister exchange
  (channel.send% channel
      (make-instance 'amqp-method-exchange-delete
                     :exchange (exchange-name exchange)
                     :if-unused if-unused
                     :nowait nowait)
    exchange))

(defun exchange.bind (destination source &key (routing-key "") (nowait nil) (arguments nil) (channel *channel*))
  (channel.send% channel
      (make-instance 'amqp-method-exchange-bind
                     :destination destination
                     :source source
                     :routing-key routing-key
                     :nowait nowait
                     :arguments arguments)
    destination))

(defun exchange.unbind (destination source &key (routing-key "") (nowait nil) (arguments nil) (channel *channel*))
  (channel.send% channel
      (make-instance 'amqp-method-exchange-unbind
                     :destination destination
                     :source source
                     :routing-key routing-key
                     :nowait nowait
                     :arguments arguments)
    destination))

(defmethod channel.receive (channel (method amqp-method-basic-return))
  (let* ((message (make-instance 'returned-message :channel channel
                                                   :reply-code (amqp-method-field-reply-code method)
                                                   :reply-text (amqp-method-field-reply-text method)
                                                   :exchange (amqp-method-field-exchange method)
                                                   :routing-key (amqp-method-field-routing-key method)
                                                   :body (amqp-method-content method)
                                                   :properties (amqp-method-content-properties method)))
         (exchange (get-registered-exchange channel (message-exchange message)))
         (callback (or (and exchange
                            (exchange-on-return-callback exchange))
                       (exchange-on-return-callback channel))))
    (if callback
        (funcall callback message)
        (log:warn "Got unhandled returned message"))))
