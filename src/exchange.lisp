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
   ;; events
   (on-return :type function
              :initform (make-instance 'bunny-event)
              :accessor exchange-on-return)))

(defmethod exchange-name ((exchange string))
  exchange)

(defmethod exchange-channel ((exchange string))
  nil)

(defun exchange.exists-p (name)
  (ignore-some-conditions (amqp-error-not-found)
    (with-channel ()
      (exchange.declare name
                        :passive t))))

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

(defun exchange.default (&optional (channel *channel*))
  (or
   (get-registered-exchange channel "")
   (register-exchange channel (make-instance 'exchange :name ""
                                                       :durable t
                                                       :channel channel))))

(defun exchange.topic (&optional exchange &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'exchange.declare
         (or exchange "amq.topic")
         (append (list :type "topic")
                 (if exchange
                     args
                     (list :durable t)))))

(defun exchange.fanout (&optional exchange &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'exchange.declare
         (or exchange "amq.fanout")
         (append (list :type "fanout")
                 (if exchange
                     args
                     (list :durable t)))))

(defun exchange.direct (&optional exchange &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'exchange.declare
         (or exchange "amq.direct")
         (append (list :type "direct")
                 (if exchange
                     args
                     (list :durable t)))))

(defun exchange.headers (&optional exchange &rest args &key passive durable auto-delete internal arguments (channel *channel*))
  (apply #'exchange.declare
         (or exchange "amq.headers")
         (append (list :type "headers")
                 (if exchange
                     args
                     (list :durable t)))))

(defun exchange.match ()
  (exchange.declare "amq.match"
                    :type "headers"
                    :durable t))

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
                     :destination (exchange-name destination)
                     :source (exchange-name source)
                     :routing-key routing-key
                     :nowait nowait
                     :arguments arguments)
    destination))

(defun exchange.unbind (destination source &key (routing-key "") (nowait nil) (arguments nil) (channel *channel*))
  (channel.send% channel
      (make-instance 'amqp-method-exchange-unbind
                     :destination (exchange-name destination)
                     :source (exchange-name source)
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
         (ex-event (and exchange
                        (exchange-on-return exchange)))
         (ch-event (channel-on-return channel))
         (unhandled t))

    (when (and ch-event
               (not (emptyp (event-handlers-list (channel-on-return% channel)))))
      (setf unhandled nil)
      (event! ch-event message))
    (when (and ex-event
               (not (emptyp (event-handlers-list (exchange-on-return exchange)))))
      (setf unhandled nil)
      (event! ex-event message))
    (when unhandled
      (log:warn "Got unhandled returned message"))))
