(in-package :cl-bunny)

(defclass queue ()
  ((name :type string
         :initarg :name
         :reader queue-name)
   (server-named :type boolean
                 :initarg :server-named
                 :reader queue-server-named-p)
   (durable :initarg :durable
            :reader queue-durable-p)
   (exclusive :initarg :exclusive
              :reader queue-exclusive-p)
   (auto-delete :initarg :auto-delete
                :reader queue-auto-delete-p)
   (arguments :initarg :arguments
              :reader queue-arguments)
   (mailbox :type safe-queue:mailbox
            :initarg :mailbox
            :initform (safe-queue:make-mailbox :name "AMQP Queue mailbox"))))

;; maybe there should be queue object with its own mailbox?

(defmethod queue-name ((queue string))
  queue)

(defmethod reply-to ((queue string))
  queue)

(defmethod reply-to ((queue queue))
  (queue-name queue))

(defun queue.exists-p (name)
  (ignore-some-conditions (amqp-error-not-found)
    (with-channel ()
      (queue.declare :name name
                     :passive t))))

(defun queue.declare (&key (name "") (passive) (durable) (exclusive nil) (auto-delete nil) (nowait) (arguments nil) (channel *channel*))
  ;; (when (and (equal name "")
  ;;            (or exclusive (not exclusive-supplied-p))
  ;;            (or auto-delete (not auto-delete-supplied-p)))
  ;;   (setf exclusive t
  ;;         auto-delete t))
  ;; todo register exclusive queues
  ;; mark them as invalid on connection close
  (channel.send% channel (make-instance 'amqp-method-queue-declare
                                        :queue name
                                        :passive passive
                                        :durable durable
                                        :exclusive exclusive
                                        :auto-delete auto-delete
                                        :nowait nowait
                                        :arguments arguments)
    (values (make-instance 'queue :name (amqp-method-field-queue reply)
                                  :server-named (equal name "")
                                  :durable durable
                                  :exclusive exclusive
                                  :auto-delete auto-delete
                                  :arguments arguments)
            (amqp-method-field-message-count reply)
            (amqp-method-field-consumer-count reply))))

(defun queue.declare-temp (&key (auto-delete nil) (nowait) (arguments nil) (channel *channel*))
  (queue.declare :name ""
                 :passive nil
                 :durable nil
                 :exclusive t
                 :auto-delete auto-delete
                 :nowait nowait
                 :arguments arguments
                 :channel channel))

(defun queue.bind (queue exchange &key (routing-key "") (nowait nil) (arguments nil) (channel *channel*))
  (channel.send% channel
                 (make-instance 'amqp-method-queue-bind
                   :queue (queue-name queue)
                   :exchange (exchange-name exchange)
                   :routing-key routing-key
                   :nowait nowait
                   :arguments arguments)
    queue))

(defun queue.purge (queue &key (nowait) (channel *channel*))
  (channel.send% channel (make-instance 'amqp-method-queue-purge
                                        :queue (queue-name queue)
                                        :nowait nowait)
    queue))

(defun queue.delete (&optional queue &key (if-unused) (if-empty) (nowait) (channel *channel*))
  (channel.send% channel
                 (make-instance 'amqp-method-queue-delete
                                :queue (queue-name queue)
                                :if-unused if-unused
                                :if-empty if-empty
                                :nowait nowait)
    queue))

(defun queue.unbind (queue exchange &key (routing-key "") (arguments nil) (channel *channel*))
  (channel.send% channel
                 (make-instance 'amqp-method-queue-unbind
                   :queue (queue-name queue)
                   :exchange (exchange-name exchange)
                   :routing-key routing-key
                   :arguments arguments)
    queue))

(defun queue.status (queue &key (channel *channel*))
  (channel.send% channel (make-instance 'amqp-method-queue-declare
                                        :passive t
                                        :queue (queue-name queue)
                                        :durable (queue-durable-p queue)
                                        :exclusive (queue-exclusive-p queue)
                                        :auto-delete (queue-auto-delete-p queue)
                                        :arguments (queue-arguments queue))
    (values (amqp-method-field-message-count reply)
            (amqp-method-field-consumer-count reply))))

(defun queue.message-count (queue &key (channel *channel*))
  (multiple-value-bind (message-count consumer-count)
      (queue.status queue :channel channel)
      (declare (ignore consumer-count))
    message-count))

(defun queue.consumer-count (queue &key (channel *channel*))
  (multiple-value-bind (message-count consumer-count)
      (queue.status queue :channel channel)
      (declare (ignore message-count))
    consumer-count))

(defun queue.put (queue content &key (mandatory nil) (immediate nil) (channel *channel*) (properties (make-instance 'amqp-basic-class-properties)) (nowait))
  (publish "" content :routing-key queue
                      :mandatory mandatory
                      :immediate immediate
                      :properties properties
                      :nowait nowait
                      :channel channel))

(defun queue.peek (&optional (queue ""))
  (with-channel ()
    (let ((message (queue.get queue)))
      (message.nack message :requeue t)
      message)))

(defun queue.get (&optional (queue "") &key (no-ack nil) (channel *channel*))
  (channel.send% channel
      (make-instance 'amqp-method-basic-get
                     :queue (queue-name queue)
                     :no-ack no-ack)
    (if (typep reply 'amqp-method-basic-get-empty)
        (values nil (amqp-method-field-cluster-id reply))
        (values (make-instance 'message
                               :channel channel                       
                               :body (amqp-method-content reply)
                               :properties (amqp-method-content-properties reply)
                               :routing-key (amqp-method-field-routing-key reply)
                               :exchange (amqp-method-field-exchange reply)
                               :redelivered (amqp-method-field-redelivered reply)
                               :delivery-tag (amqp-method-field-delivery-tag reply))
                (amqp-method-field-message-count reply)))))
