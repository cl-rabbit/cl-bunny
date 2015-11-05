(in-package :cl-bunny)

(defclass queue ()
  ((name :type string
         :initarg :name
         :reader queue-name)
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

(defmethod print-object ((queue queue) s)
  (print-unreadable-object (queue s :type t :identity t)
    (format s "~s" (queue-name queue))))

;; maybe there should be queue object with its own mailbox?

(defmethod queue-name ((queue string))
  queue)

(defun queue.declare (&key (name "") (passive) (durable) (exclusive nil exclusive-supplied-p) (auto-delete nil auto-delete-supplied-p) (nowait) (arguments nil) (channel *channel*))
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
                                  :durable durable
                                  :exclusive exclusive
                                  :auto-delete auto-delete
                                  :arguments arguments)
            (amqp-method-field-message-count reply)
            (amqp-method-field-consumer-count reply))))

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

(defun queue.delete (queue &key (if-unused) (if-empty) (nowait) (channel *channel*))
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
