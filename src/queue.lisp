(in-package :cl-bunny)

(defclass queue ()
  ((name :type string
         :initarg :name
         :reader queue-name)
   (passive :initarg :passive
            :reader queue-passive-p)
   (durable :initarg :durable
            :reader queue-durable-p)
   (exclusive :initarg :exclusive
              :reader queue-exclusive-p)
   (auto-delete :initarg :auto-delete
                :reader queue-auto-delete-p)
   (arguments :initarg :arguments
              :reader queue-arguments)
   (mailbox :type #+sbcl sb-concurrency:mailbox #-sbcl cons ;; TODO: make safe-queue:mailbox type
            :initarg :mailbox
            :initform (safe-queue:make-mailbox :name "AMQP Queue mailbox"))))

;; maybe there should be queue object with its own mailbox?

(defmethod queue-name ((queue string))
  queue)

(defun queue.declare (name &key passive durable exclusive auto-delete arguments (channel *channel*))
  (multiple-value-bind (queue-name messages-count consumers-count)
      (amqp-queue-declare name :passive passive
                               :durable durable
                               :exclusive exclusive
                               :auto-delete auto-delete
                               :arguments arguments
                               :channel channel)
    (values (make-instance 'queue :name queue-name
                                  :passive passive
                                  :durable durable
                                  :exclusive exclusive
                                  :auto-delete auto-delete
                                  :arguments arguments)
            messages-count
            consumers-count)))

(defun queue.bind (queue exchange &key routing-key arguments (channel *channel*))
  (amqp-queue-bind (queue-name queue) :exchange (exchange-name exchange)
                                      :routing-key routing-key
                                      :arguments arguments
                                      :channel channel)
  queue)

(defun queue.purge (queue &key (channel *channel*))
  (amqp-queue-purge (queue-name queue) :channel channel)
  queue)

(defun queue.delete (queue &key if-unused if-empty (channel *channel*))
  (amqp-queue-delete (queue-name queue) :if-unused if-unused
                                        :if-empty if-empty
                                        :channel channel))
