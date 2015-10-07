(in-package :cl-bunny)

(defclass channel ()
  ((connection :type connection
               :initarg :connection
               :reader channel-connection)
   (mailbox    :type #+sbcl sb-concurrency:mailbox #-sbcl cons
               :initarg :mailbox
               :initform (safe-queue:make-mailbox :name "AMQP Channel mailbox")
               :reader channel-mailbox)
   (channel-id :type fixnum
               :initarg :id
               :reader channel-id)
   (open-p     :type boolean
               :initform nil
               :accessor channel-open-p)
   (exchanges :type hash-table
              :initform (make-hash-table :test #'equal)
              :reader channel-exchanges)
   (consumers :type hash-table
              :initform (make-hash-table :test #'equal)
              :reader channel-consumers)
   ;; callbacks
   (on-exchange-return :type function
                       :initform nil
                       :accessor exchange-on-return-callback)))

(defvar *channel*)
(defconstant +max-channels+ 320)

(defun new-channel (connection &optional (channel-id (next-channel-id (connection-channel-id-allocator connection))))
  (make-instance 'channel :connection connection
                          :id channel-id))

(defun channel-open (channel)
  (amqp-channel-open channel)
  channel)

(defun (setf channel-prefetch) (value channel &key global)
  (amqp-basic-qos value :global global :channel channel))


(defun channel-consume-message (channel message &key return)
  (if-let ((consumer (find-message-consumer channel message)))
    (if (eq :sync (consumer-type consumer))
        (mailbox-send-message (channel-mailbox channel) message)
        (execute-consumer consumer message))
    (log:error "Unknown consumer tag ~a." (message-consumer-tag message))))

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
