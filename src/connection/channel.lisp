(in-package :cl-bunny)

(deftype channel-mode ()
  `(and symbol (member :default :transactional :consume)))

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
   (exchanges  :type hash-table
               :initform (make-hash-table :test #'equal)
               :reader channel-exchanges)
   (consumers  :type hash-table
               :initform (make-hash-table :test #'equal)
               :reader channel-consumers)
   (mode       :type channel-mode
               :initform :default
               :initarg :mode
               :reader channel-mode)
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
