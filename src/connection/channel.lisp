(in-package :cl-bunny)

(defclass channel ()
  ((connection :type connection
               :initarg :connection
               :reader channel-connection)
   (mailbox    :type #+sbcl sb-concurrency:mailbox #-sbcl cons
               :initarg :mailbox
               :initform (safe-queue:make-mailbox :name "AMQP Channel mailbox")
               :reader channel-mailbox)
   (number     :type fixnum
               :initarg :number
               :reader channel-number)
   (open-p     :type boolean
               :initform nil
               :accessor channel-open-p)))

(defvar *channel*)
(defconstant +max-channels+ 65535)

(defun make-channel (connection &optional (number (next-channel-id (connection-channel-id-allocator connection))))
  (make-instance 'channel :connection connection
                          :number number))

(defun channel-consume-message (channel message)
  (mailbox-send-message (channel-mailbox channel) message))
