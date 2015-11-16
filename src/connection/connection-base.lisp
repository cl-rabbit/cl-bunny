(in-package :cl-bunny)

(defvar *connection* nil
  "Current AMQP connection")

(defparameter *connection-type* 'librabbitmq-connection)

(defclass connection ()
  ((spec :initarg :spec :reader connection-spec)
   (channel-id-allocator :type channel-id-allocator
                         :initform (new-channel-id-allocator +max-channels+)
                         :reader connection-channel-id-allocator)
   (channels :type hash-table
             :initform (make-hash-table :synchronized t)
             :reader connection-channels)
   (pool-tag :initarg :pool-tag :accessor connection-pool-tag)
   (pool :initform nil :accessor connection-pool)
   (state :initform :closed :reader connection-state)))
