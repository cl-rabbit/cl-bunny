(in-package :cl-bunny)

(defvar *connection* nil
  "Current AMQP connection")

(defparameter *connection-type* 'librabbitmq-connection)

(defparameter *debug-connection* nil)

(defclass connection ()
  ((spec :initarg :spec :reader connection-spec)
   (channel-id-allocator :type channel-id-allocator
                         :reader connection-channel-id-allocator)
   (channels :type hash-table
             :initform (make-hash-table :synchronized t)
             :reader connection-channels)
   (pool-tag :initarg :pool-tag :accessor connection-pool-tag)
   (pool :initform nil :accessor connection-pool)
   (heartbeat :initform 0 :initarg :heartbeat)
   (state :initform :closed :reader connection-state)))

(defgeneric connection-channel-max% (connection))

(defgeneric connection-frame-max% (connection))

(defgeneric connection-heartbeat% (connection))

(defgeneric connection-server-properties% (connection))

(defun connection-channel-max (&optional (connection *connection*))
  (connection-channel-max% connection))

(defun connection-frame-max (&optional (connection *connection*))
  (connection-frame-max% connection))

(defun connection-heartbeat (&optional (connection *connection*))
  (connection-heartbeat% connection))

(defun connection-server-properties (&optional (connection *connection*))
  (connection-server-properties% connection))

(defgeneric connection-open-p% (connection))

(defun connection-open-p (&optional (connection *connection*))
  (connection-open-p% connection))

(defun check-connection-alive (connection)
  (when (connection-open-p connection)
    connection))

(defgeneric connection.new% (connection-type spec shared heartbeat))

(defun connection.new (&optional (spec "amqp://") &key shared (heartbeat 0))
  (assert (or (positive-integer-p heartbeat)
              :default))
  (connection.new% *connection-type* spec shared (if (eq :default heartbeat)
                                                     +heartbeat-interval+
                                                     heartbeat)))

(defgeneric connection.open% (connection))

(defun connection.open (&optional (connection *connection*))
  (connection.open% connection))

(defgeneric connection.close% (connection timeout))

(defun connection.close (&key (connection *connection*) (timeout *force-timeout*))
  (connection.close% connection timeout))

(defgeneric register-channel (connection channel))

(defmethod register-channel ((connection connection) channel)
  (unless (channel-id channel)
    (setf (slot-value channel 'channel-id) (next-channel-id (connection-channel-id-allocator connection))))
  (setf (gethash (channel-id channel) (connection-channels connection)) channel))

(defun connection.register-channel (channel)
  (register-channel (channel-connection channel) channel))

(defgeneric deregister-channel (connection channel))

(defmethod deregister-channel ((connection connection) channel)
  (remhash (channel-id channel) (connection-channels connection))
  (release-channel-id (connection-channel-id-allocator connection) (channel-id channel)))

(defun connection.deregister-channel (channel)
  (deregister-channel (channel-connection channel) channel))

(defgeneric get-channel (connection channel-id))

(defmethod get-channel ((connection connection) channel-id)
  (gethash channel-id (connection-channels connection)))

(defun connection.get-channel (channel-id &key (connection *connection*))
  (get-channel connection channel-id))

(defgeneric connection.send (connection channel method))

(defgeneric connection.receive (connection method))

(defmethod connection.receive ((connection connection) (method amqp-method-connection-close))
  (log:debug "Received connection.closed ~a" method)
  (connection.close-ok% connection nil))

(defmethod connection.receive ((connection connection) (method amqp-method-connection-blocked))
  (log:error "Connection blocked ~a" method))

(defmethod connection.receive ((connection connection) (method amqp-method-connection-unblocked))
  (log:error "Connection unblocked ~a" method))
