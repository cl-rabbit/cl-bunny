(in-package :cl-bunny)

(defvar *connection* nil
  "Current AMQP connection")

(defparameter *connection-type* 'librabbitmq-connection)

(defparameter *debug-connection* nil)

(defclass connection (connection-in-pool)
  ((spec :initarg :spec :reader connection-spec)
   (channel-id-allocator :type channel-id-allocator
                         :reader connection-channel-id-allocator)
   (channels :type hash-table
             :initform (make-hash-table :synchronized t)
             :reader connection-channels)
   (state :initform :closed :reader connection-state)
   ;; events
   (on-close :type function
             :initform (make-instance 'bunny-event)
             :initarg :on-close
             :accessor connection-on-close%)))

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

(defun connection-on-close (&optional (connection *connection*))
  (connection-on-close% connection))

(defgeneric connection.new% (connection-type spec pool-tag))

(defun connection.new (&optional (spec "amqp://") &key (heartbeat +heartbeat-interval+) pool-tag)
  (assert (or (positive-integer-p heartbeat)
              :default))
  (let ((spec (make-connection-spec spec)))
    (unless (= heartbeat +heartbeat-interval+)
      (setf (connection-spec-heartbeat-interval spec) heartbeat))
    (connection.new% *connection-type* spec (or pool-tag (with-output-to-string (s) (print-amqp-object spec s))))))

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

(defun parse-with-connection-params-list (params)
  (if (and (keywordp (first params))
           (evenp (length params)))
      (append (list nil) params)
      params))

(defun parse-with-connection-params (params)
  (etypecase params
    (string (list params :shared nil))
    (symbol (list params :shared nil))
    (list (parse-with-connection-params-list params))))

(defmacro with-connection (params &body body)
  (destructuring-bind (spec &key shared (heartbeat 0)) (parse-with-connection-params params)
    (with-gensyms (connection-spec-val shared-val)
      `(let* ((,connection-spec-val ,spec)
              (,shared-val ,shared)
              (*connection* (if ,shared-val
                                (connections-pool.find-or-run ,connection-spec-val)
                                (connection.open (connection.new ,connection-spec-val :heartbeat ,heartbeat)))))
         (unwind-protect
              (progn
                ,@body)
           (when (and (not ,shared-val))
             (connection.close)))))))
