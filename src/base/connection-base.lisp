(in-package :cl-bunny)

(defvar *connection* nil
  "Current AMQP connection")

(defparameter *connection-type* 'threaded-iolib-connection)
(defparameter *notification-lambda* nil
  "If not NIL expected to be lambda (lambda) -> bb:promise.")

(defparameter *debug-connection* nil)

(defclass connection (connection-in-pool channel-base)
  ((spec :initarg :spec :reader connection-spec)
   (channel-id-allocator :type channel-id-allocator
                         :reader connection-channel-id-allocator)
   (channels :type hash-table
             :initform (make-hash-table :synchronized t)
             :reader connection-channels)
   (event-base :reader connection-event-base :initarg :event-base)
   ;; events
   (on-close :initform (make-instance 'bunny-event)
             :initarg :on-close
             :accessor connection-on-close%)
   (on-error :initform (make-instance 'bunny-event)
             :initarg :on-error
             :accessor connection-on-error%)))

(defmethod channel-id ((channel connection))
  0)

(defmethod channel-connection ((channel connection))
  connection)

(defmethod channel-id ((channel fixnum))
  channel)

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

(defgeneric connection-open-p% (connection)
  (:method ((connection connection))
    (eq (channel-state connection) :open)))

(defun connection-open-p (&optional (connection *connection*))
  (connection-open-p% connection))

(defun check-connection-alive (connection)
  (when (connection-open-p connection)
    connection))

(defun connection-on-close (&optional (connection *connection*))
  (connection-on-close% connection))

(defun get-connection-type (spec)
  (or *connection-type*
      (if (= 0 (connection-spec-heartbeat-interval spec))
          'librabbitmq-connection
          'threaded-librabbitmq-connection)))

(defgeneric connection.new% (connection-type spec pool-tag))

(defun connection.new (&optional (spec "amqp://") &key (heartbeat +heartbeat-interval+) pool-tag)
  (assert (or (positive-integer-p heartbeat)
              :default))
  (let ((spec (make-connection-spec spec)))
    (unless (= heartbeat +heartbeat-interval+)
      (setf (connection-spec-heartbeat-interval spec) heartbeat))
    (connection.new% (get-connection-type spec) spec (or pool-tag (with-output-to-string (s) (print-amqp-object spec s))))))

(defgeneric connection.open% (connection)
  (:method ((connection connection))
    (connection.init connection)
    connection))

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
  (if (= 0 channel-id)
      connection
      (gethash channel-id (connection-channels connection))))

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
  (destructuring-bind (spec &key shared (heartbeat 0) (type '*connection-type*)) (parse-with-connection-params params)
    (with-gensyms (connection-spec-val shared-val)
      `(let* ((,connection-spec-val ,spec)
              (,shared-val ,shared)
              (*connection* (let ((*connection-type* (or ,type
                                                         (if ,shared-val 'threaded-librabbitmq-connection
                                                             'librabbitmq-connection))))
                              (if ,shared-val
                                  (connections-pool.find-or-run (if (eq t ,shared-val) ,connection-spec-val ,shared-val) ,connection-spec-val)
                                  (connection.open (connection.new ,connection-spec-val :heartbeat ,heartbeat))))))
         (unwind-protect
              (progn
                ,@body)
           (unless ,shared-val
             (connection.close)))))))

(defgeneric connection.consume% (connection timeout one-shot))

(defun connection.consume (&key (connection *connection*) (timeout 1) one-shot)
  (assert connection)
  (assert (connection-open-p connection) () 'connection-closed-error :connection connection)
  (connection.consume% connection timeout one-shot))

(defgeneric execute-on-connection-thread (connection channel lambda))

(defmethod execute-on-connection-thread ((connection connection) channel lambda)
  "Single-thread sync connection"
  (assert (connection-open-p connection) () 'connection-closed-error :connection connection)
  (funcall lambda))
