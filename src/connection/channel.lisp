(in-package :cl-bunny)

(deftype channel-mode ()
  `(and symbol (member :default :transactional :consume)))

(defclass channel ()
  ((connection :type connection
               :initarg :connection
               :reader channel-connection)
   (mailbox    :type safe-queue:mailbox
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

(defclass confirm-channel (channel)
  ((counter :initform 1 :type fixnum :accessor confirm-channel-counter)
   (unconfirmed-set :initform (make-hash-table :test 'eql) :accessor channel-unconfirmed-set)
   (lock :initform (bt:make-lock) :reader confirm-channel-lock)
   (condition-var :initform (bt:make-condition-variable) :reader confirm-channel-condition-var)))

(defmethod channel.wait-confirms% ((channel confirm-channel) timeout)
  (bt:with-lock-held ((confirm-channel-lock channel))
    (when (= 0 (hash-table-count (channel-unconfirmed-set channel)))
      (return-from channel.wait-confirms% t))
    (if (eq (bt:current-thread) (connection-thread (channel-connection channel)))
        (error "Waiting for confirms on connection thread not supported yet")
        (sb-thread:condition-wait (confirm-channel-condition-var channel) (confirm-channel-lock channel) :timeout timeout))))

(defun channel.wait-confirms (&key (channel *channel*) timeout)
  (channel.wait-confirms% channel timeout))

(defvar *channel*)
(defconstant +max-channels+ 320)

(defun channel.new (&key (connection *connection*) (channel-id (next-channel-id (connection-channel-id-allocator connection))))
  (make-instance 'channel :connection connection
                          :id channel-id))

(defun channel.open (channel)
  (amqp-channel-open channel)
  channel)

(defun channel.new.open (&optional (connection *connection*))
  (channel.open (channel.new :connection connection)))

(defun channel.close (&optional (channel *channel*))
  (amqp-channel-close channel))

(defun (setf channel-prefetch) (value channel &key global)
  (amqp-basic-qos value :global global :channel channel))

(defun channel-consume-message (channel message &key return)
  (if-let ((consumer (find-message-consumer channel message)))
    (if (eq :sync (consumer-type consumer))
        (mailbox-send-message (channel-mailbox channel) message)
        (execute-consumer consumer message))
    (log:error "Unknown consumer tag ~a." (message-consumer-tag message))))

(defgeneric channel.send (channel method)
  (:documentation "API Endpoint, hides transport implementation"))

(defmethod channel.send :around (channel method)
  (assert (channel-open-p channel) () 'channel-closed-error :channel channel)
  (assert (connection-alive-p (channel-connection channel)) () 'connection-closed-error :connection (channel-connection channel))
  (if (eq (bt:current-thread) (connection-thread (channel-connection channel)))
      ;; we are inside of connection thread, just return promise
      (call-next-method)
      ;; we are calling from different thread,
      ;; for now we accept this as call from regular sync lisp code
      ;; use lparallel promise to lift errors
      (let ((promise (lparallel:promise)))
        (execute-in-connection-thread ((channel-connection channel))
          (blackbird:catcher
           (blackbird:attach
            (channel.send channel method)
            (lambda (&rest vals)
              (lparallel:fulfill promise (values-list vals))))
           (t (e) (lparallel:fulfill promise (lparallel.promise::wrap-error e)))))
        (lparallel:force promise))))

(defmethod channel.send (channel method)
  (connection.send (channel-connection channel) channel method))

(defmacro channel.send% (channel method &body body)
  (with-gensyms (cb)
    `(let ((reply (channel.send ,channel ,method)))
       (flet ((,cb (reply)
                (declare (ignorable reply))
                ,@body))
         (if (blackbird:promisep reply)
             (blackbird:attach reply (function ,cb))
             (,cb reply))))))

(defun channel.confirm (&optional (channel *channel*))
  (channel.send% channel (make-instance 'amqp-method-confirm-select)
    (change-class channel 'confirm-channel)))

(defgeneric channel.receive (channel method))

(defmethod channel.receive ((channel confirm-channel) (method amqp-method-basic-ack))
  (log:debug "Ack received ~a" method)
  (bt:with-lock-held ((confirm-channel-lock channel))
    (let ((unconfirmed-set (channel-unconfirmed-set channel)))
      (unless
          (remhash (amqp-method-field-delivery-tag method) unconfirmed-set)
        (error "Unknown delivery-tag ~a" (amqp-method-field-delivery-tag method)))
      (when (amqp-method-field-multiple method)
          (with-hash-table-iterator (next-entry unconfirmed-set)
            (loop (multiple-value-bind (more delivery-tag message) (next-entry)
                    (declare (ignore message))
                    (unless more (return nil))
                    (when (< delivery-tag (amqp-method-field-delivery-tag method))
                      (remhash delivery-tag unconfirmed-set)))))))
    (bt:condition-notify (confirm-channel-condition-var channel))))

(defmethod channel.publish (channel content exchange &key (routing-key "") (mandatory nil) (immediate nil) (properties (make-instance 'amqp-basic-class-properties)))
  (channel.send% channel
      (make-instance 'amqp-method-basic-publish
                     :exchange exchange
                     :routing-key (routing-key routing-key)
                     :mandatory mandatory
                     :immediate immediate
                     :content content
                     :content-properties properties)))

(defmethod channel.publish ((channel confirm-channel) content exchange &key (routing-key "") (mandatory nil) (immediate nil) (properties (make-instance 'amqp-basic-class-properties)))
  (assert (channel-open-p channel) () 'channel-closed-error :channel channel)
  (bt:with-lock-held ((confirm-channel-lock channel))
    (let ((unconfirmed-set (channel-unconfirmed-set channel)))
      (setf (gethash (confirm-channel-counter channel) unconfirmed-set) content))
    (incf (confirm-channel-counter channel))) ;; check for max counter value here
  (call-next-method))

(defun parse-with-channel-params (params)
  (etypecase params
    (string (list params :close t))
    (symbol (list params :close t))
    (list params)))

(defmacro with-channel (params &body body)
  (destructuring-bind (channel &key close) (parse-with-channel-params params)
    (with-gensyms (allocated-p channel-val close-val)
      `(let ((,channel-val ,channel)
             (,close-val ,close))
         (multiple-value-bind (*channel* ,allocated-p) (if ,channel-val
                                                           ,channel-val
                                                           (values
                                                            (channel.new.open *connection*)
                                                            t))
           (unwind-protect
                (progn
                  ,@body)
             (when (and ,close-val ,allocated-p)
               (channel.close *channel*))))))))


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
