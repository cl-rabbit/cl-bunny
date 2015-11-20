(in-package :cl-bunny)

(defvar *channel* nil
  "Current AMQP channel")
(defconstant +max-channels+ 0)

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
               :accessor channel-open-p%)
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
                       :accessor exchange-on-return-callback)
   (on-error :type function
             :initform nil
             :initarg :on-error
             :accessor channel-on-error-callback%)))

(defmethod channel-connection ((connection connection))
  connection)

(defmethod channel-open-p% ((connection connection))
  (connection-open-p connection))

(defmethod channel-open-p (&optional (channel *channel*))
  (channel-open-p% channel))

(defun channel.new (&key on-error (connection *connection*) (channel-id (next-channel-id (connection-channel-id-allocator connection))))
  (let ((channel (make-instance 'channel :on-error on-error
                                         :connection connection
                                         :id channel-id)))
    (connection.register-channel channel)
    channel))

(defun channel-on-error-callback (&optional (channel *channel*))
  (channel-on-error-callback% channel))

(defun (setf channel-on-error-callback) (cb &optional (channel *channel*))
  (setf (channel-on-error-callback% channel) cb))

(defgeneric channel.send (channel method)
  (:documentation "API Endpoint, hides transport implementation"))

(defmethod channel.send :around (channel method)
  (if (eq (bt:current-thread) (connection-thread (channel-connection channel)))
      ;; we are inside of connection thread, just return promise
      (call-next-method)
      ;; we are calling from different thread,
      ;; for now we accept this as call from regular sync lisp code
      ;; use lparallel promise to lift errors
      (let ((promise (threaded-promise)))
        (execute-in-connection-thread ((channel-connection channel))
          (blackbird:catcher
           (blackbird:attach
            (channel.send channel method)
            (lambda (&rest vals)
              (promise.resolve promise (values-list vals))))
           (amqp-channel-error (e)
                               (log:debug "~a" e)
                               (channel.close-ok% channel)
                               (when (channel-on-error-callback% channel)
                                 (funcall (channel-on-error-callback% channel) e))
                               (promise.reject promise e))
           (amqp-connection-error (e)
                                  (log:debug "~a" e)
                                  (throw 'stop-connection
                                    (lambda ()
                                      (promise.reject promise e))))
           (transport-error (e)
                            (log:debug "~a" e)
                            (throw 'stop-connection
                              (lambda ()
                                (promise.reject promise e))))
           (error (e)
                  (log:debug "~a" e)
                  (promise.reject promise e))))
        (promise.force promise :timeout *force-timeout*))))

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

(defun channel.open (&optional (channel *channel*))
  (channel.send% channel
      (make-instance 'amqp-method-channel-open)
    (setf (channel-open-p% channel) t)
    channel))

(defun channel.new.open (&key on-error (connection *connection*) (channel-id (next-channel-id (connection-channel-id-allocator connection))))
  ;; TODO: if open fails automatically generated channel-id should be released
  (channel.open (channel.new :on-error on-error
                             :connection connection
                             :channel-id channel-id)))

(defun channel.flow (active &key (channel *channel*))
  (channel.send% channel
      (make-instance 'amqp-method-channel-flow
                     :active active)
    (assert (eql active (amqp-method-field-active reply)) nil 'error "channel-flow-ok has different active value") ;; TODO: specialize error
    t))

(defun channel.flow-ok (active &key (channel *channel*))
  (channel.send% channel
      (make-instance 'amqp-method-channel-flow-ok
                     :active active)))

(defun channel.close (reply-code class-id method-id &key (reply-text "") (channel *channel*))
  (handler-case
      (channel.send% channel
          (make-instance 'amqp-method-channel-close
                         :reply-code reply-code
                         :reply-text reply-text
                         :class-id class-id
                         :method-id method-id)
        (setf (channel-open-p% channel) nil)     ;; TODO: <- unwind-protect?
        (connection.deregister-channel channel))
    (connection-closed-error () (log:debug "Closing channel on closed connection"))))

(defun channel.safe-close (reply-code class-id method-id &key (reply-text "") (channel *channel*))
  (ignore-some-conditions (network-error)
    (when (channel-open-p channel)
      (let ((reply (channel.send channel (make-instance 'amqp-method-channel-close
                                                        :reply-code reply-code
                                                        :reply-text reply-text
                                                        :class-id class-id
                                                        :method-id method-id))))
        (flet ((cb (reply)
                 (declare (ignorable reply))
                 (setf (channel-open-p% channel) nil)     ;; TODO: <- unwind-protect?
                 (connection.deregister-channel channel)))
          (if (blackbird:promisep reply)
              (blackbird:attach reply (function cb))
              (cb reply)))))))

(defun channel.close-ok% (channel)
  (channel.send% channel
      (make-instance 'amqp-method-channel-close-ok)
    (setf (channel-open-p% channel) nil)
    (connection.deregister-channel channel)))

#|
TODO: promote :prefetch-size and prefetch-count to channel slots
(defun (setf channel-prefetch) (value channel &key global)
  (qos :prefetch-count value :global global :channel channel))
|#

(defmethod channel.publish (channel content exchange &key (routing-key "") (mandatory nil) (immediate nil) (properties (make-instance 'amqp-basic-class-properties)) &allow-other-keys)
  (channel.send% channel
      (make-instance 'amqp-method-basic-publish
                     :exchange exchange
                     :routing-key (routing-key routing-key)
                     :mandatory mandatory
                     :immediate immediate
                     :content content
                     :content-properties properties)))

(defun parse-with-channel-params-list (params)
  (if (keywordp (first params))
      (append (list nil) params)
      params))

(defun parse-with-channel-params (params)
  (etypecase params
    (string (list params :close t))
    (symbol (list params :close t))
    (list (parse-with-channel-params-list params))))

(defmacro with-channel (params &body body)
  ;; TODO: maybe with-channel should rethrow channel-error?
  #|
  (handler-case (with-channel () ..)
    (channel-error (e) (log:error "Channel closed unexpectedly ~a" e)))
  |#
  (destructuring-bind (channel &key close on-error) (parse-with-channel-params params)
    (with-gensyms (allocated-p channel-val close-val)
      `(let ((,channel-val ,channel)
             (,close-val ,close))
         (multiple-value-bind (*channel* ,allocated-p) (if ,channel-val
                                                           ,channel-val
                                                           (values
                                                            (channel.new.open :connection *connection*
                                                                              :on-error ,on-error)
                                                            t))
           (unwind-protect
                (progn
                  ,@body)
             (when (and ,close-val ,allocated-p)
               (channel.safe-close +amqp-reply-success+ 0 0 :channel *channel*))))))))


(defun get-registered-exchange (channel name)
  (gethash name (channel-exchanges channel)))

(defun register-exchange (channel exchange)
  (setf (gethash (exchange-name exchange) (channel-exchanges channel))
        exchange))


(defgeneric channel.receive (channel method))

(defmethod channel.receive (channel (method amqp-method-channel-close))
  (log:debug "Received channel.close ~a" method)
  (channel.close-ok% channel)
  (let ((error-type (ignore-errors (amqp-error-type-from-reply-code (amqp-method-field-reply-code method)))))
    (when (and error-type (channel-on-error-callback% channel))
      (funcall (channel-on-error-callback% channel) (make-condition error-type
                                                                    :reply-code (amqp-method-field-reply-code method)
                                                                    :reply-text (amqp-method-field-reply-text method)
                                                                    :connection (channel-connection channel)
                                                                    :channel channel
                                                                    :class-id (amqp-method-field-class-id method)
                                                                    :method-id (amqp-method-field-method-id method))))))
