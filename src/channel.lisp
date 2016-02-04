(in-package :cl-bunny)

(defvar *channel* nil
  "Current AMQP channel")

(deftype channel-mode ()
  `(and symbol (member :default :transactional :consume)))

(defclass channel (channel-base)
  ((mailbox    :type safe-queue:mailbox
               :initarg :mailbox
               :initform (safe-queue:make-mailbox :name "AMQP Channel mailbox")
               :reader channel-mailbox)
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

   ;; events
   (on-error :type function
             :initform (make-instance 'bunny-event)
             :initarg :on-error
             :accessor channel-on-error%)
   (on-return :type function
              :initform (make-instance 'bunny-event)
              :accessor channel-on-return%)))

(defmethod channel-connection ((connection connection))
  connection)

(defmethod channel-open-p% ((connection connection))
  (connection-open-p connection))

(defmethod channel-open-p (&optional (channel *channel*))
  (channel-open-p% channel))

;; (defun channel-method-queue.push (channel method)
;;   (lparallel.raw-queue:push-raw-queue method (channel-method-queue channel)))

;; (defun channel-method-queue.pop (channel)
;;   (lparallel.raw-queue:pop-raw-queue (channel-method-queue channel)))

;; (defun channel-method-queue-count (channel)
;;   (lparallel.raw-queue:raw-queue-count (channel-method-queue channel)))

(defun channel.new (&key on-error (connection *connection*) (channel-id))
  (assert connection)
  (assert (connection-open-p connection) nil 'connection-closed-error :connection connection)
  (assert (or (null channel-id) (and (positive-integer-p channel-id)
                                     (<= channel-id (connection-channel-max% connection)))))
  ;; with-read-lock (connection-state-lock connection)
  (let ((promise (make-sync-promise)))
    (execute-in-connection-thread (connection)
      (let ((channel (make-instance 'channel :connection connection
                                             :id channel-id
                                             :method-assembler (make-instance 'method-assembler :max-body-size (- (connection-frame-max% connection) 9)))))
        (when on-error
          (event+ (channel-on-error% channel) on-error))
        (connection.register-channel channel)
        (promise.resolve promise channel)))
    (promise.force promise :timeout *force-timeout*)))

(defun channel-on-error (&optional (channel *channel*))
  (channel-on-error% channel))

(defun channel-on-return (&optional (channel *channel*))
  (channel-on-return% channel))

(defgeneric channel.send (channel method)
  (:documentation "API Endpoint, hides transport implementation"))

(defun channel.send (channel method &optional sync-callback)
  (multiple-value-bind (sync reply-matcher) (amqp-method-synchronous-p method)
    (if sync
        (let ((promise (make-sync-promise)))
          (setf (channel-expected-reply channel) (list reply-matcher (if sync-callback
                                                                         (lambda (reply)
                                                                           (promise.resolve promise 
                                                                             (funcall sync-callback reply)))
                                                                         (lambda (reply)
                                                                           (promise.resolve promise reply)))))
          (connection.send (channel-connection channel) channel method)
          (promise.force promise :timeout *force-timeout*))
        (connection.send (channel-connection channel) channel method))))

(defmacro channel.send% (channel method &body body)
  `(let ((reply (channel.send ,channel ,method)))
     (declare (ignorable reply))
     ,@body))

(defmacro channel.send-with-callback% (channel method &body body)
  `(channel.send ,channel ,method (lambda (reply)
                                    (declare (ignorable reply))
                                    ,@body)))

(defun channel.open (&optional (channel *channel*))
  (channel.send% channel
      (make-instance 'amqp-method-channel-open)
    (setf (channel-state channel) :open)
    channel))

(defun channel.new.open (&key on-error (connection *connection*) (channel-id))
  (assert connection)
  (assert (connection-open-p connection) nil 'connection-closed-error :connection connection)
  ;; TODO: if open fails automatically generated channel-id should be released
  (channel.open (channel.new :on-error on-error
                             :connection connection
                             :channel-id (or channel-id
                                             (next-channel-id (connection-channel-id-allocator connection))))))

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
  (let ((method (make-instance 'amqp-method-channel-close
                         :reply-code reply-code
                         :reply-text reply-text
                         :class-id class-id
                         :method-id method-id)))
    (handler-case
        (channel.send% channel
            method
            (setf (channel-open-p% channel) nil)     ;; TODO: <- unwind-protect?
          (safe-queue:mailbox-send-message (channel-mailbox channel) method) ;; TODO: maybe check if there any sync consumers first?
          (connection.deregister-channel channel))
      (connection-closed-error () (log:debug "Closing channel on closed connection")))))

(defun channel.safe-close (reply-code class-id method-id &key (reply-text "") (channel *channel*))
  (ignore-some-conditions (connection-closed-error network-error)
    (ignore-some-conditions (channel-closed-error)
      (let ((reply (channel.send channel (make-instance 'amqp-method-channel-close
                                                        :reply-code reply-code
                                                        :reply-text reply-text
                                                        :class-id class-id
                                                        :method-id method-id))))
        (declare (ignorable reply))
        (setf (channel-state channel) :closed) ;; TODO: <- unwind-protect?
        (connection.deregister-channel channel)))))

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

(defun deregister-exchange (channel exchange)
  (remhash (exchange-name exchange) (channel-exchanges channel)))

(defun channel.receive-frame (channel frame)
  (log:debug frame)
  (when-let ((method (consume-frame (channel-method-assembler channel) frame)))
    (log:debug method)
    (channel.receive channel method)))

(defmethod channel.receive (channel method)
  (destructuring-bind (reply-matcher callback) (channel-expected-reply channel)
    (when (and reply-matcher (funcall reply-matcher method))
      (setf (channel-expected-reply channel) nil)
      (funcall callback method))))

(defmethod channel.receive (channel (method amqp-method-channel-close))
  (log:debug "Received channel.close ~a" (amqp-method-field-reply-text method))
  (channel.close-ok% channel)
  (setf (channel-open-p% channel) nil)
  (safe-queue:mailbox-send-message (channel-mailbox channel) method) ;; TODO: maybe check if there any sync consumers first?
  (let ((error-type (ignore-errors (amqp-error-type-from-reply-code (amqp-method-field-reply-code method)))))
    (when (and error-type (channel-on-error% channel))
      (event! (channel-on-error% channel) (make-condition error-type
                                                          :reply-code (amqp-method-field-reply-code method)
                                                          :reply-text (amqp-method-field-reply-text method)
                                                          :connection (channel-connection channel)
                                                          :channel channel
                                                          :class-id (amqp-method-field-class-id method)
                                                          :method-id (amqp-method-field-method-id method))))))
