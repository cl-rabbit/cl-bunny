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

(defgeneric channel-send (channel method)
  (:documentation "API Endpoint, hides transport implementation"))

(defmethod channel-send :around (channel method)
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
            (channel-send channel method)
            (lambda (&rest vals)
              (lparallel:fulfill promise (values-list vals))))
           (t (e) (lparallel:fulfill promise (lparallel.promise::wrap-error e)))))
        (lparallel:force promise))))

(defmethod channel-send (channel method)
  (connection-send (channel-connection channel) channel method))

(defmacro channel-send% (channel method &body body)
  (with-gensyms (cb)
    `(let ((reply (channel-send ,channel ,method)))
       (flet ((,cb (reply)
                (declare (ignorable reply))
                ,@body))
         (if (blackbird:promisep reply)
             (blackbird:attach reply (function ,cb))
             (,cb reply))))))

(defun allocate-and-open-new-channel (connection)
  (let ((channel (new-channel connection)))
    (channel-open channel)))

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
                                                            (allocate-and-open-new-channel *connection*)
                                                            t))
           (unwind-protect
                (progn
                  ,@body)
             (when (and ,close-val ,allocated-p)
               (amqp-channel-close *channel*))))))))
