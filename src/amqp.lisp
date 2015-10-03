(in-package :cl-bunny)

(defvar *connection* nil
  "Current AMQP connection")

(defvar *channel* nil
  "Current AMQP channel")


(defun amqp-channel-open (channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (with-slots (connection open-p number) channel
      (when open-p
        (error 'channel-already-open :channel channel))
      (cl-rabbit:channel-open (connection-cl-rabbit-connection connection)
                              number)
      (setf open-p t)
      (setf (gethash number (connection-channels connection)) channel))))

(defun amqp-channel-close (channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (with-slots (connection open-p number) channel
      (when open-p
        (ignore-errors ;; TODO: Are you sure?
         (cl-rabbit:channel-close (connection-cl-rabbit-connection connection)
                                  number)))
      (setf open-p nil)
      (if (eql channel  (gethash number (connection-channels connection)))
          (remhash number (connection-channels connection)))
      (release-channel-id (connection-channel-id-allocator connection) number))))

(defun amqp-queue-declare (name &rest args &key passive durable exclusive auto-delete arguments (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:queue-declare (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-number channel) :queue name) args))))

(defun amqp-basic-publish (body &rest args &key (exchange "") routing-key mandatory immediate properties
                                      (encoding :utf-8)
                                      (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:basic-publish (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-number channel) :body body) args))))

(defun amqp-basic-consume (queue &rest args &key consumer-tag no-local no-ack exclusive arguments
                                             (channel *channel*))
  (remf args :channel)
  (execute-in-connection-thread-sync ((channel-connection channel))
    (apply #'cl-rabbit:basic-consume (append (list (connection-cl-rabbit-connection (channel-connection channel)) (channel-number channel) queue) args))))

;; (defclass connection ()
;;   ((amqp-connection :initarg :amqp-connection-state)
;;    (channel-id-allocator :initarg :channel-id-allocator)))

;; (defmacro with-amqp-connection ((name connection) &body body)
;;   `(let ((,name (slot-value ,connection 'amqp-connection)))
;;      ,@body))

;; (defun amqp-get-channel-max (amqp-connection)
;;   (cl-rabbit::amqp-get-channel-max amqp-connection))

;; (defun amqp-channel-open (channel-id &key (connection *connection*))
;;   (with-amqp-connection (amqp-conneciton conneciton)
;;     (cl-rabbit:channel-open amqp-conneciton
;;                             channel-id)))

;; (defun amqp-channel-close (channel-id &key (code cl-rabbit:+amqp-reply-success+) (connection *connection*))
;;   (with-amqp-connection (amqp-conneciton connection)
;;     (cl-rabbit:channel-close amqp-conneciton channel-id :code code)))



;; (defun amqp-queue-declare (name &rest args &key passive durable exclusive auto-delete arguments (connection *connection*) (channel *channel*))
;;   (remf args :connection)
;;   (remf args :channel)
;;   (with-amqp-connection (amqp-connection connection)
;;     (apply #'cl-rabbit:queue-declare (append (list amqp-connection channel :queue name) args))))

;; (defun amqp-publish (body &rest args &key (exchange "") routing-key mandatory immediate properties
;;                                  (encoding :utf-8)
;;                                  (connection *connection*) (channel *channel*))
;;   (remf args :connection)
;;   (remf args :channel)
;;   (with-amqp-connection (amqp-connection connection)
;;     (apply #'cl-rabbit:basic-publish (append (list amqp-connection channel :body body) args))))
