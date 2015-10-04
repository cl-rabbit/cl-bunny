(in-package :cl-bunny)

(defparameter *connections-pool* (make-hash-table :test #'equal))
;; TODO: maybe replace with synchronized hash-table on sbcl?
(defvar *connections-pool-lock* (bt:make-lock "CL-BUNNY connections pool lock"))

(defvar *connection*)

(defun connection-alive-p (connection)
  (and connection
       (slot-boundp connection 'connection-thread)
       (bt:thread-alive-p (connection-thread connection))))

(defun check-connection-alive (connection)
  (when (connection-alive-p connection)
    connection))

(defun get-connection-from-pool (spec)
  (check-connection-alive (gethash spec *connections-pool*)))

(defun add-connection-to-pool (spec connection)
  (setf (gethash spec *connections-pool*)
        connection))

(defun remove-connection-from-pool (connection)
  (bt:with-lock-held (*connections-pool-lock*)
    (remhash (connection-spec connection) *connections-pool*)))

(defun find-or-create-connection (spec)
  (bt:with-lock-held (*connections-pool-lock*)
    (or (get-connection-from-pool spec)
        (create-new-connection spec))))

(defun create-new-connection (spec)
  (let ((connection (make-connection-object spec)))
    (add-connection-to-pool spec connection)
    (connection-run connection)
    connection))

(defclass connection ()
  ((spec :initarg :spec :reader connection-spec)

   (cl-rabbit-connection          :type cl-rabbit::connection
                                  :initarg :connection
                                  :reader connection-cl-rabbit-connection)
   (channel-id-allocator :type channel-id-allocator
                         :initform (new-channel-id-allocator +max-channels+)
                         :reader connection-channel-id-allocator)

   (channels :type hash-table
             :initform (make-hash-table :synchronized t)
             :reader connection-channels)

   (event-base :initform (make-instance 'iolib:event-base) :reader connection-event-base :initarg :event-base)

   (control-fd :initform (eventfd:eventfd.new 0))
   (control-mailbox :initform (make-queue) :reader connection-control-mailbox)
   (execute-in-connection-lambda :initform nil :reader connection-lambda)

   (connection-thread :reader connection-thread)))

(defun setup-execute-in-connection-lambda (connection)
  (with-slots (control-fd control-mailbox execute-in-connection-lambda) connection
    (setf execute-in-connection-lambda
          (lambda (thunk)
            (enqueue thunk control-mailbox)
            (log:debug "Notifying connection thread")
            (eventfd.notify-1 control-fd)))))

(defmacro execute-in-connection-thread ((&optional (connection '*connection*)) &body body)
  `(funcall (connection-lambda ,connection)
            (lambda () ,@body)))

(defmacro execute-in-connection-thread-sync ((&optional (connection '*connection*)) &body body)
  (with-gensyms (lock condition return connection%)
    `(let ((,lock (bt:make-lock))
           (,condition (bt:make-condition-variable))
           (,return nil)
           (,connection% ,connection))
       (when (connection-alive-p ,connection)
         (bt:with-lock-held (,lock)
           (funcall (connection-lambda ,connection)
                    (lambda (&aux (*connection* ,connection%))
                      (bt:with-lock-held (,lock)
                        (setf ,return
                              (multiple-value-list
                               (unwind-protect
                                    (progn ,@body)
                                 (bt:condition-notify ,condition)))))))
           (bt:condition-wait ,condition ,lock)
           (values-list ,return))))))

(defun make-connection-object (spec)
  (let ((connection (make-instance 'connection :spec (make-connection-spec spec)
                                               :connection (cl-rabbit:new-connection))))
    (setup-execute-in-connection-lambda connection)
    connection))

(defun parse-with-connection-params (params)
  (etypecase params
    (string (list params :one-shot nil))
    (symbol (list params :one-shot nil))
    (list params)))

(defmacro with-connection (params &body body)
  (destructuring-bind (spec &key one-shot) (parse-with-connection-params params)
    `(let ((*connection* (if ,one-shot
                             (create-new-connection ,spec)
                             (find-or-create-connection ,spec))))
       (unwind-protect
            (progn
              ,@body)
         (when ,one-shot
           (connection-close))))))

(defun connection-run (connection)
  (connection-start connection)
  (setf (slot-value connection 'connection-thread)
        (bt:make-thread (lambda () (connection-loop connection)))))

(defun connection-start (connection)
  (with-slots (cl-rabbit-connection cl-rabbit-socket spec) connection
    (cl-rabbit:socket-open (cl-rabbit:tcp-socket-new cl-rabbit-connection) (connection-spec-host spec) (connection-spec-port spec))
    (cl-rabbit:login-sasl-plain cl-rabbit-connection
                                (connection-spec-vhost spec)
                                (connection-spec-login spec)
                                (connection-spec-password spec))))

(defun wait-for-frame (connection)
  (log:trace "Waiting for frame on async connection: ~s" connection)
  (with-slots (channels cl-rabbit-connection) connection
    ;;
    (handler-bind ((cl-rabbit:rabbitmq-library-error
                     (lambda (condition)
                       (when (eq (cl-rabbit:rabbitmq-library-error/error-code condition)
                                 :amqp-unexpected-frame)
                         (log:warn "Got unexpected frame")
                         ;; (process-unexpected-frame cl-rabbit-connection)
                         ))))
      ;;
      (let ((envelope (cl-rabbit:consume-message cl-rabbit-connection :timeout 0)))
        (let* ((channel-id (cl-rabbit:envelope/channel envelope)))
          (labels ((warn-nonexistent-channel ()
                     (log:warn "Message received for closed channel: ~a" channel-id)))
            (if-let ((channel (gethash channel-id channels)))
              (if (channel-open-p channel)
                  (channel-consume-message channel (create-message channel envelope))
                  ;; ELSE: We won't deliver messages to a closed channel
                  (log:warn "Incoming message on closed channel: ~s" channel))
              ;; ELSE: Unused channel
              (warn-nonexistent-channel))))))))

(defun connection-loop (connection)
  (with-slots (cl-rabbit-connection control-fd control-mailbox event-base) connection
    (iolib:with-event-base (event-base)
      (iolib:set-io-handler event-base
                            control-fd
                            :read (lambda (fd e ex)
                                    (declare (ignorable fd e ex))
                                    (log:debug "Got lambda to execute on connection thread ~a" (eventfd.read control-fd))
                                    (loop for lambda = (dequeue control-mailbox)
                                          while lambda
                                          do (funcall lambda))))
      (iolib:set-io-handler event-base
                            (cl-rabbit::get-sockfd cl-rabbit-connection)
                            :read (lambda (fd e ex)
                                    (declare (ignorable fd e ex))
                                    (wait-for-frame connection)))

      (handler-case
          (loop
            if (or (cl-rabbit::data-in-buffer cl-rabbit-connection)
                   (cl-rabbit::frames-enqueued cl-rabbit-connection))
            do (wait-for-frame connection)
            else
            do (iolib:event-dispatch event-base :one-shot t))
        (stop-connection ()
          (eventfd.close control-fd)
          (cl-rabbit:destroy-connection cl-rabbit-connection))))))

(defun connection-close (&optional (connection *connection*))
  (when (connection-alive-p connection)
    (execute-in-connection-thread (connection)
      (error 'stop-connection))
    (bt:join-thread (connection-thread connection)))
  (remove-connection-from-pool connection))

(defun allocate-and-open-new-channel (connection)
  (let* ((channel (make-channel connection)))
    (amqp-channel-open channel)
    channel))

(defmacro with-channel (channel &body body)
  (with-gensyms (allocated-p)
    `(multiple-value-bind (*channel* ,allocated-p) (if ,channel
                                                       ,channel
                                                       (values
                                                        (allocate-and-open-new-channel *connection*)
                                                        t))
       (unwind-protect
            (progn
              ,@body)
         (when ,allocated-p
           (amqp-channel-close *channel*))))))
