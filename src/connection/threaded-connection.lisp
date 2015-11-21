(in-package :cl-bunny)

(defparameter *force-timeout* nil)

(defstruct connection-state-lock
  (rlock (bt:make-lock))
  (rtrylock (bt:make-lock))
  (resource (bt:make-lock))
  (rcount 0 :type (unsigned-byte 32)))

(defun read-lock-begin (connection-state-lock)
  (declare (optimize (speed 3) (debug 0) (safety 0)))
  (bt:acquire-lock (connection-state-lock-rtrylock connection-state-lock))
  (bt:acquire-lock (connection-state-lock-rlock connection-state-lock))
  (when (= 1 (incf (connection-state-lock-rcount connection-state-lock)))
    (bt:acquire-lock (connection-state-lock-resource connection-state-lock)))
  (bt:release-lock (connection-state-lock-rlock connection-state-lock))
  (bt:release-lock (connection-state-lock-rtrylock connection-state-lock)))

(defun read-lock-end (connection-state-lock)
  (declare (optimize (speed 3) (debug 0) (safety 0)))
  (bt:acquire-lock (connection-state-lock-rlock connection-state-lock))
  (when (= 0 (decf (connection-state-lock-rcount connection-state-lock)))
    (bt:release-lock (connection-state-lock-resource connection-state-lock)))
  (bt:release-lock (connection-state-lock-rlock connection-state-lock)))

(defmacro with-read-lock (connection-state-lock &body body)
  (with-gensyms (rwlock%)
    `(let ((,rwlock% ,connection-state-lock))
       (read-lock-begin ,rwlock%)
       (unwind-protect
            (progn ,@body)
         (read-lock-end ,rwlock%)))))

(defun write-lock-begin (connection-state-lock)
  (declare (optimize (speed 3) (debug 0) (safety 0)))
  (bt:acquire-lock (connection-state-lock-rtrylock connection-state-lock))
  (bt:acquire-lock (connection-state-lock-resource connection-state-lock)))

(defun write-lock-end (connection-state-lock)
  (declare (optimize (speed 3) (debug 0) (safety 0)))
  (bt:release-lock (connection-state-lock-resource connection-state-lock))
  (bt:release-lock (connection-state-lock-rtrylock connection-state-lock)))

(defmacro with-write-lock (connection-state-lock &body body)
  (with-gensyms (rwlock%)
    `(let ((,rwlock% ,connection-state-lock))
       (write-lock-begin ,rwlock%)
       (unwind-protect
            (progn ,@body)
         (write-lock-end ,rwlock%)))))

(defclass threaded-connection (connection)
  ((event-base :reader connection-event-base :initarg :event-base)
   (control-fd)
   (control-mailbox :reader connection-control-mailbox)
   (state-lock :initform (make-connection-state-lock) :reader connection-state-lock)
   (execute-in-connection-lambda :initform nil :reader connection-lambda)
   (connection-thread :reader connection-thread)))

(defmethod connection-open-p% ((connection threaded-connection))
  (and connection
       (slot-boundp connection 'connection-thread)
       (bt:thread-alive-p (connection-thread connection))
       (eq (connection-state connection) :open)))

(defun setup-execute-in-connection-lambda (connection)
  (with-slots (control-fd control-mailbox execute-in-connection-lambda) connection
    (setf execute-in-connection-lambda
          (lambda (thunk)
            (with-read-lock (connection-state-lock connection)
              (if (connection-open-p connection)
                  (progn (safe-queue:enqueue thunk control-mailbox)
                         (log:debug "Notifying connection thread")
                         (eventfd.notify-1 control-fd))
                  (error 'connection-closed-error :connection connection)))))))

(defmacro execute-in-connection-thread ((&optional (connection '*connection*)) &body body)
  `(funcall (connection-lambda ,connection)
            (lambda () ,@body)))

(defmacro execute-in-connection-thread-sync ((&optional (connection '*connection*)) &body body)
  (with-gensyms (lock condition return connection% error)
    `(let ((,lock (bt:make-recursive-lock))
           (,condition (bt:make-condition-variable))
           (,return nil)
           (,connection% ,connection)
           (,error))
       (if (connection-open-p ,connection%)
           (bt:with-recursive-lock-held (,lock)
             (funcall (connection-lambda ,connection%)
                      (lambda (&aux (*connection* ,connection%))
                        (bt:with-recursive-lock-held (,lock)
                          (handler-case
                              (setf ,return
                                    (multiple-value-list
                                     (unwind-protect
                                          (progn
                                            ,@body)
                                       (bt:condition-notify ,condition))))
                            (cl-rabbit::rabbitmq-server-error (e)
                              (log:error "Server error: ~a" e)
                              (setf ,error e))))))
             (bt:condition-wait ,condition ,lock)
             (if ,error
                 (error ,error)
                 (values-list ,return)))
           (error 'connection-closed-error :connection ,connection%)))))

(defmethod connection.close% ((connection threaded-connection) timeout)
  (if (eq (bt:current-thread) (connection-thread connection))
      (throw 'stop-connection (values))
      (if (bt:thread-alive-p (connection-thread connection))
          (handler-case
              (progn (execute-in-connection-thread (connection)
                       (connection.close :connection connection))
                     (handler-case
                         (sb-thread:join-thread (connection-thread connection) :timeout timeout)
                       (sb-thread:join-thread-error (e)
                         (case (sb-thread::join-thread-problem e)
                           (:timeout (log:error "Connection thread stalled?")
                            (sb-thread:terminate-thread (connection-thread connection)))
                           (:abort (log:error "Connection thread aborted"))
                           (t (log:error "Connection state is unknown"))))))
            (connection-closed-error () (log:debug "Closing already closed connection")))
          t)))
