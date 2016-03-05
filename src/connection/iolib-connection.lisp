(in-package :cl-bunny)

(defclass iolib-connection (connection iolib-transport)
  ((read-buffer :initform (nibbles:make-octet-vector 4096))
   (rb-index :initform nil)
   (rb-end :initform nil)
   (last-client-activity :accessor connection-last-client-activity) ;; TODO: monotonic time?
   (last-server-activity :accessor connection-last-server-activity) ;; TODO: monotonic time?))
   (of-queue :initform (make-output-frame-queue))
   (fap-parser :initform (make-fap-parser))

   ;;events
   (on-blocked :initform (make-instance 'bunny-event)
               :initarg :on-close
               :accessor connection-on-blocked%)
   (on-unblocked :initform (make-instance 'bunny-event)
                 :initarg :on-close
                 :accessor connection-on-unblocked%)))



(defun connection-on-blocked (&optional (connection *connection*))
  (connection-on-blocked% connection))

(defun connection-on-unblocked (&optional (connection *connection*))
  (connection-on-unblocked% connection))
