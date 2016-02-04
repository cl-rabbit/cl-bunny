(in-package :cl-bunny)

(defclass iolib-connection (connection iolib-transport)
  ((read-buffer :initform (nibbles:make-octet-vector 4096))
   (rb-index :initform nil)
   (rb-end :initform nil)
   (last-client-activity :accessor connection-last-client-activity) ;; TODO: monotonic time?
   (last-server-activity :accessor connection-last-server-activity) ;; TODO: monotonic time?))
   ))
