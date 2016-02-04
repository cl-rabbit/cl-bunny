(in-package :cl-bunny)

(defclass iolib-connection (connection iolib-transport)
  ((read-buffer :initform (nibbles:make-octet-vector 4096))
   (rb-index :initform nil)
   (rb-end :initform nil)
   (channel-max :reader connection-channel-max% :initform +channel-max+)
   (frame-max :reader connection-frame-max% :initform +frame-max+)
   (last-client-activity :accessor connection-last-client-activity) ;; TODO: monotonic time?
   (last-server-activity :accessor connection-last-server-activity) ;; TODO: monotonic time?
   (heartbeat :reader connection-heartbeat% :initform 5)))
