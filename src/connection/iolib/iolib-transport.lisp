(in-package :cl-bunny)

(defclass iolib-transport ()
  ((socket :accessor connection-socket)))

(defmacro with-connection-close-on-socket-error (connection &body body)
  `(handler-case
       (progn ,@body)
     (iolib.sockets:socket-error (e) ;; should we have meaningful descriptions for each socket type
       (close-connection-with-error ,connection e))))

(defgeneric receive-from (connection &key buffer start))
(defmethod receive-from ((connection iolib-transport) &key buffer start)
  (with-connection-close-on-socket-error connection
    (iolib:receive-from (connection-socket connection) :buffer buffer :start start)))

(defgeneric send-to (connection buffer &key start end))
(defmethod send-to ((connection iolib-transport) buffer &key start end)
  (with-connection-close-on-socket-error connection
    (iolib:send-to (connection-socket connection) buffer :start start :end end)))
