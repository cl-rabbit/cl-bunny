(in-package :cl-bunny)

(defclass iolib-transport ()
  ((socket :accessor connection-socket)))

(defgeneric receive-from (connection &key buffer start))
(defmethod receive-from ((connection iolib-transport) &key buffer start)
  (iolib:receive-from (connection-socket connection) :buffer buffer :start start))

(defgeneric send-to (connection buffer &key start end))
(defmethod send-to ((connection iolib-transport) buffer &key start end)
  (iolib:send-to (connection-socket connection) buffer :start start :end end))
