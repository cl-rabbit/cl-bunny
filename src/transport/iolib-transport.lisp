(in-package :cl-bunny)

(defclass iolib-transport ()
  ((socket :accessor connection-socket)))

(defvar *event-base*)

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

(defun socket-writeable (socket)
  (bb:with-promise (resolve reject)
    (iolib:set-io-handler *event-base* (iolib:socket-os-fd socket) :write
                          (lambda (fd e ex)
                            (iolib:remove-fd-handlers *event-base* (iolib:socket-os-fd socket) :write t)
                            (resolve)))))

(defun socket-readable (socket)
  (bb:with-promise (resolve reject)
    (iolib:set-io-handler *event-base* (iolib:socket-os-fd socket) :read
                          (lambda (fd e ex)
                            (iolib:remove-fd-handlers *event-base* (iolib:socket-os-fd socket) :read t)
                            (resolve)))))

(defun socket.check (socket)
  (let ((errcode (iolib:socket-option socket :error)))
    (unless (zerop errcode)
      (error "Unable to connect"))))

(defmethod transport.open ((connection iolib-transport) address port)
  (setf (connection-socket connection) (iolib:make-socket))
  (iolib:connect (connection-socket connection) address :port port :wait nil)
  (bb:wait (socket-writeable (connection-socket connection))
    (socket.check (connection-socket connection))))
