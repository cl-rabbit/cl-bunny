(in-package :cl-bunny)

(defclass iolib-transport ()
  ((socket :accessor connection-socket)
   (writer :initform nil)
   (reader :initform nil)
   (want-write :initform nil)
   (want-read :initform nil)))

(defmacro with-connection-close-on-socket-error (connection &body body)
  `(handler-case
       (progn ,@body)
     (iolib.sockets:socket-error (e) ;; should we have meaningful descriptions for each socket type
       (close-connection-with-error ,connection e))))

(defgeneric receive-from (connection buffer callback &key start))
(defmethod receive-from ((connection iolib-transport) buffer callback &key start)
  (with-connection-close-on-socket-error connection
    (handler-case
        (multiple-value-bind (_octets read) (iolib:receive-from (connection-socket connection) :buffer buffer :start start)
          (declare (ignore _octets))
          (unless (= 0 read)
            (setf (connection-last-server-activity connection) (get-universal-time)))
          (funcall callback read))
      (iolib.sockets::ssl-error-want-read ()
        (setf (slot-value connection 'want-read)
              (lambda ()
                (setf (slot-value connection 'want-read) nil)
                (receive-from connection buffer callback :start start))))
      (iolib.sockets::ssl-error-want-write ()
        (setf (slot-value connection 'want-write)
              (lambda ()
                (setf (slot-value connection 'want-write) nil)
                (receive-from connection buffer callback :start start)))
        (unless (slot-value connection 'writer) ;; here :write should be scheduled
          (transport.set-writer connection nil))))))

(defgeneric send-to (connection buffer callback &key start end))
(defmethod send-to ((connection iolib-transport) buffer callback  &key start end)
  (with-connection-close-on-socket-error connection
    (handler-case
        (let ((sent
                (iolib:send-to (connection-socket connection) buffer :start start :end end)))
          (setf (connection-last-client-activity connection) (get-universal-time))
          (funcall callback sent))
      (iolib.sockets::ssl-error-want-read ()
        (setf (slot-value connection 'want-read)
              (lambda ()
                (setf (slot-value connection 'want-read) nil)
                (send-to connection buffer callback :start start :end end))))
      (iolib.sockets::ssl-error-want-write ()
        (setf (slot-value connection 'want-write)
              (lambda ()
                (setf (slot-value connection 'want-write) nil)
                (send-to connection buffer callback :start start :end end)))))))

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

(defmethod transport.set-writer ((connection iolib-transport) writer)
  (if (slot-value connection 'writer)
      (error "Transport writer already set"))
  (setf (slot-value connection 'writer) writer)
  (unless (slot-value connection 'want-write)
    (iolib:set-io-handler (connection-event-base connection)
                          (iolib:socket-os-fd (connection-socket connection))
                          :write
                          (lambda (fd e ex)
                            (declare (ignore fd e ex))
                            (if-let ((want-write (slot-value connection 'want-write)))
                              (progn (setf (slot-value connection 'want-write) nil)
                                     (funcall want-write)
                                     (unless writer
                                       (transport.remove-writer connection)))
                              (funcall (slot-value connection 'writer)))))))

(defmethod transport.set-reader ((connection iolib-transport) reader)
  (if (slot-value connection 'reader)
      (error "Transport reader already set"))
  (setf (slot-value connection 'reader) reader)
  (iolib:set-io-handler (connection-event-base connection)
                        (iolib:socket-os-fd (connection-socket connection))
                        :read
                        (lambda (fd e ex)
                          (declare (ignore fd e ex))
                          (if-let ((want-read (slot-value connection 'want-read)))
                            (progn (setf (slot-value connection 'want-read) nil)
                                   (funcall want-read))
                            (funcall (slot-value connection 'reader))))))

(defmethod transport.remove-writer ((connection iolib-transport))
  (iolib:remove-fd-handlers (connection-event-base connection)
                            (iolib:socket-os-fd (connection-socket connection))
                            :write t)
  (setf (slot-value connection 'writer) nil))


(defmethod transport.remove-reader ((connection iolib-transport))
  (iolib:remove-fd-handlers (connection-event-base connection)
                            (iolib:socket-os-fd (connection-socket connection))
                            :read t)
  (setf (slot-value connection 'reader) nil))
