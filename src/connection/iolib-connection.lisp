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

(defmethod send-to :after ((connection iolib-connection) buffer &key start end)
  (setf (connection-last-client-activity connection) (get-universal-time)))

(defmethod receive-from :around ((connection iolib-connection) &key buffer start)
  (multiple-value-bind (buffer read) (call-next-method)
    (unless (= 0 read)
      (setf (connection-last-server-activity connection) (get-universal-time)))
    (values buffer read)))

(defmethod connection.new% ((type (eql 'iolib-connection)) spec pool-tag)
  (let ((connection (make-instance 'iolib-connection :spec spec
                                                     :pool-tag pool-tag)))
    connection))

(defclass threaded-iolib-connection (iolib-connection threaded-connection)
  ((connection-open-promise :initarg :open-promise :reader connection-open-promise)))

(defmethod connection.new% ((type (eql 'threaded-iolib-connection)) spec pool-tag)
  (let ((connection (make-instance 'threaded-iolib-connection :spec spec
                                                              :pool-tag pool-tag)))
    (setup-execute-in-connection-lambda connection)
    connection))

(defmethod connection.init ((connection threaded-iolib-connection))
  (setf (slot-value connection 'control-fd) (eventfd:eventfd.new 0 :semaphore t)
        (slot-value connection 'control-mailbox) (safe-queue:make-queue)))

(defun channel.receive-frame (channel frame)
  (print frame)
  (break))

(defmethod connection-loop ((connection threaded-iolib-connection))
  (with-slots (control-fd control-mailbox event-base socket) connection
    (let ((of-queue (make-output-frame-queue))
          (fap-parser (make-fap-parser))
          (heartbeat-frame (make-instance 'heartbeat-frame)))
      (flet ((enqueue-frame (frame)
               (output-frame-queue-push of-queue frame)
               (unless (eq (output-frame-queue-state of-queue) :sending)
                 (log:debug "Setting :write io handler")
                 (iolib:set-io-handler (connection-event-base connection)
                                       (iolib:socket-os-fd (connection-socket connection))
                                       :write
                                       (lambda (fd e ex)
                                         (declare (ignorable fd e ex))
                                         (send-queued-frames connection of-queue)))
                 (setf (output-frame-queue-state of-queue) :sending)))
             (read-frames ()
               (multiple-value-bind (_octets read)
                   (receive-from connection
                                 :buffer (fap-parser-buffer fap-parser)
                                 :start (fap-parser-end-index fap-parser))
                 (declare (ignore _octets))
                 (log:debug "Read: ~a" read)
                 (unless (= 0 read)
                   (fap-parser-advance-end fap-parser read)
                   (loop
                     :as frame = (fap-parser-advance fap-parser)
                     :if frame
                     :collect (prog1 frame
                                (log:debug frame))
                     :else :do
                        (log:debug "no frame, exiting loop")
                        (return))))))
        (iolib:with-event-base (eb)
          (setf event-base eb)
          (let ((*event-base* eb))
            (bb:chain (connection.open-async connection)
              (:then ()
                     ;; naively reset activity timestamps
                     ;; heartbeat tracking should be started in connection.open-async really
                     (setf (connection-last-server-activity connection) (get-universal-time)
                           (connection-last-client-activity connection) (get-universal-time))
                     (setf (slot-value connection 'state) :open)
              ;;       (promise.resolve (connection-open-promise connection))
                     )
              (:then ()
                     (iolib:set-io-handler event-base
                                           control-fd
                                           :read (lambda (fd e ex)
                                                   (declare (ignorable fd e ex))
                                                   (eventfd.read control-fd)
                                                   (log:debug "Got lambda to execute on connection thread")
                                                   ;; eventfd is in semaphore mode
                                                   ;; dequeue only once
                                                   (if-let ((thing (safe-queue:dequeue control-mailbox)))
                                                     (typecase thing
                                                       (amqp::frame (enqueue-frame thing))
                                                       (function (funcall thing))
                                                       (symbol (iolib:exit-event-loop event-base))))))
                     (iolib:set-io-handler event-base
                                           (iolib:socket-os-fd socket)
                                           :read (lambda (fd e ex)
                                                   (declare (ignorable fd e ex))
                                                   (log:debug "Got something to read on connection thread")
                                                   (loop for frame in (read-frames)
                                                         as channel = (get-channel connection (frame-channel frame))
                                                         if channel do
                                                            (channel.receive-frame channel frame)
                                                         else do
                                                            (log:warn "Message received for closed channel: ~a" (frame-channel frame)))))
                     (unless (= 0 (connection-heartbeat% connection))
                       (iolib:add-timer event-base
                                        (lambda ()
                                          (let ((now (get-universal-time)))  ;; TODO: monotonic time?
                                            (cond
                                              ((> (/ (- now (connection-last-server-activity connection)) (connection-heartbeat% connection))
                                                  2)
                                               (throw 'stop-connection 'transport-error))
                                              ((> now (+ (connection-last-client-activity connection) (connection-heartbeat% connection)))
                                               (log:debug "Sending HEARTBEAT")
                                               ;; btw send_frame_inner should update send_heartbeat deadline
                                               (enqueue-frame heartbeat-frame)))))
                                        (+ 0.4 (/ (connection-heartbeat% connection) 2)))))
              (:catch (e)
                (error e)))

            (iolib:event-dispatch event-base)))))))

(defmethod connection-open-p% ((connection threaded-iolib-connection))
  (and connection
       ;;;(bt:thread-alive-p (connection-thread connection))
       (eq (connection-state connection) :open)))

(defmethod connection.send ((connection iolib-connection) channel method)
  (loop for frame in (method-to-frames method (channel-id channel) (connection-frame-max% connection)) do
           (send-to-connection-thread (connection)
             frame)))
