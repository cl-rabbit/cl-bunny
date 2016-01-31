(in-package :cl-bunny)

(defclass iolib-connection (connection)
  ((socket :accessor connection-socket)
   (read-buffer :initform (nibbles:make-octet-vector 4096))
   (rb-index :initform nil)
   (rb-end :initform nil)
   (channel-max :reader connection-channel-max% :initform +channel-max+)
   (frame-max :reader connection-frame-max% :initform +frame-max+)
   (heartbeat :reader connection-heartbeat% :initform +heartbeat-interval+)))

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
  (setf (slot-value connection 'control-fd) (eventfd:eventfd.new :semaphore t)
        (slot-value connection 'control-mailbox) (safe-queue:make-queue)))

(defmethod connection-loop ((connection threaded-iolib-connection))
  (with-slots (control-fd control-mailbox event-base socket) connection
    (let ((of-queue (make-output-frame-queue))
          (fap-parser (make-fap-parser)))
      (flet ((enqueue-frame (frame)              
               (lparallel.raw-queue:push-raw-queue frame of-queue)
               (unless (eq (output-frame-queue-state of-queue) :sending)
                 (iolib:set-io-handler (connection-event-base connection)
                                       (iolib:socket-os-fd (connection-socket connection))
                                       :write
                                       (lambda (fd e ex)
                                         (send-queued-frames connection of-queue)))
                 (setf (output-frame-queue-state of-queue) :sending)))
             (read-frame ()
               (let ((read (iolib:receive-from (iolib:socket-os-fd socket)
                                               :buffer (fap-parser-buffer fap-parser)
                                               :start (fap-parser-buffer-index fap-parser)))))
               (fap-parser-advance fap-parser read)))
        (iolib:with-event-base (event-base)

          (bb:chain (connection.open-async connection)
            (:then ()
                   (promise.resolve (connection-open-promise connection)))
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
                                                     (frame (enqueue-frame thing))
                                                     (function (funcall thing))))))
                   (iolib:set-io-handler event-base
                                         socket
                                         :read (lambda (fd e ex)                                                      
                                                 (declare (ignorable fd e ex))
                                                 (log:debug "Got something to read on connection thread")
                                                 (if-let ((frame (read-frame)))
                                                   (if-let ((channel (get-channel connection (frame-channel frame))))
                                                     (channel.receive-frame channel frame)
                                                     (log:warn "Message received for closed channel: ~a" (frame-channel frame))))))))

          (iolib:event-dispatch))))))
