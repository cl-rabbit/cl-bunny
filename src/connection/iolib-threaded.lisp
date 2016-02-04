(in-package :cl-bunny)

(amqp:enable-binary-string-syntax)

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
        (slot-value connection 'control-mailbox) (safe-queue:make-queue)
        (slot-value connection 'channel-id-allocator) (new-channel-id-allocator (connection-channel-max connection))
        (slot-value connection 'method-assembler) (make-instance 'method-assembler :max-body-size (- +frame-max+ 9))))

(defmethod close-connection-with-error ((connection iolib-connection) error)
  (log:error "Connection error: ~a" error)
  (cl-events:event! (connection-on-error% connection))
  (throw 'stop-connection (values)))

(defmethod connection-loop ((connection threaded-iolib-connection) promise)
  (with-slots (control-fd control-mailbox event-base socket spec) connection
    (let ((of-queue (make-output-frame-queue))
          (fap-parser (make-fap-parser))
          (heartbeat-frame (make-instance 'heartbeat-frame)))
      (labels ((enqueue-frame (frame)
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
                   (when (= 0 read)
                     (error "EOF"))
                   (fap-parser-advance-end fap-parser read)
                   (collectors:with-appender-output (add-frame)
                     (loop
                       :as frame = (fap-parser-advance fap-parser)
                       :if frame do
                          (add-frame (prog1 frame
                                       (log:debug "received frame ~a" frame)
                                       (ignore-errors (log:debug "frame payload ~a" (amqp::frame-payload frame)))))
                       :else :do
                          (log:debug "no frame, exiting loop")
                          (return)))))
               (connection.send% (method)
                 (dolist (frame (method-to-frames method 0 (connection-frame-max% connection)))
                   (enqueue-frame frame)))
               (install-main-reader-and-heartbeat-timer ()
                 (iolib:set-io-handler event-base
                                       control-fd
                                       :read (lambda (fd e ex)
                                               (declare (ignorable fd e ex))
                                               (eventfd.read control-fd)
                                               ;; eventfd is in semaphore mode
                                               ;; dequeue only once
                                               (when-let ((thing (safe-queue:dequeue control-mailbox)))
                                                 (log:debug thing)
                                                 (etypecase thing
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
                                                     unless (when (typep frame 'heartbeat-frame)
                                                              (log:debug "received heartbeat from server")
                                                              t)
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
                                           (receive-from connection :start 0 :Buffer #b"\x0\x0\x0x\0")
                                           (log:error "Missed heartbeat from server")
                                           (throw 'stop-connection 'transport-error))
                                          ((>= now (+ (connection-last-client-activity connection) (connection-heartbeat% connection)))
                                           (log:debug "Sending HEARTBEAT")
                                           (enqueue-frame heartbeat-frame)))))
                                    (max (1- (/ (connection-heartbeat% connection) 2)) 0.4)))))


        (setf (slot-value connection 'state) :opening)
        (setf socket (iolib:make-socket))
        (setf (iolib.sockets:socket-option socket :tcp-nodelay) t)
        (iolib:connect (connection-socket connection) (iolib:lookup-hostname (connection-spec-host spec)) :port (connection-spec-port spec) :wait t)
        (write-sequence #b"AMQP\x0\x0\x9\x1" socket)
        (force-output socket)
        (setf (slot-value connection 'state) :waiting-for-connection-start)

        (catch 'stop-connection
          (handler-bind ((error
                           (lambda (e)
                             (log:error "Unhandled unknown error: ~a" e)
                             (trivial-backtrace:print-backtrace e)
                             (unless *debug-connection*
                               (throw 'stop-connection e)))))
            (iolib:with-event-base (eb)
              (setf event-base eb)
              (let ((*event-base* eb))

                (iolib:set-io-handler event-base
                                      (iolib:socket-os-fd socket)
                                      :read (lambda (fd e ex)
                                              (declare (ignorable fd e ex))
                                              (log:trace e)
                                              (log:trace ex)
                                              (log:debug "Got something to read on connection thread")
                                              (loop for frame in (read-frames) do
                                                       (assert (= (frame-channel frame) 0))
                                                       (unless (typep frame 'heartbeat-frame)
                                                         (when-let ((method (consume-frame (channel-method-assembler connection) frame)))
                                                           (log:debug "While opening connection consumed method: ~a" method)
                                                           (ecase (slot-value connection 'state)
                                                             (:waiting-for-connection-start
                                                              (assert (typep method 'amqp-method-connection-start))
                                                              (connection.send% (make-instance 'amqp-method-connection-start-ok :response " guest guest" :client-properties `(("product" . "CL-BUNNY")
                                                                                                                                                                                ("information" . "http://cl-rabbit.io/cl-bunny/")
                                                                                                                                                                                ("platform" . ,(format nil "Runtime: ~A (~A), OS: ~A"
                                                                                                                                                                                                       (lisp-implementation-type)
                                                                                                                                                                                                       (lisp-implementation-version)
                                                                                                                                                                                                       (software-type)))
                                                                                                                                                                                ("version" . ,(asdf:component-version (asdf:find-system :cl-bunny)))
                                                                                                                                                                                ("capabilities" . (("connection.blocked" . nil)
                                                                                                                                                                                                   ("publisher_confirms" . t)
                                                                                                                                                                                                   ("consumer_cancel_notify" . t)
                                                                                                                                                                                                   ("exchange_exchange_bindings" . t)
                                                                                                                                                                                                   ("basic.nack" . t)
                                                                                                                                                                                                   ("authentication_failure_close" . t))))))
                                                              (setf (slot-value connection 'state) :waiting-for-connection-tune))
                                                             (:waiting-for-connection-tune
                                                              (assert (typep method 'amqp-method-connection-tune))
                                                              (connection.send% (make-instance 'amqp-method-connection-tune-ok :heartbeat (setf (connection-heartbeat% connection)
                                                                                                                                                (if (zerop (amqp-method-field-heartbeat method))
                                                                                                                                                    (connection-spec-heartbeat-interval spec)
                                                                                                                                                    (min (amqp-method-field-heartbeat method)
                                                                                                                                                         (connection-spec-heartbeat-interval spec))))
                                                                                                                               :frame-max (setf (connection-frame-max% connection)
                                                                                                                                                (if (zerop (amqp-method-field-frame-max method))
                                                                                                                                                    (connection-spec-frame-max spec)
                                                                                                                                                    (min (amqp-method-field-frame-max method)
                                                                                                                                                         (connection-spec-frame-max spec))))
                                                                                                                               :channel-max (setf (connection-channel-max% connection)
                                                                                                                                                  (if (zerop (amqp-method-field-channel-max method))
                                                                                                                                                      (connection-spec-channel-max spec)
                                                                                                                                                      (min (amqp-method-field-channel-max method)
                                                                                                                                                           (connection-spec-channel-max spec))))))
                                                              (connection.send%  (make-instance 'amqp-method-connection-open))
                                                              (setf (slot-value connection 'state) :waiting-for-connection-open-ok))
                                                             (:waiting-for-connection-open-ok
                                                              (assert (typep method 'amqp-method-connection-open-ok))
                                                              (setf (slot-value connection 'state) :open)
                                                              (iolib:remove-fd-handlers event-base (iolib:socket-os-fd socket) :read t)
                                                              (install-main-reader-and-heartbeat-timer)
                                                              (promise.resolve promise))))))))

                (iolib:event-dispatch event-base)))))
        (bt:with-lock-held ((connection-state-lock connection))
          (setf (slot-value connection 'state) :closing)
          (event! (connection-on-close% connection) connection)
          (eventfd.close control-fd)
          (log:debug "Stopping AMQP connection")
          (when (connection-pool connection)
            (connections-pool.remove connection))
          (maphash (lambda (id channel)
                     (declare (ignorable id))
                     (setf (channel-state channel) :closed)
                     (safe-queue:mailbox-send-message (channel-mailbox channel)
                                                      (make-instance 'amqp-method-connection-close)))
                   (connection-channels connection))
          (log:debug "closed-all-channels")
          ;; ;; drain control mailbox
          ;; (loop for lambda = (safe-queue:dequeue control-mailbox)
          ;;       while lambda
          ;;     do (ignore-errors (funcall lambda)))
          ;; (log:debug "queue drained")
          (setf (slot-value connection 'state) :closed))))))

(defmethod connection-open-p% ((connection threaded-iolib-connection))
  (and connection
       ;;;(bt:thread-alive-p (connection-thread connection))
       (eq (channel-state connection) :open)))

(defmethod connection.send ((connection iolib-connection) channel method)
  (dolist (frame (method-to-frames method (channel-id channel) (connection-frame-max% connection)))
    (send-to-connection-thread (connection)
      frame)))


(defmethod channel.receive ((connection threaded-iolib-connection) (method amqp-method-connection-close-ok))
  (call-next-method)
  (throw 'stop-connection (values)))

(defmethod channel.receive ((connection threaded-iolib-connection) (method amqp-method-connection-close))
  ;; what to do with enqueued frames?
  ;; probably of-queue must be cleaned from all non-connection frames
  ;; then close-ok should be enqueued
  ;; while cleaning up do not forget to reject pending 'waiting-for-sync-reply' promises of respective channels
  (connection.send connection connection (make-instance 'amqp-method-connection-close-ok))
  ;; this should be executed only after close-ok is fully sent
  (send-to-connection-thread (connection)
    (throw 'stop-connection (values method))))

(defmethod connection.close% ((connection threaded-iolib-connection) timeout)
  (channel.send connection (make-instance 'amqp-method-connection-close :class-id 0 :method-id 0 :reply-code +amqp-reply-success+))
  (handler-case
      (sb-thread:join-thread (connection-thread connection) :timeout timeout)
    (sb-thread:join-thread-error (e)
      (case (sb-thread::join-thread-problem e)
        (:timeout (log:error "Connection thread stalled?")
         (sb-thread:terminate-thread (connection-thread connection)))
        (:abort (log:error "Connection thread aborted"))
        (t (log:error "Connection state is unknown"))))))
