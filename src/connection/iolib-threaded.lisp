(in-package :cl-bunny)

(amqp:enable-binary-string-syntax)
(cl-interpol:enable-interpol-syntax)

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

(defmethod close-connection-with-error ((connection threaded-iolib-connection) error)
  (log:error "Connection error: ~a" error)
  (cl-events:event! (connection-on-error% connection))
  (with-slots (on-connect-promise) connection
    (when (and on-connect-promise (not (promise-finished-p on-connect-promise)))
      (promise.reject on-connect-promise error)))
  (throw 'stop-connection (values)))

(defun open-socket-and-say-hello (connection)
  (with-slots (on-connect-promise socket spec) connection
    (handler-bind ((error
                     (lambda (e)
                       (log:error "Unhandled unknown error: ~a" e)
                       (trivial-backtrace:print-backtrace e)
                       (promise.reject on-connect-promise e)
                       (throw 'stop-connection nil))))
      (setf (slot-value connection 'state) :opening)
      (setf socket (if (connection-spec-use-tls-p spec)
                       (make-instance 'iolib.sockets::ssl-socket :protocol :default)
                       (iolib:make-socket)))
      (setf (iolib.sockets:socket-option socket :tcp-nodelay) t)
      (iolib:connect (connection-socket connection) (iolib:lookup-hostname (connection-spec-host spec)) :port (connection-spec-port spec) :wait t)
      (send-to connection #b"AMQP\x0\x0\x9\x1" :start 0)
      (setf (slot-value connection 'state) :waiting-for-connection-start))))

(defun enqueue-frame (connection frame)
  (with-slots (of-queue) connection
    (output-frame-queue-push of-queue frame)
    (unless (eq (output-frame-queue-state of-queue) :sending)
      (log:debug "Setting :write io handler")
      (transport.set-writer connection (lambda () (send-queued-frames connection of-queue)))
      (setf (output-frame-queue-state of-queue) :sending))))

(defun read-frames (connection callback)
  (with-slots (fap-parser) connection
    (receive-from connection (fap-parser-buffer fap-parser)  (lambda (read)
                               (log:debug "Read: ~a" read)
                               (when (= 0 read)
                                 (close-connection-with-error connection "Unexpected eof")) ;; TODO: specialize error
                               (fap-parser-advance-end fap-parser read)
                               (funcall callback
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
                  :start (fap-parser-end-index fap-parser))))

(defun connection.send% (connection method)
  (dolist (frame (method-to-frames method 0 (connection-frame-max% connection)))
    (enqueue-frame connection frame)))

(defun validate-server-protocol-version (connection-start)
  (and (= (amqp-method-field-version-major connection-start) 0)
       (= (amqp-method-field-version-minor connection-start) 9)))

(defun validate-server-auth-mechanisms (connection-start)
  (let ((mechanisms (split-sequence:split-sequence #\Space (amqp-method-field-mechanisms connection-start))))
    (find "PLAIN" mechanisms :test #'equal)))

(defun maybe-install-heartbeat-timer (connection)
  (with-slots (heartbeat-frame event-base) connection
    (unless (= 0 (connection-heartbeat% connection))
      (iolib:add-timer event-base
                       (lambda ()
                         (let ((now (get-universal-time))) ;; TODO: monotonic time?
                           (cond
                             ((> (/ (- now (connection-last-server-activity connection)) (connection-heartbeat% connection))
                                 2)
                              (log:error "Missed heartbeat from server")
                              (throw 'stop-connection 'transport-error))
                             ((>= now (+ (connection-last-client-activity connection) (connection-heartbeat% connection)))
                              (log:debug "Sending HEARTBEAT")
                              (enqueue-frame connection heartbeat-frame)))))
                       (max (1- (/ (connection-heartbeat% connection) 2)) 0.4)))))

(defun perform-handshake (connection callback)
  (with-slots (on-connect-promise event-base socket spec) connection
    (transport.set-reader connection
     (lambda ()
       (read-frames connection
                    (lambda (frames)
                      (loop for frame in frames do
                               (assert (= (frame-channel frame) 0))
                               (unless (typep frame 'heartbeat-frame)
                                 (when-let ((method (consume-frame (channel-method-assembler connection) frame)))
                                   (log:debug "While opening connection consumed method: ~a" method)
                                   (ecase (slot-value connection 'state)
                                     (:waiting-for-connection-start
                                      (assert (typep method 'amqp-method-connection-start))
                                      (unless (validate-server-protocol-version method)
                                        (promise.reject on-connect-promise "Unsupported server protocol version")
                                        (log:error "Unsupported server protocol version"))
                                                 (unless (validate-server-auth-mechanisms method)
                                                   (promise.reject on-connect-promise "Unsupported server auth mechanisms")
                                                   (log:error "Unsupported server auth mechanisms: ~a" (amqp-method-field-mechanisms method)))
                                      (connection.send% connection (make-instance 'amqp-method-connection-start-ok
                                                                                  :response #?"\x0${(connection-spec-login spec)}\x0${(connection-spec-password spec)}"
                                                                                             :client-properties `(("product" . "CL-BUNNY")
                                                                                                                  ("information" . "http://cl-rabbit.io/cl-bunny/")
                                                                                                                  ("platform" . ,(format nil "Runtime: ~A (~A), OS: ~A"
                                                                                                                                         (lisp-implementation-type)
                                                                                                                                         (lisp-implementation-version)
                                                                                                                                         (software-type)))
                                                                                                                  ("version" . ,(asdf:component-version (asdf:find-system :cl-bunny)))
                                                                                                                  ("capabilities" . (("connection.blocked" . t)
                                                                                                                                     ("publisher_confirms" . t)
                                                                                                                                     ("consumer_cancel_notify" . t)
                                                                                                                                     ("exchange_exchange_bindings" . t)
                                                                                                                                     ("basic.nack" . t)
                                                                                                                                     ("authentication_failure_close" . t))))))
                                                 (setf (slot-value connection 'state) :waiting-for-connection-tune))
                                                (:waiting-for-connection-tune
                                                 (when (typep method 'amqp-method-connection-close)
                                                   (promise.reject on-connect-promise (close-method-to-error connection method))
                                                   (throw 'stop-connection nil))
                                                 (assert (typep method 'amqp-method-connection-tune))
                                                 (connection.send% connection (make-instance 'amqp-method-connection-tune-ok
                                                                                             :heartbeat (setf (connection-heartbeat% connection)
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
                                                 (connection.send% connection (make-instance 'amqp-method-connection-open
                                                                                             :virtual-host (connection-spec-vhost spec)))
                                                 (setf (slot-value connection 'state) :waiting-for-connection-open-ok)
                                                 (maybe-install-heartbeat-timer connection))
                                                (:waiting-for-connection-open-ok
                                                 (when (typep method 'amqp-method-connection-close)
                                                   (promise.reject on-connect-promise (close-method-to-error connection method))
                                                   (throw 'stop-connection nil))
                                                 (assert (typep method 'amqp-method-connection-open-ok))
                                                 (setf (slot-value connection 'state) :open)
                                                 (transport.remove-reader connection)
                                                 (funcall callback)
                                                 (promise.resolve on-connect-promise))))))))))))

(defun install-main-readers (connection)
  (with-slots (control-fd control-mailbox event-base socket) connection
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
                                      (amqp::frame (enqueue-frame connection thing))
                                      (function (funcall thing))
                                      (symbol (iolib:exit-event-loop event-base))))))
    (transport.set-reader connection
                          (lambda ()
                            (log:debug "Got something to read on connection thread")
                            (read-frames connection
                                         (lambda (frames)
                                           (loop for frame in frames
                                                 as channel = (get-channel connection (frame-channel frame))
                                                 unless (when (typep frame 'heartbeat-frame)
                                                          (log:debug "received heartbeat from server")
                                                          t)
                                                 if channel do
                                                    (channel.receive-frame channel frame)
                                                 else do
                                                    (log:warn "Message received for closed channel: ~a" (frame-channel frame)))))))))

(defun shutdown-connection (connection)
  (bt:with-recursive-lock-held ((connection-state-lock connection))
    (with-slots (control-fd control-mailbox socket) connection
      (when socket
        (ignore-errors (close socket)))
      (let ((old-state (slot-value connection 'state)))
        (setf (slot-value connection 'state) :closing)
        (when (eql old-state :open)
          (event! (connection-on-close% connection) connection)))
      (with-slots (on-connect-promise) connection
        (when (and on-connect-promise (not (promise-finished-p on-connect-promise)))
          (promise.reject on-connect-promise (make-condition 'connection-closed-error))))
      (eventfd.close control-fd)
      (log:debug "Stopping AMQP connection")
      (when (connection-pool connection)
        (connections-pool.remove connection))
      (maphash (lambda (id channel)
                 (declare (ignorable id))
                 (setf (channel-state channel) :closed)
                 (safe-queue:mailbox-send-message (channel-mailbox channel)
                                                  (make-instance 'amqp-method-connection-close))
                 (deregister-channel connection channel)
                 (destructuring-bind (&optional rm callback) (channel-expected-reply channel)
                   (when rm
                     (setf (channel-expected-reply channel) nil)
                     (funcall callback (make-condition 'connection-closed-error) t))))
               (connection-channels connection))
      (destructuring-bind (&optional rm callback) (channel-expected-reply connection)
        (when rm
          (setf (channel-expected-reply connection) nil)
          (funcall callback (make-condition 'connection-closed-error) t)))
      (log:debug "closed-all-channels")
      ;; drain control mailbox
      (loop for lambda = (safe-queue:dequeue control-mailbox)
            while lambda
            do (ignore-errors (funcall lambda)))
      (log:debug "queue drained")
      (setf (slot-value connection 'state) :closed))))

(defmethod connection-loop ((connection threaded-iolib-connection))
  (with-slots (event-base) connection
    (unwind-protect
         (catch 'stop-connection
           (open-socket-and-say-hello connection)
           (handler-bind ((error
                            (lambda (e)
                              (log:error "Unhandled unknown error: ~a" e)
                              (trivial-backtrace:print-backtrace e)
                              (unless *debug-connection*
                                (throw 'stop-connection e)))))
             (iolib:with-event-base (eb)
               (setf event-base eb)
               (perform-handshake connection
                                  (lambda ()
                                    (install-main-readers connection)))
               (iolib:event-dispatch event-base))))

      (shutdown-connection connection))))

(defmethod connection-open-p% ((connection threaded-iolib-connection))
  (and connection
       ;;;(bt:thread-alive-p (connection-thread connection))
       (eq (channel-state connection) :open)))

(defmethod connection.send ((connection iolib-connection) channel method)
  (dolist (frame (method-to-frames method (channel-id channel) (connection-frame-max% connection)))
    (send-to-connection-thread (connection)
      frame)))

(defmethod channel.receive ((connection threaded-iolib-connection) (method amqp-method-connection-blocked))
  (event! (connection-on-blocked% connection) connection (amqp-method-field-reason method)))

(defmethod channel.receive ((connection threaded-iolib-connection) (method amqp-method-connection-unblocked))
  (event! (connection-on-unblocked% connection) connection))

(defmethod channel.receive ((connection threaded-iolib-connection) (method amqp-method-connection-close-ok))
  (call-next-method)
  (throw 'stop-connection (values)))

(defmethod channel.receive ((connection threaded-iolib-connection) (method amqp-method-connection-close))
  (log:warn "Connection closed by server: ~a" (close-method-to-error connection method))
  ;; what to do with enqueued frames?
  ;; probably of-queue must be cleaned from all non-connection frames
  ;; then close-ok should be enqueued
  ;; while cleaning up do not forget to reject pending 'waiting-for-sync-reply' promises of respective channels
  (connection.send connection connection (make-instance 'amqp-method-connection-close-ok))
  ;; this should be executed only after close-ok is fully sent
  (send-to-connection-thread (connection)
    (throw 'stop-connection (values method))))

(defmethod connection.close% ((connection threaded-iolib-connection) timeout)
  (ignore-some-conditions (connection-closed-error)
    (channel.send connection (make-instance 'amqp-method-connection-close :class-id 0 :method-id 0 :reply-code +amqp-reply-success+)))
  (handler-case
      (sb-thread:join-thread (connection-thread connection) :timeout timeout)
    (sb-thread:join-thread-error (e)
      (case (sb-thread::join-thread-problem e)
        (:timeout (log:error "Connection thread stalled?")
         (sb-thread:terminate-thread (connection-thread connection)))
        (:abort (log:error "Connection thread aborted"))
        (t (log:error "Connection state is unknown"))))))
