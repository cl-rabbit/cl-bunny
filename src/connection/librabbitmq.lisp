(in-package :cl-bunny)

(defclass librabbitmq-connection (connection)
  ((cl-rabbit-connection :type cl-rabbit::connection
                         :initarg :connection
                         :reader connection-cl-rabbit-connection)))

(defclass shared-librabbitmq-connection (librabbitmq-connection shared-connection)
  ())

(defmethod connection-channel-max% ((connection librabbitmq-connection))
  (cl-rabbit::amqp-get-channel-max (cl-rabbit::connection/native-connection (connection-cl-rabbit-connection connection))))

(defmethod connection-frame-max% ((connection librabbitmq-connection))
  (cl-rabbit::amqp-get-frame-max (cl-rabbit::connection/native-connection (connection-cl-rabbit-connection connection))))

(defmethod connection-heartbeat% ((connection librabbitmq-connection))
  (cl-rabbit::amqp-get-heartbeat (cl-rabbit::connection/native-connection (connection-cl-rabbit-connection connection))))

(defmethod connection-server-properties% ((connection librabbitmq-connection))
  ;; TODO: cache this
  (cl-rabbit::amqp-table->lisp
   (cffi:convert-from-foreign
    (cl-rabbit::amqp-get-server-properties (cl-rabbit::connection/native-connection (connection-cl-rabbit-connection connection)))
    '(:struct  cl-rabbit::amqp-table-t))))

(defun librabbitmq-error->transport-error (e)
  (let ((code (cl-rabbit:rabbitmq-library-error/error-code e))
        (description (cl-rabbit:rabbitmq-library-error/error-description e)))
    (case code
      (:amqp-status-unknown-class 'amqp-unknown-method-class-error)
      (:amqp-status-unknown-method 'amqp-unknown-method-error)
      (:amqp-status-hostname-resolution-failed 'transport-error)
      (:amqp-status-incompatible-amqp-version 'transport-error)
      (:amqp-status-connection-closed 'connection-closed-error)
      (:amqp-status-bad-url 'transport-error)
      (:amqp-status-socket-error 'network-error)
      (:amqp-status-invalid-parameter 'transport-error)
      (:amqp-status-table-too-big 'transport-error)
      (:amqp-status-wrong-method 'transport-error)
      (:amqp-status-timeout 'transport-error)
      (:amqp-status-timer-failure 'transport-error)
      (:amqp-status-heartbeat-timeout 'network-error)
      (:amqp-status-unexpected-state 'transport-error)
      (:amqp-status-tcp-error 'network-error)
      (:amqp-status-tcp-socketlib-init-error 'network-error)
      (:amqp-status-ssl-error 'network-error)
      (:amqp-status-ssl-hostname-verify-failed 'network-error)
      (:amqp-status-ssl-peer-verify-failed 'network-error)
      (:amqp-status-ssl-connection-failed 'network-error)
      (t e))))

(defun translate-rabbitmq-server-error (e connection channel)
  (error (amqp-error-type-from-reply-code (cl-rabbit:rabbitmq-server-error/reply-code e))
         :reply-code (cl-rabbit:rabbitmq-server-error/reply-code e)
         :reply-text (cl-rabbit:rabbitmq-server-error/reply-text e)
         :connection connection
         :channel channel
         :class-id (cl-rabbit:rabbitmq-server-error/class-id e)
         :method-id (cl-rabbit:rabbitmq-server-error/method-id e)))

(defmethod connection.new% ((type (eql 'shared-librabbitmq-connection)) spec pool-tag)
  (let ((connection (make-instance 'shared-librabbitmq-connection :spec spec
                                                                  :pool-tag pool-tag)))
    (setup-execute-in-connection-lambda connection)
    connection))

(defmethod connection.new% ((type (eql 'librabbitmq-connection)) spec pool-tag)
  (let ((connection (make-instance 'librabbitmq-connection :spec spec
                                                           :pool-tag pool-tag)))
    connection))

(defmethod connection.close% ((connection librabbitmq-connection) timeout)
  (log:debug "Stopping AMQP connection")
  (setf (slot-value connection 'state) :closing)
  (event! (connection-on-close% connection) connection)
  (maphash (lambda (id channel)
              (declare (ignorable id))
              (setf (channel-open-p% channel) nil)
              (safe-queue:mailbox-send-message (channel-mailbox channel)
                                               (make-instance 'amqp-method-connection-close)))
           (connection-channels connection))
  (log:debug "closed-all-channels")
  (setf (slot-value connection 'state) :closed)
  (cl-rabbit:destroy-connection (connection-cl-rabbit-connection connection)))

(defmethod connection.close% ((connection shared-librabbitmq-connection) timeout)
  (if (eq (bt:current-thread) (connection-thread connection))
      (progn (connection.send connection connection (make-instance 'amqp-method-connection-close :reply-code +amqp-reply-success+))
             (throw 'stop-connection (values)))
      (handler-case
          (progn (execute-in-connection-thread (connection)
                   (connection.close% connection timeout))
                 (handler-case
                     (sb-thread:join-thread (connection-thread connection) :timeout timeout)
                   (sb-thread:join-thread-error (e)
                     (case (sb-thread::join-thread-problem e)
                       (:timeout (log:error "Connection thread stalled?")
                        (sb-thread:terminate-thread (connection-thread connection)))
                       (:abort (log:error "Connection thread aborted"))
                       (t (log:error "Connection state is unknown"))))))
        (connection-closed-error () (log:debug "Closing already closed connection"))))
  t)

(defmethod connection.init ((connection librabbitmq-connection))
  (handler-case
      (with-slots (cl-rabbit-connection cl-rabbit-socket spec) connection
        (setf cl-rabbit-connection (cl-rabbit:new-connection))
        (if (connection-spec-use-tls-p spec)
            (let ((socket (cl-rabbit:ssl-socket-new cl-rabbit-connection)))
              (when (connection-spec-tls-ca spec)
                (cl-rabbit::ssl-socket-set-cacert socket (connection-spec-tls-ca spec)))
              (when (and (connection-spec-tls-cert spec)
                         (connection-spec-tls-key spec))
                (cl-rabbit::ssl-socket-set-key socket
                                               (connection-spec-tls-cert spec)
                                               (connection-spec-tls-key spec)))
              (cl-rabbit::amqp-ssl-socket-set-verify-peer socket (connection-spec-tls-verify-peer spec))
              (cl-rabbit::amqp-ssl-socket-set-verify-hostname socket (connection-spec-tls-verify-hostname spec))
              (cl-rabbit:socket-open socket (connection-spec-host spec) (connection-spec-port spec)))
            (cl-rabbit:socket-open (cl-rabbit:tcp-socket-new cl-rabbit-connection) (connection-spec-host spec) (connection-spec-port spec)))
        (handler-bind ((cl-rabbit::rabbitmq-server-error
                         (lambda (error)
                            (when (= 403 (cl-rabbit:rabbitmq-server-error/reply-code error))
                              (error 'authentication-error :connection connection)))))
          (cl-rabbit:login-sasl-plain cl-rabbit-connection
                                      (connection-spec-vhost spec)
                                      (connection-spec-login spec)
                                      (connection-spec-password spec)
                                      :heartbeat (connection-spec-heartbeat-interval spec)
                                      :channel-max (connection-spec-channel-max spec)
                                      :frame-max (connection-spec-frame-max spec)
                                      :properties '(("product" . "cl-bunny(cl-rabbit transport)")
                                                    ("version" . "0.1")
                                                    ("copyright" . "Copyright (c) 2015 Ilya Khaprov <ilya.khaprov@publitechs.com> and CONTRIBUTORS <https://github.com/cl-rabbit/cl-bunny/blob/master/CONTRIBUTORS.md>")
                                                    ("information" . "see https://github.com/cl-rabbit/cl-bunny")
                                                    ("capabilities" . (("connection.blocked" . nil)
                                                                       ("publisher_confirms" . t)
                                                                       ("consumer_cancel_notify" . t)
                                                                       ("exchange_exchange_bindings" . t)
                                                                       ("basic.nack" . t)
                                                                       ("authentication_failure_close" . t)))))

          (setf (slot-value connection 'channel-id-allocator) (new-channel-id-allocator (connection-channel-max connection))
                (slot-value connection 'state) :open)))
    (cl-rabbit:rabbitmq-server-error (e)
      (error (translate-rabbitmq-server-error e connection nil)))
    (cl-rabbit:rabbitmq-library-error (e)
      (error (librabbitmq-error->transport-error e)))))

(defmethod connection.consume% ((connection librabbitmq-connection) timeout one-shot)
  (with-slots (cl-rabbit-connection event-base) connection
    (loop
      (block continue
        (assert (connection-open-p connection) () 'connection-closed-error :connection connection)
        (unless (or (cl-rabbit::data-in-buffer cl-rabbit-connection)
                    (cl-rabbit::frames-enqueued cl-rabbit-connection))
          (handler-case
              (iolib:wait-until-fd-ready (cl-rabbit::get-sockfd cl-rabbit-connection)
                                         :input timeout t)
            (iolib:poll-timeout () (if one-shot
                                       (return (values nil nil))
                                       (return-from continue)))))
        (let ((ret (wait-for-frame connection)))
          (when one-shot
            (return (values ret t))))))))

(defmethod connection.init ((connection shared-librabbitmq-connection))
  (handler-case
      (with-slots (cl-rabbit-connection cl-rabbit-socket spec) connection
        (setf cl-rabbit-connection (cl-rabbit:new-connection))
        (if (connection-spec-use-tls-p spec)
            (let ((socket (cl-rabbit:ssl-socket-new cl-rabbit-connection)))
              (when (connection-spec-tls-ca spec)
                (cl-rabbit::ssl-socket-set-cacert socket (connection-spec-tls-ca spec)))
              (when (and (connection-spec-tls-cert spec)
                         (connection-spec-tls-key spec))
                (cl-rabbit::ssl-socket-set-key socket
                                               (connection-spec-tls-cert spec)
                                               (connection-spec-tls-key spec)))
              (cl-rabbit::amqp-ssl-socket-set-verify-peer socket (connection-spec-tls-verify-peer spec))
              (cl-rabbit::amqp-ssl-socket-set-verify-hostname socket (connection-spec-tls-verify-hostname spec))
              (cl-rabbit:socket-open socket (connection-spec-host spec) (connection-spec-port spec)))
            (cl-rabbit:socket-open (cl-rabbit:tcp-socket-new cl-rabbit-connection) (connection-spec-host spec) (connection-spec-port spec)))
        (handler-bind ((cl-rabbit::rabbitmq-server-error
                         (lambda (error)
                            (when (= 403 (cl-rabbit:rabbitmq-server-error/reply-code error))
                              (error 'authentication-error :connection connection)))))
          (cl-rabbit:login-sasl-plain cl-rabbit-connection
                                      (connection-spec-vhost spec)
                                      (connection-spec-login spec)
                                      (connection-spec-password spec)
                                      :heartbeat (connection-spec-heartbeat-interval spec)
                                      :channel-max (connection-spec-channel-max spec)
                                      :frame-max (connection-spec-frame-max spec)
                                      :properties '(("product" . "cl-bunny(cl-rabbit transport)")
                                                    ("version" . "0.1")
                                                    ("copyright" . "Copyright (c) 2015 Ilya Khaprov <ilya.khaprov@publitechs.com> and CONTRIBUTORS <https://github.com/cl-rabbit/cl-bunny/blob/master/CONTRIBUTORS.md>")
                                                    ("information" . "see https://github.com/cl-rabbit/cl-bunny")
                                                    ("capabilities" . (("connection.blocked" . nil)
                                                                       ("publisher_confirms" . t)
                                                                       ("consumer_cancel_notify" . t)
                                                                       ("exchange_exchange_bindings" . t)
                                                                       ("basic.nack" . t)
                                                                       ("authentication_failure_close" . t)))))

          (setf (slot-value connection 'control-fd) (eventfd:eventfd.new 0)
                (slot-value connection 'control-mailbox) (safe-queue:make-queue)
                (slot-value connection 'channel-id-allocator) (new-channel-id-allocator (connection-channel-max connection))
                (slot-value connection 'state) :open)))
    (cl-rabbit:rabbitmq-server-error (e)
      (error (translate-rabbitmq-server-error e connection nil)))
    (cl-rabbit:rabbitmq-library-error (e)
      (error (librabbitmq-error->transport-error e)))))

(defun process-basic-ack-method (connection method channel)
  (declare (ignore connection))
  (assert (typep channel 'confirm-channel)) ;; TODO: specialize error
  (let ((basic-ack
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-basic-ack-t))))
    (channel.receive channel
                     (make-instance 'amqp-method-basic-ack :delivery-tag (getf basic-ack 'cl-rabbit::delivery-tag)
                                                           :multiple (getf basic-ack 'cl-rabbit::multiple)))))

(defun process-basic-return-method (connection method channel)
  (let ((cl-rabbit-connection (connection-cl-rabbit-connection connection)))
    (cl-rabbit::with-state (state cl-rabbit-connection)
      (cffi:with-foreign-objects ((message '(:struct cl-rabbit::amqp-message-t)))
        (log:debug "Got return method, reading message")
        (cl-rabbit::verify-rpc-reply state (channel-id channel) (cl-rabbit::amqp-read-message state (channel-id channel) message 0))
        (let* ((basic-return
                 (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                            '(:struct cl-rabbit::amqp-basic-return-t)))
               (method (make-instance 'amqp-method-basic-return
                                      :reply-code (getf basic-return 'cl-rabbit::reply-code)
                                      :reply-text (cl-rabbit::bytes->string (getf basic-return 'cl-rabbit::reply-text))
                                      :exchange (cl-rabbit::bytes->string (getf basic-return 'cl-rabbit::exchange))
                                      :routing-key (cl-rabbit::bytes->string (getf basic-return 'cl-rabbit::routing-key))
                                      :content-properties (apply #'make-instance 'amqp-basic-class-properties
                                                                 (cl-rabbit::load-properties-to-plist
                                                                  (cffi:foreign-slot-value message
                                                                                           '(:struct cl-rabbit::amqp-message-t) 'cl-rabbit::properties)))
                                      :content (cl-rabbit::bytes->array
                                                (cffi:foreign-slot-value message
                                                                         '(:struct cl-rabbit::amqp-message-t) 'cl-rabbit::body)))))
          (channel.receive channel method))))))

(defun process-channel-close-method (connection method channel)
  (declare (ignore connection))
  (let ((channel-close
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-channel-close-t))))
    (channel.receive channel
                     (make-instance 'amqp-method-channel-close :reply-code (getf channel-close 'cl-rabbit::reply-code)
                                                               :reply-text (cl-rabbit::bytes->string (getf channel-close 'cl-rabbit::reply-text))
                                                               :class-id (getf channel-close 'cl-rabbit::class-id)
                                                               :method-id (getf channel-close 'cl-rabbit::method-id)))))

(defun process-basic-cancel-method (connection method channel)
  (declare (ignore connection))
  (let ((basic-cancel
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-basic-cancel-t))))
    (channel.receive channel
                     (make-instance 'amqp-method-basic-cancel :consumer-tag (cl-rabbit::bytes->string (getf basic-cancel 'cl-rabbit::consumer-tag))
                                                              :nowait (getf basic-cancel 'cl-rabbit::nowait)))))

(defun process-connection-close-method (connection method)
  (let ((connection-close
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-connection-close-t))))
    (connection.receive connection
                        (make-instance 'amqp-method-connection-close :reply-code (getf connection-close 'cl-rabbit::reply-code)
                                                                     :reply-text (cl-rabbit::bytes->string (getf connection-close 'cl-rabbit::reply-text))
                                                                     :class-id (getf connection-close 'cl-rabbit::class-id)
                                                                     :method-id (getf connection-close 'cl-rabbit::method-id)))))

(defun process-connection-blocked-method (connection method)
  (let ((connection-blocked
          (cffi:convert-from-foreign (getf method 'cl-rabbit::decoded)
                                     '(:struct cl-rabbit::amqp-connection-blocked-t))))
    (connection.receive connection
                        (make-instance 'amqp-method-connection-blocked :reason (cl-rabbit::bytes->string (getf connection-blocked 'cl-rabbit::reason))))))

(defun process-connection-unblocked-method (connection method)
  (declare (ignore method))
  (connection.receive connection
                      (make-instance 'amqp-method-connection-unblocked)))

;;; see https://github.com/alanxz/rabbitmq-c/blob/master/examples/amqp_consumer.c
(defun process-unexpected-frame (connection)
  (let ((cl-rabbit-connection (connection-cl-rabbit-connection connection)))
    (cl-rabbit::with-state (state cl-rabbit-connection)
      (cffi:with-foreign-objects ((frame '(:struct cl-rabbit::amqp-frame-t)))
        (let* ((result (cl-rabbit::amqp-simple-wait-frame state frame))
               (result-tag (cl-rabbit::verify-status result)))
          (log:debug "Unexpected frame - channel: ~a, type: ~a"
                     (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::channel)
                     (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::frame-type))
          (when (= cl-rabbit::+amqp-frame-method+
                   (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::frame-type))
            (let* ((channel-id
                     (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::channel))
                   (method
                     (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::payload-method))
                   (method-id
                     (getf method 'cl-rabbit::id)))
              (labels ((warn-nonexistent-channel ()
                         (log:warn "Message received for closed channel: ~a" channel-id)))
                (case method-id
                  (#.cl-rabbit::+amqp-connection-close-method+ (process-connection-close-method connection method))
                  (#.cl-rabbit::+amqp-connection-blocked-method+ (process-connection-blocked-method connection method))
                  (#.cl-rabbit::+amqp-connection-unblocked-method+ (process-connection-unblocked-method connection method))
                  (otherwise
                   (if-let ((channel (connection.get-channel channel-id :connection connection)))
                     (if (channel-open-p% channel)
                         (case method-id
                           (#.cl-rabbit::+amqp-basic-ack-method+ (process-basic-ack-method connection method channel))
                           (#.cl-rabbit::+amqp-basic-return-method+  (process-basic-return-method connection method channel))
                           (#.cl-rabbit::+amqp-channel-close-method+ (process-channel-close-method connection method channel))
                           (#.cl-rabbit::+amqp-basic-cancel-method+ (process-basic-cancel-method connection method channel))
                           (otherwise (error "Unsupported unexpected method ~a" (amqp:method-class-from-signature method-id))))
                         ;; ELSE: We won't deliver messages to a closed channel
                         (log:warn "Incoming message on closed channel: ~s" channel))
                     ;; ELSE: Unused channel
                     (warn-nonexistent-channel)))))))
          result-tag)))))


(defun create-deliver-method-from-cl-rabbit-envelope (envelope)
  (make-instance 'amqp-method-basic-deliver
                 :consumer-tag (cl-rabbit:envelope/consumer-tag envelope)
                 :delivery-tag (cl-rabbit:envelope/delivery-tag envelope)
                 :redelivered (cl-rabbit:envelope/redelivered envelope)
                 :exchange (cl-rabbit:envelope/exchange envelope)
                 :routing-key (cl-rabbit:envelope/routing-key envelope)
                 :content-properties (let ((properties (cl-rabbit:message/properties (cl-rabbit:envelope/message envelope))))
                                       (when (find :persistent properties)
                                         (setf (getf properties :delivery-mode) (if (getf properties :persistent)
                                                                                    2
                                                                                    1))
                                         (remf properties :persistent))
                                       (apply #'make-instance 'amqp-basic-class-properties properties))
                 :content (cl-rabbit:message/body (cl-rabbit:envelope/message envelope))))

(defun wait-for-frame (connection)
  (log:trace "Waiting for frame on async connection: ~s" connection)
  (with-slots (channels cl-rabbit-connection) connection
    (tagbody
       (handler-bind ((cl-rabbit:rabbitmq-library-error
                        (lambda (condition)
                           (when (eq (cl-rabbit:rabbitmq-library-error/error-code condition)
                                     :amqp-status-unexpected-state)
                             (go unexpected-frame))
                           ;; why is that?
                           ;; when heartbeats enabled wait-for-frame called from event-base loop callback
                           ;; when server heartbeat arrives (i.e. socket available for reading)
                           ;; and consume-message/wait_frame_inner? returns AMQP-STATUS-TIMEOUT
                           ;; also, this happens no matter what supplied as :timout to consume-message
                           (when (and
                                  (not (= 0 (connection-heartbeat connection)))
                                  (eq (cl-rabbit::rabbitmq-library-error/error-code condition)
                                      :amqp-status-timeout))
                             (go exit)))))
         ;;
         (let ((envelope (cl-rabbit:consume-message cl-rabbit-connection :timeout 0)))
           (let* ((channel-id (cl-rabbit:envelope/channel envelope)))
             (labels ((warn-nonexistent-channel ()
                        (log:warn "Message received for closed channel: ~a" channel-id)))
               (if-let ((channel (gethash channel-id channels)))
                 (if (channel-open-p% channel)
                     (return-from wait-for-frame (channel.receive channel (create-deliver-method-from-cl-rabbit-envelope envelope)))
                     ;; ELSE: We won't deliver messages to a closed channel
                     (log:warn "Incoming message on closed channel: ~s" channel))
                 ;; ELSE: Unused channel
                 (warn-nonexistent-channel))))
           (go exit)))
     unexpected-frame
       (log:debug "Got unexpected frame")
       (process-unexpected-frame connection)
     exit)))

(defmethod connection-loop ((connection librabbitmq-connection))
  (with-slots (cl-rabbit-connection control-fd control-mailbox event-base) connection
    (let ((ret)
          (last-server-activity (get-universal-time))
          (last-client-activity (get-universal-time))) ;; TODO: monotonic time?
      (unwind-protect
           (setf ret
                 (catch 'stop-connection
                   (handler-bind ((cl-rabbit:rabbitmq-library-error
                                    (lambda (e)
                                       (let ((actual-error (librabbitmq-error->transport-error e)))
                                         (log:error "Unhandled transport error: ~a" actual-error)
                                         (unless *debug-connection*
                                           (throw 'stop-connection actual-error)))))
                                  (error
                                    (lambda (e)
                                       (log:error "Unhandled unknown error: ~a" e)
                                       (unless *debug-connection*
                                         (throw 'stop-connection e)))))
                     (iolib:with-event-base (event-base)
                       (iolib:set-io-handler event-base
                                             control-fd
                                             :read (lambda (fd e ex)
                                                      (declare (ignorable fd e ex))
                                                      (eventfd.read control-fd)
                                                      (log:debug "Got lambda to execute on connection thread")
                                                      ;; last-client-acitivity probably should be also updated there
                                                      ;; but it's not required for lambda to do any networking
                                                      (loop for lambda = (safe-queue:dequeue control-mailbox)
                                                            while lambda
                                                            do (funcall lambda))))
                       (iolib:set-io-handler event-base
                                             (cl-rabbit::get-sockfd cl-rabbit-connection)
                                             :read (lambda (fd e ex)
                                                      (declare (ignorable fd e ex))
                                                      (when ex
                                                        (throw 'stop-connection nil))
                                                      ;; something to be read on the socket
                                                      ;;  wait_frame_inner should send heartbeat
                                                      ;; actually last-activity not only shows when last hearbeat
                                                      ;; sent to the server but also when we received something from the
                                                      ;; server last time
                                                      ;; TODO: what if server quiet for more than 2 heartbeats intervals
                                                      (setf last-server-activity (get-universal-time)) ;; TODO: monotonic time?
                                                      ;; setting this here because as already said wait_frame_inner will send hearbeat
                                                      ;; but also it will throw an error if something badly wrong
                                                      (setf last-client-activity (get-universal-time)) ;; TODO: monotonic time?
                                                      (wait-for-frame connection)))
                       ;; on one hand this isn't needed: wait_frame_inner should send heartbeat anyway
                       ;; however wait_frame_inner only responds
                       ;; what if server gone?
                       (unless (= 0 (connection-heartbeat connection))
                         (iolib:add-timer event-base
                                          (lambda ()
                                             (let ((now (get-universal-time)))  ;; TODO: monotonic time?
                                               (cond
                                                 ((> (/ (- now last-server-activity) (connection-heartbeat% connection))
                                                     2)
                                                  (throw 'stop-connection 'transport-error))
                                                 ((> now (+ last-client-activity (connection-heartbeat% connection)))
                                                  (log:debug "Sending HEARTBEAT")
                                                  ;; btw send_frame_inner should update send_heartbeat deadline
                                                  (cffi:with-foreign-objects ((frame '(:struct cl-rabbit::amqp-frame-t)))
                                                    (setf (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::channel) 0
                                                          (cffi:foreign-slot-value frame '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::frame-type) +amqp-frame-heartbeat+)
                                                    (cl-rabbit::verify-status (cl-rabbit::amqp-send-frame (cl-rabbit::connection/native-connection (connection-cl-rabbit-connection connection))
                                                                                                          frame))
                                                    (setf last-client-activity (get-universal-time)))))))
                                          (+ 0.4 (/ (connection-heartbeat% connection) 2))))
                       (loop
                         if (or (cl-rabbit::data-in-buffer cl-rabbit-connection)
                                (cl-rabbit::frames-enqueued cl-rabbit-connection))
                         do (wait-for-frame connection)
                         else
                         do (iolib:event-dispatch event-base :one-shot t))))))
        (with-write-lock (connection-state-lock connection)
          (setf (slot-value connection 'state) :closing)
          (event! (connection-on-close% connection) connection)
          (eventfd.close control-fd)
          (log:debug "Stopping AMQP connection")
          (when (connection-pool connection)
            (connections-pool.remove connection))
          (maphash (lambda (id channel)
                      (declare (ignorable id))
                      (setf (channel-open-p% channel) nil)
                      (safe-queue:mailbox-send-message (channel-mailbox channel)
                                                       (make-instance 'amqp-method-connection-close)))
                   (connection-channels connection))
          (log:debug "closed-all-channels")
          ;; drain control mailbox
          (loop for lambda = (safe-queue:dequeue control-mailbox)
                while lambda
                do (ignore-errors (funcall lambda)))
          (log:debug "queue drained")
          (setf (slot-value connection 'state) :closed))
        (cl-rabbit:destroy-connection cl-rabbit-connection)
        (if (functionp ret)
            (funcall ret))))))

(defmethod connection.close-ok% ((connection librabbitmq-connection) callback)
  (connection.send connection connection (make-instance 'amqp-method-connection-close-ok))
  (throw 'stop-connection callback)
  t)

(defmethod properties->alist ((properties list))
  (let ((properties (copy-list properties)))
    (when (find :persistent properties)
      (setf (getf properties :delivery-mode) (if (getf properties :persistent)
                                                 2
                                                 1))
      (remf properties :persistent))
    (properties->alist (apply #'make-instance 'amqp-basic-class-properties properties))))

(defgeneric reply-to (queue))

(defmethod properties->alist ((properties amqp-basic-class-properties))
  (collectors:with-alist-output (add-prop)
    (when (slot-boundp properties 'amqp::content-type)
      (add-prop :content-type (amqp-property-content-type properties)))
    (when (slot-boundp properties 'amqp::content-encoding)
      (add-prop :content-encoding (amqp-property-content-encoding properties)))
    (when (slot-boundp properties 'amqp::headers)
      (add-prop :headers (amqp-property-headers properties)))
    (when (slot-boundp properties 'amqp::delivery-mode)
      (add-prop :persistent (ecase (amqp-property-delivery-mode properties)
                              (2 t)
                              (1 nil))))
    (when (slot-boundp properties 'amqp::priority)
      (add-prop :priority (amqp-property-priority properties)))
    (when (slot-boundp properties 'amqp::correlation-id)
      (add-prop :correlation-id (amqp-property-correlation-id properties)))
    (when (slot-boundp properties 'amqp::reply-to)
      (add-prop :reply-to (reply-to (amqp-property-reply-to properties))))
    (when (slot-boundp properties 'amqp::expiration)
      (add-prop :expiration (amqp-property-expiration properties)))
    (when (slot-boundp properties 'amqp::message-id)
      (add-prop :message-id (amqp-property-message-id properties)))
    (when (slot-boundp properties 'amqp::timestamp)
      (add-prop :timestamp (amqp-property-timestamp properties)))
    (when (slot-boundp properties 'amqp::type)
      (add-prop :type (amqp-property-type properties)))
    (when (slot-boundp properties 'amqp::user-id)
      (add-prop :user-id (amqp-property-user-id properties)))
    (when (slot-boundp properties 'amqp::app-id)
      (add-prop :app-id (amqp-property-app-id properties)))
    (when (slot-boundp properties 'amqp::cluster-id)
      (add-prop :cluster-id (amqp-property-cluster-id properties)))))

(defmethod connection.send :before ((connection librabbitmq-connection) channel method)
  (when (and (amqp-method-synchronous-p method)
             (ignore-errors (amqp-method-field-nowait method)))
    (log:warn "Librabbitmq connection: nowait not supported")))

(defmethod connection.send :around ((connection librabbitmq-connection) channel method)
  (log:trace "connection: ~a, channel: ~a, method: ~a" connection channel method)
  (assert (connection-open-p (channel-connection channel)) () 'connection-closed-error :connection (channel-connection channel))
  (unless (typep method 'amqp-method-channel-open)
    (assert (channel-open-p% channel) () 'channel-closed-error :channel channel))
  ;; convert close replies for sync methods
  (handler-case
      (call-next-method)
    (cl-rabbit:rabbitmq-server-error (e)
      (translate-rabbitmq-server-error e connection channel))
    (cl-rabbit:rabbitmq-library-error (e)
      (error (librabbitmq-error->transport-error e)))))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-connection-close))
  (cl-rabbit:connection-close (connection-cl-rabbit-connection connection) :code (amqp-method-field-reply-code method)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-connection-close-ok))
  (declare (ignore channel))
  (cl-rabbit:connection-close-ok (connection-cl-rabbit-connection connection)
                                 0))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-open))
  (cl-rabbit:channel-open (connection-cl-rabbit-connection connection)
                          (channel-id channel))
  (make-instance 'amqp-method-channel-open-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-flow))
  (multiple-value-bind (active)
      (cl-rabbit:channel-flow (connection-cl-rabbit-connection connection)
                              (channel-id channel)
                              (amqp-method-field-active method))
    (make-instance 'amqp-method-channel-flow-ok
                   :active active)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-flow-ok))
  (cl-rabbit:channel-flow-ok (connection-cl-rabbit-connection connection)
                             (channel-id channel)
                             (amqp-method-field-active method)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-close))
  (assert (equal (amqp-method-field-reply-text method) "") nil 'error "librabbitmq channel.close only supports reply-code") ;; TODO: specialize error
  (assert (= (amqp-method-field-class-id method) 0) nil 'error "librabbitmq channel.close only supports reply-code") ;; TODO: specialize error
  (assert (= (amqp-method-field-method-id method) 0) nil 'error "librabbitmq channel.close only supports reply-code") ;; TODO: specialize error
  (cl-rabbit:channel-close (connection-cl-rabbit-connection connection)
                           (channel-id channel)
                           :reply-code (amqp-method-field-reply-code method))
  (make-instance 'amqp-method-channel-close-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-channel-close-ok))
  (cl-rabbit:channel-close-ok (connection-cl-rabbit-connection connection)
                              (channel-id channel)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-exchange-declare))
  (cl-rabbit:exchange-declare (connection-cl-rabbit-connection connection)
                              (channel-id channel)
                              (amqp-method-field-exchange method)
                              :type (amqp-method-field-type method)
                              :passive (amqp-method-field-passive method)
                              :durable (amqp-method-field-durable method)
                              :auto-delete (amqp-method-field-auto-delete method)
                              :internal (amqp-method-field-internal method)
                              :arguments (amqp-method-field-arguments method))
  (make-instance 'amqp-method-exchange-declare-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-exchange-delete))
  (cl-rabbit:exchange-delete (connection-cl-rabbit-connection connection)
                             (channel-id channel)
                             (amqp-method-field-exchange method)
                             :if-unused (amqp-method-field-if-unused method))
  (make-instance 'amqp-method-exchange-delete-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-exchange-bind))
  (cl-rabbit:exchange-bind (connection-cl-rabbit-connection connection)
                           (channel-id channel)
                           :destination (amqp-method-field-destination method)
                           :source (amqp-method-field-source method)
                           :routing-key (amqp-method-field-routing-key method)
                           :arguments (amqp-method-field-arguments method))
  (make-instance 'amqp-method-exchange-bind-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-exchange-unbind))
  (cl-rabbit:exchange-unbind (connection-cl-rabbit-connection connection)
                             (channel-id channel)
                             :destination (amqp-method-field-destination method)
                             :source (amqp-method-field-source method)
                             :routing-key (amqp-method-field-routing-key method)
                             :arguments (amqp-method-field-arguments method))
  (make-instance 'amqp-method-exchange-unbind-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-queue-declare))
  (multiple-value-bind (queue message-count consumer-count)
      (cl-rabbit:queue-declare (connection-cl-rabbit-connection connection)
                               (channel-id channel)
                               :queue (amqp-method-field-queue method)
                               :passive (amqp-method-field-passive method)
                               :durable (amqp-method-field-durable method)
                               :exclusive (amqp-method-field-exclusive method)
                               :auto-delete (amqp-method-field-auto-delete method)
                               :arguments (amqp-method-field-arguments method))
    (make-instance 'amqp-method-queue-declare-ok
                   :queue queue
                   :message-count message-count
                   :consumer-count consumer-count)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-queue-bind))
  (cl-rabbit:queue-bind (connection-cl-rabbit-connection connection)
                        (channel-id channel)
                        :queue (amqp-method-field-queue method)
                        :exchange (amqp-method-field-exchange method)
                        :routing-key (amqp-method-field-routing-key method)
                        :arguments (amqp-method-field-arguments method))
  (make-instance 'amqp-method-queue-bind-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-queue-purge))
  (multiple-value-bind (message-count)
      (cl-rabbit:queue-purge (connection-cl-rabbit-connection connection)
                             (channel-id channel)
                             :queue (amqp-method-field-queue method))
    (make-instance 'amqp-method-queue-purge-ok
                   :message-count message-count)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-queue-delete))
  (multiple-value-bind (message-count)
      (cl-rabbit:queue-delete (connection-cl-rabbit-connection connection)
                              (channel-id channel)
                              :queue (amqp-method-field-queue method)
                              :if-unused (amqp-method-field-if-unused method)
                              :if-empty (amqp-method-field-if-empty method))
    (make-instance 'amqp-method-queue-delete-ok
                   :message-count message-count)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-queue-unbind))
  (cl-rabbit:queue-unbind (connection-cl-rabbit-connection connection)
                          (channel-id channel)
                          :queue (amqp-method-field-queue method)
                          :exchange (amqp-method-field-exchange method)
                          :routing-key (amqp-method-field-routing-key method)
                          :arguments (amqp-method-field-arguments method))
  (make-instance 'amqp-method-queue-unbind-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-publish))
  (cl-rabbit:basic-publish (connection-cl-rabbit-connection connection)
                           (channel-id channel)
                           :exchange (amqp-method-field-exchange method)
                           :routing-key (amqp-method-field-routing-key method)
                           :mandatory (amqp-method-field-mandatory method)
                           :immediate (amqp-method-field-immediate method)
                           :content (amqp-method-content method)
                           :content-properties (properties->alist (amqp-method-content-properties method))))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-qos))
  (cl-rabbit:basic-qos (connection-cl-rabbit-connection connection)
                       (channel-id channel)
                       (amqp-method-field-prefetch-size method)
                       (amqp-method-field-prefetch-count method)
                       :global (amqp-method-field-global method))
  (make-instance 'amqp-method-basic-qos-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-get))
  (let ((state (cl-rabbit::connection/native-connection (connection-cl-rabbit-connection connection))))
    (unwind-protect
         (let ((rpc-reply (cl-rabbit::with-bytes-string (queue-bytes (queue-name (amqp-method-field-queue method)))
                            (cl-rabbit::amqp-basic-get state (channel-id channel) queue-bytes (if (amqp-method-field-no-ack method) 1 0)))))
           (cl-rabbit::verify-rpc-reply state (channel-id channel) rpc-reply)
           (let* ((reply (getf rpc-reply 'cl-rabbit::reply))
                  (id (getf reply 'cl-rabbit::id))
                  (decoded (getf reply 'cl-rabbit::decoded)))
             (ecase id
               (#.cl-rabbit::+amqp-basic-get-empty-method+
                (make-instance 'amqp-method-basic-get-empty
                               :cluster-id (cl-rabbit::bytes->string
                                            (cffi:foreign-slot-value decoded '(:struct cl-rabbit::amqp-basic-get-empty-t) 'cl-rabbit::cluster-id))))
               (#.cl-rabbit::+amqp-basic-get-ok-method+
                (let ((struct (cffi:convert-from-foreign decoded '(:struct cl-rabbit::amqp-basic-get-ok-t))))
                  (cffi:with-foreign-objects ((message '(:struct cl-rabbit::amqp-message-t)))
                    (log:debug "Got return method, reading message")
                    (cl-rabbit::verify-rpc-reply state (channel-id channel) (cl-rabbit::amqp-read-message state (channel-id channel) message 0))
                    (make-instance 'amqp-method-basic-get-ok
                                   :delivery-tag (getf struct 'cl-rabbit::delivery-tag)
                                   :redelivered (getf struct 'cl-rabbit::redelivered)
                                   :exchange (cl-rabbit::bytes->string (getf struct 'cl-rabbit::exchange))
                                   :routing-key (cl-rabbit::bytes->string (getf struct 'cl-rabbit::routing-key))
                                   :message-count (getf struct 'cl-rabbit::message-count)
                                   :content-properties (let ((properties (cl-rabbit::load-properties-to-plist
                                                                          (cffi:foreign-slot-value message
                                                                                                   '(:struct cl-rabbit::amqp-message-t) 'cl-rabbit::properties))))
                                                         (when (find :persistent properties)
                                                           (setf (getf properties :delivery-mode) (if (getf properties :persistent)
                                                                                                      2
                                                                                                      1))
                                                           (remf properties :persistent))
                                                         (apply #'make-instance 'amqp-basic-class-properties properties))
                                   :content (cl-rabbit::bytes->array
                                             (cffi:foreign-slot-value message
                                                                      '(:struct cl-rabbit::amqp-message-t) 'cl-rabbit::body)))))))))
      (cl-rabbit::maybe-release-buffers state))))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-consume))
  (multiple-value-bind (consumer-tag)
      (cl-rabbit:basic-consume (connection-cl-rabbit-connection connection)
                               (channel-id channel)
                               (amqp-method-field-queue method)
                               :consumer-tag (amqp-method-field-consumer-tag method)
                               :no-local (amqp-method-field-no-local method)
                               :no-ack (amqp-method-field-no-ack method)
                               :exclusive (amqp-method-field-exclusive method)
                               :arguments (amqp-method-field-arguments method))
    (make-instance 'amqp-method-basic-consume-ok
                   :consumer-tag consumer-tag)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-cancel))
  (multiple-value-bind (consumer-tag)
      (cl-rabbit:basic-cancel (connection-cl-rabbit-connection connection)
                              (channel-id channel)
                              (amqp-method-field-consumer-tag method))
    (make-instance 'amqp-method-basic-cancel-ok
                   :consumer-tag (amqp-method-field-consumer-tag method);; I'm lazy here because librabbitmq is sync anyway
                   )))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-ack))
  (cl-rabbit:basic-ack (connection-cl-rabbit-connection connection)
                       (channel-id channel)
                       (amqp-method-field-delivery-tag method)
                       :multiple (amqp-method-field-multiple method)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-basic-nack))
  (cl-rabbit:basic-nack (connection-cl-rabbit-connection connection)
                        (channel-id channel)
                        (amqp-method-field-delivery-tag method)
                        :multiple (amqp-method-field-multiple method)
                        :requeue (amqp-method-field-requeue method)))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-confirm-select))
  (cl-rabbit:confirm-select (connection-cl-rabbit-connection connection)
                            (channel-id channel))
  (make-instance 'amqp-method-confirm-select-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-tx-select))
  (cl-rabbit:tx-select (connection-cl-rabbit-connection connection)
                       (channel-id channel))
  (make-instance 'amqp-method-tx-select-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-tx-commit))
  (cl-rabbit:tx-commit (connection-cl-rabbit-connection connection)
                       (channel-id channel))
  (make-instance 'amqp-method-tx-commit-ok))

(defmethod connection.send ((connection librabbitmq-connection) channel (method amqp-method-tx-rollback))
  (cl-rabbit:tx-rollback (connection-cl-rabbit-connection connection)
                         (channel-id channel))
  (make-instance 'amqp-method-tx-rollback-ok))
