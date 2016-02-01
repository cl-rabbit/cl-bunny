(in-package :cl-bunny)
(enable-binary-string-syntax)



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

(defvar *event-base*)

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

(defun socket.connect (socket address port)
  (iolib:connect socket address :port port :wait nil)
  (bb:wait (socket-writeable socket)
    (socket.check socket)))


(defstruct output-buffer
  (sequence)
  (index))

(defun write-sequnce-async%% (socket buffer resolve)
  (let ((sent (iolib:send-to socket (output-buffer-sequence buffer) :start (output-buffer-index buffer))))
    (incf (output-buffer-index buffer) sent)
    (when (= (output-buffer-index buffer) (length (output-buffer-sequence buffer)))
      (iolib:remove-fd-handlers *event-base* (iolib:socket-os-fd socket) :write t)
      (funcall resolve))))

(defun write-sequence-async% (socket sequence index resolve)
  (let ((output-buffer (make-output-buffer :sequence sequence :index index)))
    (iolib:set-io-handler *event-base* (iolib:socket-os-fd socket) :write
                          (lambda (fd e ex)
                            (write-sequnce-async%% socket output-buffer resolve)))))

(defun write-sequence-async (socket sequence &optional (start 0))
  (bb:with-promise (resolve reject :resolve-fn resolve-fn)
    (write-sequence-async% socket sequence start resolve-fn)))

(defun read-frame-async%% (connection parser resolve)
  (with-slots (socket spec read-buffer rb-index rb-end) connection
    (bb:alet ((start-index (or rb-index
                               (bb:wait (socket-readable (connection-socket connection))
                                 (multiple-value-bind (_octets read) (iolib:receive-from socket :buffer read-buffer)
                                   (declare (ignore _octets))
                                   (setf rb-end read)
                                   0)))))
      (multiple-value-bind (read-buffer-index parsed)
          (frame-parser-consume parser read-buffer :start start-index :end rb-end)
        (if parsed
            (progn (if (= read-buffer-index rb-end)
                       (setf rb-index nil)
                       (setf rb-index read-buffer-index))
                   (funcall resolve))
            (progn (if (= read-buffer-index rb-end)
                       (setf rb-index nil)
                       (setf rb-index read-buffer-index))
                   (read-frame-async%% connection parser resolve)))))))

(defun read-frame-async% (connection parser)
  (bb:with-promise (resolve reject :resolve-fn resolve-fn)
    (read-frame-async%% connection parser resolve-fn)))

(defun read-frame-async (connection)
  (bb:with-promise (resolve reject)
    (let* ((frame-ended)
           (payload-parser)
           (frame)
           (parser (amqp:make-frame-parser
                    :on-frame-type (lambda (parser frame-type)
                                     (declare (ignore parser))
                                     (setf frame (make-instance (amqp:frame-class-from-frame-type frame-type))))
                    :on-frame-channel (lambda (parser frame-channel)
                                        (declare (ignore parser))
                                        (setf (amqp:frame-channel frame) frame-channel))
                    :on-frame-payload-size (lambda (parser payload-size)
                                             (declare (ignore parser))
                                             ;; validate frame size
                                             (unless (= +amqp-frame-heartbeat+ (amqp::frame-type frame))
                                               (setf (amqp:frame-payload-size frame) payload-size
                                                     payload-parser (amqp:make-frame-payload-parser frame))))
                    :on-frame-payload (lambda (parser data start end)
                                        (declare (ignore parser))
                                        (when payload-parser
                                          (amqp:frame-payload-parser-consume payload-parser data :start start :end end)))
                    :on-frame-end (lambda (parser)
                                    (declare (ignore parser))
                                    (when payload-parser
                                      (amqp:frame-payload-parser-finish payload-parser))
                                    (setf frame-ended t)))))
      (bb:wait (read-frame-async% connection parser)
        (resolve frame)))))

(defun read-method-async% (method-assembler connection)
  (bb:chain (read-frame-async connection)
    (:then (frame)
           (if-let ((method (consume-frame method-assembler frame)))
             method
             (read-method-async% method-assembler connection)))))

(defun read-method-async (connection)
  (let* ((method-assembler (make-instance 'method-assembler)))
    (read-method-async% method-assembler connection)))

(defun read-method-for-channel-async% (method-assembler connection channel)
  (bb:chain (read-frame-async connection)
    (:then (frame)
           (if (= (frame-channel frame) (channel-id channel))
               (if-let ((method (consume-frame method-assembler frame)))
                 method
                 (read-method-for-channel-async% method-assembler connection channel))
               (process-async-frame connection frame)))))

(defun read-method-for-channel-async (connection channel)
  (let* ((method-assembler (make-instance 'method-assembler)))
    (read-method-for-channel-async% method-assembler connection channel)))

(defun get-frame-bytes (frame)
  (let ((obuffer (amqp:new-obuffer)))
    (amqp:frame-encoder frame obuffer)
    (amqp:obuffer-get-bytes obuffer)))

(defun send-frame-async (c frame)
  (write-sequence-async (connection-socket c) (get-frame-bytes frame)))

(defun wait-for-sync-reply (connection channel reply-matcher)
  (bb:alet* ((im (read-method-for-channel-async connection channel)))
    (if (funcall reply-matcher im)
        im
        (progn
          ;; (connection.receive connection im
          (wait-for-sync-reply connection channel reply-matcher)))))

(defun connection.send-async (connection channel method)
  (bb:chain
      (bb::aeach (lambda (frame)
                   (send-frame-async connection frame))
                 (method-to-frames method (channel-id channel) (connection-frame-max% connection)))
    (:then ()
           (multiple-value-bind (sync reply-matcher) (amqp-method-synchronous-p method)
             (if sync
                 (wait-for-sync-reply connection channel reply-matcher)
                 t)))
    (:catch (e) (print e))))

(defun connection.open-async (connection)
  (with-slots (socket spec) connection
    (setf socket (iolib:make-socket))
    (bb:chain (socket.connect socket (iolib:lookup-hostname (connection-spec-host spec)) (connection-spec-port spec))
      (:then ()
             (write-sequence-async socket #b"AMQP\x0\x0\x9\x1"))
      (:then ()
             (force-output socket))
      (:then ()
             (read-method-async connection))
      (:then (start-method)
             (assert (typep start-method 'amqp-method-connection-start))
             (connection.send-async connection connection (make-instance 'amqp-method-connection-start-ok :response " guest guest" :client-properties '())))
      (:then ()
             (read-method-async connection))
      (:then (tune-method)
             (assert (typep tune-method 'amqp-method-connection-tune))
             (connection.send-async connection connection (make-instance 'amqp-method-connection-tune-ok :heartbeat (connection-heartbeat% connection)
                                                                                                         :frame-max (connection-frame-max% connection)
                                                                                                         :channel-max (connection-channel-max% connection))))
      (:then ()
             (connection.send-async connection connection (make-instance 'amqp-method-connection-open)))
      (:then (open-ok-method)
             (assert (typep open-ok-method 'amqp-method-connection-open-ok))
             (setf (slot-value connection 'state) :open))
      (:catch (e) (print e)))))

(defun connection.new.open-async ()
  (connection.open-async (connection.new% 'iolib-connection (make-connection-spec "amqp://") "amqp://")))

(defun connection.close-async (connection)
  (setf (slot-value connection 'state) :closing)
  (bb:chain (connection.send-async connection connection
                                   (make-instance 'amqp-method-connection-close
                                                  :method-id 0
                                                  :class-id 0
                                                  :reply-text "Goodbye"
                                                  :reply-code 200))
    (:then (reply)
           (assert (typep reply
                          'amqp-method-connection-close-ok))))
  (close (connection-socket connection))
  (setf (slot-value connection 'state) :closed)
  connection)

(defun channel.new.open-async (connection)
  (bb:alet ((channel (channel.new :connection connection :channel-id 1)))
    (bb:chain (connection.send-async connection channel (make-instance 'amqp-method-channel-open))
      (:attach ()
               channel))))

(defun channel.close-async (c channel)
  (connection.send-async c channel (make-instance 'amqp-method-channel-close :method-id 0
                                                                             :class-id 0
                                                                             :reply-code 200)))

(defun channel.open-async (connection channel-id)
  (bb:alet* ((channel (channel.new :connection connection :channel-id channel-id))
             (nil (connection.send-async connection channel (make-instance 'amqp-method-channel-open))))
    channel))

(defun channel.close-async (channel)
  (connection.send-async (channel-connection channel) channel (make-instance 'amqp-method-channel-close :method-id 0
                                                                                   :class-id 0
                                                                                   :reply-code 200)))

(defun queue.declare-async (channel &optional (name ""))
  (connection.send-async (channel-connection channel) channel (make-instance 'amqp-method-queue-declare :queue name)))

#+qwe
(iolib:with-event-base (*event-base*)
  (let ((connection (connection.new% 'iolib-connection (make-connection-spec "amqp://") "amqp://")))
    (bb:chain (connection.open-async connection)
      (:then ()
             (print "connection opened"))
      (:then ()
             (bb:alet* ((channel (channel.open-async connection 1))
                        (qd-ok (queue.declare-async channel)))
               (bb:chain
                   (connection.send-async connection channel (make-instance 'amqp-method-basic-consume
                                                                      :queue (amqp-method-field-queue qd-ok)))
                 (:then ()
                        (connection.send-async connection channel (make-instance 'amqp-method-basic-publish :content #b"Hello World!"
                                                                           :routing-key (amqp-method-field-queue qd-ok))))
                 (:then ()
                        (read-method-for-channel-async connection channel))
                 (:then (method)
                        (print (amqp-method-content method)))
                 (:finally ()
                   (channel.close-async channel)))))
      (:finally ()
             (connection.close-async connection))
      (:finally ()
             (print "connection.closed")
             (iolib:exit-event-loop *event-base*))
      (:catch (e)
        (print e))))
  (iolib:event-dispatch *event-base*))
