(in-package :cl-bunny)

(enable-binary-string-syntax)
(cl-interpol:enable-interpol-syntax)
(defclass blocking-iolib-connection (iolib-connection)
  ())

(defmethod connection.new% ((type (eql 'blocking-iolib-connection)) spec pool-tag)
  (let ((connection (make-instance 'threaded-iolib-connection :spec spec
                                                              :pool-tag pool-tag)))
    connection))

(defun read-frame (connection)
  (with-slots (socket spec read-buffer rb-index rb-end) connection
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
      (loop

        (let ((start-index (or rb-index
                               (progn
                                 (iolib:wait-until-fd-ready (iolib:socket-os-fd socket) :input)
                                 (multiple-value-bind (_octets read) (iolib:receive-from socket :buffer read-buffer)
                                   (declare (ignore _octets))
                                   (setf rb-end read))
                                 0))))
          (multiple-value-bind (read-buffer-index parsed)
              (frame-parser-consume parser read-buffer :start start-index :end rb-end)
            (if parsed
                (progn (if (= read-buffer-index rb-end)
                           (setf rb-index nil)
                           (setf rb-index read-buffer-index))
                       (unless (= +amqp-frame-heartbeat+ (amqp::frame-type frame))
                         (return frame)))
                (if (= read-buffer-index rb-end)
                    (setf rb-index nil)
                    (setf rb-index read-buffer-index)))))))))

(defun read-method (connection)
  (let* ((method-assembler (make-instance 'method-assembler)))
    (loop
      as frame = (read-frame connection) do
         (if-let ((method (consume-frame method-assembler frame)))
           (return method)))))

(defun process-async-frame (connection frame)
  (break)
  (print frame))

(defun read-method-for-channel (connection channel)
  (let ((method-assembler (make-instance 'method-assembler)))
    (loop
      as frame = (read-frame connection) do
         (if (= (frame-channel frame) (channel-id channel))
             (if-let ((method (consume-frame method-assembler frame)))
               (return method))
             (process-async-frame connection frame)))))

(defun get-frame-bytes (frame)
  (let ((obuffer (amqp:new-obuffer)))
    (amqp:frame-encoder frame obuffer)
    (amqp:obuffer-get-bytes obuffer)))

(defun send-frame (connection frame)
  (with-slots (socket spec) connection
    (let ((frame-bytes (get-frame-bytes frame))
          (position 0))
      (loop
        (iolib:wait-until-fd-ready (iolib:socket-os-fd socket) :output)
        (setf position (iolib:send-to socket frame-bytes :start position))
        (when (= position (length frame-bytes))
          (return))))))

(defmethod connection.send ((connection blocking-iolib-connection) channel method)
  (loop for frame in (method-to-frames method (channel-id channel) (connection-frame-max% connection)) do
           (send-frame connection frame))
  (multiple-value-bind (sync reply-matcher) (amqp-method-synchronous-p method)
    (if sync
        (loop
          (let* ((im (read-method-for-channel connection channel)))
            (if (funcall reply-matcher im)
                (return im)
                ;; (connection.receive connection im)                
                )))
        t)))

(defmethod connection.init ((connection blocking-iolib-connection))
  (with-slots (socket spec) connection
    (setf (slot-value connection 'state) :opening)
    (setf socket (iolib:make-socket))
    (iolib:connect socket (iolib:lookup-hostname (connection-spec-host spec)) :port (connection-spec-port spec))
    (write-sequence #b"AMQP\x0\x0\x9\x1" socket)
    (force-output socket)
    (let ((start-method (read-method connection)))
      (assert (typep start-method 'amqp-method-connection-start)))
    (connection.send connection connection (make-instance 'amqp-method-connection-start-ok :response #?"\x0guest\x0guest" :client-properties '()))
    (let ((tune-method (read-method connection)))
      (assert (typep tune-method 'amqp-method-connection-tune))
      ;; TODO: handle tune
      )
    (connection.send connection connection (make-instance 'amqp-method-connection-tune-ok :heartbeat 0
                                                                                          :frame-max (connection-frame-max% connection)
                                                                                          :channel-max (connection-channel-max% connection)))

    (assert (typep (connection.send connection connection (make-instance 'amqp-method-connection-open)) 'amqp-method-connection-open-ok))
    (setf (slot-value connection 'state) :open)
    connection))

(defmethod connection.close% ((connection blocking-iolib-connection) timeout)
  (declare (ignore timeout))
  (setf (slot-value connection 'state) :closing)
  (assert (typep (connection.send connection connection (make-instance 'amqp-method-connection-close :method-id 0
                                                                                   :class-id 0
                                                                                   :reply-text "Goodbye"
                                                                                   :reply-code 200))
                 'amqp-method-connection-close-ok))
  (close (connection-socket connection))
  (setf (slot-value connection 'state) :closed)
  connection)

#++
(let ((c (connection.new% 'blocking-iolib-connection (make-connection-spec "amqp://") "amqp://")))
  (connection.open c)
  (let ((channel (channel.new :connection c :channel-id 1)))
    (connection.send c channel (make-instance 'amqp-method-channel-open))
    (let ((qd-ok (connection.send c channel (make-instance 'amqp-method-queue-declare))))
      (connection.send c channel (make-instance 'amqp-method-basic-consume :queue (amqp-method-field-queue qd-ok)))
      (connection.send c channel (make-instance 'amqp-method-basic-publish :content #b"Hello World!"
                                                :routing-key (amqp-method-field-queue qd-ok)))
      (print (amqp-method-content (read-method-for-channel c channel)))
      (connection.send c channel (make-instance 'amqp-method-channel-close :method-id 0
                                                                           :class-id 0
                                                                           :reply-code 200)))
    (connection.close :connection c)))

#++
(time (progn
  (let ((c (connection.new% 'blocking-iolib-connection (make-connection-spec "amqp://") "amqp://")))
    (connection.open c)
    (let ((channel (channel.new :connection c :channel-id 1)))
      (connection.send c channel (make-instance 'amqp-method-channel-open))
      (let ((qd-ok (connection.send c channel (make-instance 'amqp-method-queue-declare :exclusive t))))        
        (connection.send c channel (make-instance 'amqp-method-basic-consume :queue (amqp-method-field-queue qd-ok) :no-ack t))
        (loop for i from 0 to 999 do 
          (connection.send c channel (make-instance 'amqp-method-basic-publish :content #b"Hello World!"
                                                    :routing-key (amqp-method-field-queue qd-ok)))
          
          )
        ;(loop for i from 0 to 9 do
        ;  (read-method-for-channel c channel))
        (connection.send c channel (make-instance 'amqp-method-channel-close :method-id 0
                                                                           :class-id 0
                                                                           :reply-code 200)))
      (connection.close :connection c)))))

#++
(time (progn
  (let ((c (connection.new% 'blocking-iolib-connection (make-connection-spec "amqp://") "amqp://")))
    (connection.open c)
    (let ((channel (channel.new :connection c :channel-id 1)))
      (connection.send c channel (make-instance 'amqp-method-channel-open))
      (let ((qd-ok (connection.send c channel (make-instance 'amqp-method-queue-declare :exclusive t))))        
        (connection.send c channel (make-instance 'amqp-method-basic-consume :queue (amqp-method-field-queue qd-ok) :no-ack t))
        (loop for i from 0 to 9999 do 
          (connection.send c channel (make-instance 'amqp-method-basic-publish :content #b"Hello World!"
                                                    :routing-key (amqp-method-field-queue qd-ok)))
         ;; (read-method-for-channel c channel)
          )
        (connection.send c channel (make-instance 'amqp-method-channel-close :method-id 0
                                                                           :class-id 0
                                                                           :reply-code 200)))
      (connection.close :connection c)))))

#++
(time (progn
  (let ((c (connection.new% 'blocking-iolib-connection (make-connection-spec "amqp://") "amqp://")))
    (connection.open c)
    (let ((channel (channel.new :connection c :channel-id 1)))
      (connection.send c channel (make-instance 'amqp-method-channel-open))
      (let ((qd-ok (connection.send c channel (make-instance 'amqp-method-queue-declare :exclusive t))))        
        (connection.send c channel (make-instance 'amqp-method-basic-consume :queue (amqp-method-field-queue qd-ok) :no-ack t))
        (loop for i from 0 to 999 do 
          (connection.send c channel (make-instance 'amqp-method-basic-publish :content #b"Hello World!"
                                                    :routing-key (amqp-method-field-queue qd-ok)))
          (read-method-for-channel c channel)
          )
        (connection.send c channel (make-instance 'amqp-method-channel-close :method-id 0
                                                                           :class-id 0
                                                                           :reply-code 200)))
    (connection.close :connection c)))))

#++
(time (progn
  (let ((c (connection.new% 'blocking-iolib-connection (make-connection-spec "amqp://") "amqp://")))
    (connection.open c)
    (let ((channel (channel.new :connection c :channel-id 1)))
      (connection.send c channel (make-instance 'amqp-method-channel-open))
      (let ((qd-ok (connection.send c channel (make-instance 'amqp-method-queue-declare :exclusive t))))        
        (connection.send c channel (make-instance 'amqp-method-basic-consume :queue (amqp-method-field-queue qd-ok) :no-ack t))
        (loop for i from 0 to 500000 do 
          (connection.send c channel (make-instance 'amqp-method-basic-publish :content #b"Hello World!"
                                                    :routing-key (amqp-method-field-queue qd-ok)))
          
          )
        (loop for i from 0 to 99999 do
          (read-method-for-channel c channel))
        (connection.send c channel (make-instance 'amqp-method-channel-close :method-id 0
                                                                           :class-id 0
                                                                           :reply-code 200)))
    (connection.close :connection c)))))
