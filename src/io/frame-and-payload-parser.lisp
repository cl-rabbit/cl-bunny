(in-package :cl-bunny)

(defstruct (fap-parser (:constructor make-fap-parser%))
  (buffer)
  (buffer-index)
  (end-index)
  (frame)
  (parser)
  (payload-parser))

(defun make-fap-parser ()
  (let ((fap-parser (make-fap-parser% :buffer (nibbles:make-octet-vector 4096)
                                      :buffer-index 0
                                      :end-index 0)))
    (setf (fap-parser-parser fap-parser)
          (amqp:make-frame-parser
           :on-frame-type (lambda (parser frame-type)
                            (declare (ignore parser))
                            (setf (fap-parser-frame fap-parser) (make-instance (amqp:frame-class-from-frame-type frame-type))))
           :on-frame-channel (lambda (parser frame-channel)
                               (declare (ignore parser))
                               (setf (amqp:frame-channel (fap-parser-frame fap-parser)) frame-channel))
           :on-frame-payload-size (lambda (parser payload-size)
                                    (declare (ignore parser))
                                    ;; TODO validate frame size
                                    (unless (= +amqp-frame-heartbeat+ (amqp::frame-type (fap-parser-frame fap-parser)))
                                      (setf (amqp:frame-payload-size (fap-parser-frame fap-parser)) payload-size
                                            (fap-parser-payload-parser fap-parser) (amqp:make-frame-payload-parser (fap-parser-frame fap-parser)))))
           :on-frame-payload (lambda (parser data start end)
                               (declare (ignore parser))
                               (when (fap-parser-payload-parser fap-parser)
                                 (amqp:frame-payload-parser-consume (fap-parser-payload-parser fap-parser) data :start start :end end)))
           :on-frame-end (lambda (parser)
                           (declare (ignore parser))
                           (when (fap-parser-payload-parser fap-parser)
                             (amqp:frame-payload-parser-finish (fap-parser-payload-parser fap-parser)))
                           ;;(setf frame-ended t)
                           )))
    fap-parser))

(defun fap-parser-advance-end (fap-parser read)  
  (incf (fap-parser-end-index fap-parser) read))

(defun fap-parser-advance (fap-parser)
  (unless (= (fap-parser-end-index fap-parser)
             (fap-parser-buffer-index fap-parser))
    (multiple-value-bind (read-buffer-index parsed)
        (frame-parser-consume (fap-parser-parser fap-parser)
                              (fap-parser-buffer fap-parser)
                              :start (fap-parser-buffer-index fap-parser)
                              :end (fap-parser-end-index fap-parser))
      (if parsed
          (progn            
            (setf (fap-parser-buffer-index fap-parser) read-buffer-index)
            (when (= 4096 (fap-parser-end-index fap-parser) (fap-parser-buffer-index fap-parser))
              (setf (fap-parser-end-index fap-parser) 0
                    (fap-parser-buffer-index fap-parser) 0))
            (fap-parser-frame fap-parser))
          (progn (setf (fap-parser-end-index fap-parser) 0
                       (fap-parser-buffer-index fap-parser) 0)
                 nil)))))
