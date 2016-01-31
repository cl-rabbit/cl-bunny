(in-package :cl-bunny)

(defstruct output-frame-queue
  (queue (lparallel.raw-queue:make-raw-queue 10) :type lparallel.raw-queue:raw-queue)
  (current-frame-bytes nil)
  (current-frame-position 0)
  (state :idle))

(defun get-frame-bytes (frame)
  (let ((obuffer (amqp:new-obuffer)))
    (amqp:frame-encoder frame obuffer)
    (amqp:obuffer-get-bytes obuffer)))

(defun send-queued-frames (connection of-queue)
  (unless (output-frame-queue-current-frame-bytes of-queue)
    (when (= 0 (lparallel.raw-queue:raw-queue-count of-queue))
      ;; empty queue: uninstall io-handler, set state to idle
      (iolib:remove-fd-handlers (connection-event-base connection)
                                (iolib:socket-os-fd (connection-socket connection))
                                :write t)
      (setf (output-frame-queue-state of-queue) :idle)
      (return-from send-queued-frames))
    (let* ((frame (lparallel.raw-queue:peek-raw-queue of-queue))
           (frame-bytes (get-frame-bytes frame)))
      (setf (output-frame-queue-current-frame-bytes of-queue) frame-bytes
            (output-frame-queue-current-frame-position of-queue) 0)))
  (let ((sent
          (iolib:send-to (connection-socket connection) (output-frame-queue-current-frame-bytes of-queue)
                         :start (output-frame-queue-current-frame-position of-queue))))
    (incf (output-frame-queue-current-frame-position of-queue) sent)

    (when (= (output-frame-queue-current-frame-position of-queue)
             (length (output-frame-queue-current-frame-bytes of-queue)))      
      (setf (output-frame-queue-current-frame-bytes of-queue) nil
            (output-frame-queue-current-frame-position of-queue) 0)
      (if (= 0 (lparallel.raw-queue:raw-queue-count of-queue))
          (progn
            ;; empty queue: uninstall io-handler, set state to idle
            (iolib:remove-fd-handlers (connection-event-base connection)
                                      (iolib:socket-os-fd (connection-socket connection))
                                      :write t)
            (setf (output-frame-queue-state of-queue) :idle))
          (progn
            ;; we have more frames queued
            ;; lets try to send something immediately
            (send-queued-frames connection of-queue))))))
