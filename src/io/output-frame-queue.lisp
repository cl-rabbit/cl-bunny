(in-package :cl-bunny)

;; probably introduce static buffer
;; so frames can be serialized by chunks
(defstruct output-frame-queue
  (queue (lparallel.raw-queue:make-raw-queue 10) :type lparallel.raw-queue:raw-queue)
  (current-frame-bytes nil)
  (current-frame-position 0)
  (state :idle))

(defmethod get-frame-bytes (frame)
  (let ((obuffer (amqp:new-obuffer)))
    (amqp:frame-encoder frame obuffer)
    (amqp:obuffer-get-bytes obuffer)))

(defun output-frame-queue-push (of-queue frame)
  (lparallel.raw-queue:push-raw-queue frame (output-frame-queue-queue of-queue)))

(defun output-frame-queue-count (of-queue)
  (lparallel.raw-queue:raw-queue-count (output-frame-queue-queue of-queue)))

(defun output-frame-queue-pop (of-queue)
  (lparallel.raw-queue:pop-raw-queue (output-frame-queue-queue of-queue)))

(defun send-queued-frames (connection of-queue)
  (log:debug "send-queued-frames")
  (unless (output-frame-queue-current-frame-bytes of-queue)
    (when (= 0 (output-frame-queue-count of-queue))
      ;; empty queue: uninstall io-handler, set state to idle
      (iolib:remove-fd-handlers (connection-event-base connection)
                                (iolib:socket-os-fd (connection-socket connection))
                                :write t)
      (setf (output-frame-queue-state of-queue) :idle)
      (return-from send-queued-frames))
    (let* ((frame (output-frame-queue-pop of-queue))
           (frame-bytes (get-frame-bytes frame)))
      (setf (output-frame-queue-current-frame-bytes of-queue) frame-bytes
            (output-frame-queue-current-frame-position of-queue) 0)))
  (send-to connection (output-frame-queue-current-frame-bytes of-queue)
           (lambda (sent)
             (incf (output-frame-queue-current-frame-position of-queue) sent)

             (when (= (output-frame-queue-current-frame-position of-queue)
                      (length (output-frame-queue-current-frame-bytes of-queue)))
               (setf (output-frame-queue-current-frame-bytes of-queue) nil
                     (output-frame-queue-current-frame-position of-queue) 0)
               (if (= 0 (output-frame-queue-count of-queue))
                   (progn
                     ;; empty queue: uninstall io-handler, set state to idle
                     (log:debug "Removing :write io handler")
                     (transport.remove-writer connection)
                     (setf (output-frame-queue-state of-queue) :idle))
                   (progn
                     ;; we have more frames queued
                     ;; lets try to send something immediately
                     (send-queued-frames connection of-queue)))))
           :start (output-frame-queue-current-frame-position of-queue)))
