(in-package :cl-bunny)

(defstruct output-buffer
  (sequence)
  (index))

(defun write-sequnce-async%% (socket buffer resolve)
  (let ((sent (iolib:send-to socket (output-buffer-sequence buffer) :start (output-buffer-index buffer))))
    (incf (output-buffer-index buffer) sent)
    (when (= (output-buffer-index buffer) (length (output-buffer-sequence buffer)))
      (iolib:remove-fd-handlers *event-base* (iolib:socket-os-fd socket) :write t)
      (funcall resolve))))

(defun write-sequence-async% (cpnnection sequence index resolve)
  (let ((output-buffer (make-output-buffer :sequence sequence :index index)))
    (transport.set-io-handler connection :write
                              (lambda (fd e ex)
                                (write-sequnce-async%% socket output-buffer resolve)))))

(defun write-sequence-async (connection sequence &optional (start 0))
  (bb:with-promise (resolve reject :resolve-fn resolve-fn)
    (write-sequence-async% connection sequence start resolve-fn)))
