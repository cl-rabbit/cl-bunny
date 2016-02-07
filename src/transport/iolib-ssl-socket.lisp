(in-package :iolib.sockets)

(cl-interpol:enable-interpol-syntax)

(define-condition ssl-error-want-read ()
  ())

(define-condition ssl-error-want-write ()
  ())

(define-condition ssl-error-zero-return ()
  ())

(let ((ssl-ctx))
  (defun ssl-context ()
    (or ssl-ctx
        (setf ssl-ctx (cl+ssl:make-context :verify-mode cl+ssl:+SSL-VERIFY-NONE+)))))

(defclass ssl-socket (stream-socket internet-socket dual-channel-fd-mixin)
  ((ctx :accessor ssl-socket-ctx)
   (iolib.streams::fd :initform nil :initarg :fd :accessor iolib.streams::fd-of
                      :documentation "placeholder")))

(defmethod connect ((socket ssl-socket) address
                    &key (port 0) (wait t))
  (declare (ignore address port))
  (assert wait nil "non blocking on ssl socket not supported yet")
  (setf (ssl-socket-ctx socket) (cl+ssl::ssl-new (ssl-context)))
  (call-next-method)
  (cl+ssl::ssl-set-fd (ssl-socket-ctx socket) (socket-os-fd socket))
  ;;  (setf (iolib.streams::read-fn-of socket (make-ssl-socket-read-fn socket)))
  ;;  (setf (iolib.streams::write-fn-of socket (make-ssl-socket-write-fn socket)))
  (tagbody
   :start
     (let ((reply (cl+ssl::ssl-connect (ssl-socket-ctx socket))))
       (if (not (= reply 1))
           (let ((error (cl+ssl::ssl-get-error (ssl-socket-ctx socket) reply)))
             (case error
               (#.cl+ssl::+ssl-error-want-read+
                (go :wait-for-read-and-retry))
               (#.cl+ssl::+ssl-error-want-write+
                (go :wait-for-write-and-retry))
               (t
                (close socket :abort t)
                (cl+ssl::ssl-signal-error (ssl-socket-ctx socket) "ssl-connection" error reply))))
           (go :exit)))
   :wait-for-read-and-retry
     (iomux:wait-until-fd-ready (socket-os-fd socket) :input)
     (go :start)
   :wait-for-write-and-retry
     (iomux:wait-until-fd-ready (socket-os-fd socket) :output)
     (go :start)
   :exit
     ;; (let ((err (cl+ssl::ssl-get-verify-result (ssl-socket-ctx socket))))
     ;;   (unless (= err 0)
     ;;     (error 'cl+ssl:ssl-error-verify :stream socket :error-code err)))
     )
  socket)



(defmethod close ((socket ssl-socket) &key abort)
  (unless abort
    (cl+ssl::ssl-shutdown (ssl-socket-ctx socket)))
  (cl+ssl::ssl-free (ssl-socket-ctx socket))
  (setf (ssl-socket-ctx socket) nil)
  (call-next-method))


(defmethod receive-from ((socket ssl-socket) &key buffer (start 0) (end (length buffer)) flags)
  (declare (ignore flags))
  (let ((nbytes (with-pointer-to-vector-data (ptr buffer)
                  (unless (= 0 start)
                    (cffi:incf-pointer ptr start))
                  (cl+ssl::ssl-read (ssl-socket-ctx socket) ptr (or end (length buffer))))))
    (if (plusp nbytes)
        (values buffer nbytes)
        (let ((error (cl+ssl::ssl-get-error (ssl-socket-ctx socket) nbytes)))
          (case error
            (#.cl+ssl::+ssl-error-want-read+
             (error 'ssl-error-want-read))
            (#.cl+ssl::+ssl-error-want-write+
             (error 'ssl-error-want-write))
            (#.cl+ssl::+ssl-error-zero-return+
             (error 'ssl-error-zero-return))
            (t
             (cl+ssl::ssl-signal-error (ssl-socket-ctx socket) "ssl-read" error nbytes)))))))

(defmethod send-to ((socket ssl-socket) buffer &rest args
                                               &key (start 0) (end (length buffer)))

  (declare (ignore args))
  (let ((nbytes (with-pointer-to-vector-data (ptr buffer)
                  (when (and start (not (= 0 start)))
                    (cffi:incf-pointer ptr start))
                  (cl+ssl::ssl-write (ssl-socket-ctx socket) ptr (or end (length buffer))))))
    (if (plusp nbytes)
        nbytes
        (let ((error (cl+ssl::ssl-get-error (ssl-socket-ctx socket) nbytes)))
          (case error
            (#.cl+ssl::+ssl-error-want-read+
             (error 'ssl-error-want-read))
            (#.cl+ssl::+ssl-error-want-write+
             (error 'ssl-error-want-write))
            (#.cl+ssl::+ssl-error-zero-return+
             (error 'ssl-error-zero-return))
            (t
             (cl+ssl::ssl-signal-error (ssl-socket-ctx socket) "ssl-write" error nbytes)))))))
