(in-package :cl-bunny.test)

(plan 2)

(defmethod channel-send (channel (method (eql :ok)))
  (blackbird:with-promise (resolve reject)
    (resolve 1 2 3)))

(define-condition channel-send-test-error (error)
  ())

(defmethod channel-send (channel (method (eql :error)))
  (blackbird:with-promise (resolve reject)
    (reject (error 'channel-send-test-error))))

(subtest "Sync channel-send"
  (bunny:with-connection ("amap://" :one-shot t)
    (bunny:with-channel ()
      (is-values (channel-send bunny::*channel* :ok)
                 '(1 2 3))
      (is-error (channel-send bunny::*channel* :error)
                'channel-send-test-error))))

(subtest "Async channel-send"
  (let ((vals)
        (e))
    (bunny:with-connection ("amap://" :one-shot t)
      (bunny:with-channel ()
        (let ((channel bunny::*channel*))
          (bunny::execute-in-connection-thread (bunny::*connection*)
            (attach (channel-send channel :ok)
                    (lambda (&rest vals%)
                      (setf vals vals%)))
            (catcher (channel-send channel :error)
                     (t (e%)
                        (setf e e%)))))))
    (is vals '(1 2 3))
    (is-error e 'channel-send-test-error)))

(finalize)
