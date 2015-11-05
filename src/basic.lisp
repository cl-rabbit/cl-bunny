(in-package :cl-bunny)

(defmethod routing-key ((routing-key string))
  routing-key)

(defmethod routing-key ((routing-key queue))
  (queue-name routing-key))

(defun publish (exchange content &key (routing-key "") (mandatory nil) (immediate nil) (channel *channel*) (properties (make-instance 'amqp-basic-class-properties)))
  (channel.send% (or (exchange-channel exchange) channel)
      (make-instance 'amqp-method-basic-publish
                     :exchange (exchange-name exchange)
                     :routing-key (routing-key routing-key)
                     :mandatory mandatory
                     :immediate immediate
                     :content content
                     :content-properties properties)
      exchange))




;; (defun hello-world-async ()
;;   (with-conneciton ("amqp://" :async t)
;;     (with-channel ()
;;       (alet ((x (exchange.default)))
;;         (chain (queue.declare :name "cl-bunny.examples.hello-world" :auto-delete t)
;;           (:attach (q)
;;                    (subscribe q (lambda (message)
;;                                   (log:info "Received ~a"
;;                                             (message-body-string message)))))
;;           (:attach ()
;;                    (publish x "Hello world!" :routing-key "cl-bunny.examples.hello-world")))))))

;; (defun hello-world-async-raw ()
;;   (let ((bunny::*connection* (connection.new "amqp://"))
;;         (channel))
;;     (finally
;;         (catcher
;;          (chain (connection.start)
;;            (:attach (_)
;;              (channel.open))
;;            (:attach (c &out (x (exchange.default c)))
;;              (setf channel c))
;;            (:attach (_)
;;              (queue.declare :name "cl-bunny.examples.hello-world" :auto-delete t :channel channel))
;;            (:attach (q)
;;              (subscribe q (lambda (message)
;;                             (log:info "Received ~a"
;;                                       (message-body-string message))
;;                             (connection.close))))
;;            (:attach (_)
;;               (publish "" "Hello world!" :routing-key "cl-bunny.examples.hello-world" :channel channel)))
;;          (t (e)
;;             (log:error "~A" e)))
;;       (when bunny::*connection*
;;         (connection.close)))))
