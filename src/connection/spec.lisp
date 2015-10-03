(in-package :cl-bunny)

(defstruct (connection-spec (:constructor make-connection-spec%))
  (host "localhost" :type string)
  (port 5672 :type fixnum)
  (vhost "/" :type string)
  (login "guest" :type string)
  (password "guest" :type string))


(defmethod make-connection-spec ((raw list)))

(defun check-connection-string-scheme (scheme)
  (or (equal scheme "amqp")
      (equal scheme "amqps")))

(defun check-connection-string-host (host)
  (or host "localhost"))

(defun check-connection-string-port (port)
  (or port 5672))

(defun check-connection-string-vhost (vhost)
  (or vhost "/"))

(defun check-connection-string-credentials (userinfo)
  '("guest" "guest"))

;; see https://www.rabbitmq.com/uri-spec.html
(defmethod make-connection-spec ((raw string))
  (multiple-value-bind (scheme userinfo host port path query fragment)
      (quri:parse-uri raw)
    (declare (ignore query fragment))
    (check-connection-string-scheme scheme)
    (let ((host (check-connection-string-host host))
          (port (check-connection-string-port port))
          (credentials (check-connection-string-credentials userinfo))
          (vhost (check-connection-string-vhost path)))
      (make-connection-spec% :host host
                             :port port
                             :vhost vhost
                             :login (first credentials)
                             :password (second credentials)))))
