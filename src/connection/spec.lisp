(in-package :cl-bunny)

(defstruct (connection-spec (:constructor make-connection-spec%))
  (host "localhost" :type string)
  (port 5672 :type fixnum)
  (vhost "/" :type string)
  (login "guest" :type string)
  (password "guest" :type string)
  (use-tls-p nil :type boolean))

(defmethod make-connection-spec ((raw list))
  (error "Not implemented"))

(defun maybe-unescape-component (value)
  (when value
    (quri:url-decode value)))

(defun parse-user-info (userinfo)
  (destructuring-bind (login &optional (password ""))
      (split-sequence:split-sequence #\: userinfo)
    (list (quri:url-decode login) (quri:url-decode password))))

(defun check-connection-string-scheme (scheme)
  (assert (or (equal scheme "amqp")
              (equal scheme "amqps")))
  (if (equal scheme "amqps")
      t))

(defun check-connection-string-host (host)
  (or (maybe-unescape-component host) "localhost"))

(defun check-connection-string-port (scheme port)
  (or port (if (equal scheme "amqp")
               5672
               5671)))

(defun check-vhost-single-segment (vhost)
  (and vhost
       (if (find #\/ vhost :start 1)
           (error "Multi-segment vhost") ;; TODO: specialize error
          (subseq vhost 1))))

(defun check-connection-string-vhost (vhost)
  (or (maybe-unescape-component (check-vhost-single-segment vhost)) "/"))

(defun check-connection-string-credentials (userinfo)
  (cond
    ((or (null userinfo)
         (equal "" userinfo))
     '("guest" "guest"))
    ((stringp userinfo)
     (parse-user-info userinfo))))

;; see https://www.rabbitmq.com/uri-spec.html
(defmethod make-connection-spec ((raw string))
  (multiple-value-bind (scheme userinfo host port path query fragment)
      (quri:parse-uri raw)
    (declare (ignore query fragment))
    (let ((use-tls (check-connection-string-scheme scheme))
          (host (check-connection-string-host host))
          (port (check-connection-string-port scheme port))
          (credentials (check-connection-string-credentials userinfo))
          (vhost (check-connection-string-vhost path)))
      (make-connection-spec% :use-tls-p use-tls
                             :host host
                             :port port
                             :vhost vhost
                             :login (first credentials)
                             :password (second credentials)))))

(defmethod make-connection-spec ((raw (eql nil)))
  (make-connection-spec "amqp://"))
