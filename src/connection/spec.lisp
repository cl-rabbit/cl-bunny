(in-package :cl-bunny)

(defstruct (connection-spec (:constructor make-connection-spec%))
  (host "localhost" :type string)
  (port 5672 :type fixnum)
  (vhost "/" :type string)
  (login "guest" :type string)
  (password "guest" :type string)
  (use-tls-p nil :type boolean)
  (use-ipv6-p nil :type boolean))

(defun render-scheme (spec stream)
  (princ (if (connection-spec-use-tls-p spec)
             "amqps://"
             "amqp://")
         stream))

(defun render-userinfo (spec stream)
  (unless (and (equal "guest" (connection-spec-login spec))
               (equal "guest" (connection-spec-password spec)))
    (princ (quri:url-encode (connection-spec-login spec)) stream)
    (unless (equal "guest" (connection-spec-password spec))
      (format stream ":~a" (quri:url-encode (connection-spec-password spec))))
    (princ "@" stream)))

(defun render-host (spec stream)
  (unless (equal (connection-spec-host spec) "localhost")
    (if (connection-spec-use-ipv6-p spec)
        (format stream "[~a]" (connection-spec-host spec))
        (princ (quri:url-encode (connection-spec-host spec)) stream))))

(defun render-port (spec stream)
  (unless (or
           (and (not (connection-spec-use-tls-p spec))
                (= 5672 (connection-spec-port spec)))
           (and (connection-spec-use-tls-p spec)
                (= 5671 (connection-spec-port spec))))
    (format stream ":~a" (connection-spec-port spec))))

(defun render-vhost (spec stream)
  (cond
    ((equal "" (connection-spec-vhost spec))
     (princ "/" stream))
    ((equal "/" (connection-spec-vhost spec)))
    (t (format stream "/~a" (quri:url-encode (connection-spec-vhost spec))))))

(defun actually-print-connection-spec (spec stream)  
  (render-scheme spec stream)
  (render-userinfo spec stream)
  (render-host spec stream)
  (render-port spec stream)
  (render-vhost spec stream))

(defmethod print-object ((spec connection-spec) stream)
  (print-unreadable-object (spec stream :type t :identity t)
    (actually-print-connection-spec spec stream)))

(defmethod make-connection-spec ((raw list))
  (error "Not implemented"))

(defun maybe-unescape-component (value)
  (when value
    (quri:url-decode value)))

(defun parse-user-info (userinfo)
  (destructuring-bind (login &optional (password "guest"))
      (split-sequence:split-sequence #\: userinfo)
    (list (maybe-unescape-component login) (maybe-unescape-component password))))

(defun check-connection-string-scheme (scheme)
  (assert (or (equal scheme "amqp")
              (equal scheme "amqps")))
  (if (equal scheme "amqps")
      t))

(defun validate-host (host)
  (if (and (starts-with #\[ host)
           (ends-with #\] host))
      (let ((ipv6-address (subseq host 1 (1- (length host)))))
        (if (iolib:ensure-address ipv6-address :errorp nil)
          (values ipv6-address t)
          (error "Invalid IPv6 address ~a" ipv6-address))) ;; TODO: specialize error))
      (maybe-unescape-component host)))

(defun check-connection-string-host (host)
  (if host
      (validate-host host)
      "localhost"))

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
          (port (check-connection-string-port scheme port))
          (credentials (check-connection-string-credentials userinfo))
          (vhost (check-connection-string-vhost path)))
      (multiple-value-bind (host ipv6) (check-connection-string-host host)
        (make-connection-spec% :use-tls-p use-tls
                               :use-ipv6-p ipv6
                               :host host
                               :port port
                               :vhost vhost
                               :login (first credentials)
                               :password (second credentials))))))

(defmethod make-connection-spec ((raw (eql nil)))
  (make-connection-spec "amqp://"))
