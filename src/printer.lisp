(in-package :cl-bunny)

(defgeneric print-amqp-object (object stream))

(defmethod print-amqp-object ((message message) stream)
  (format stream "~@[consumer-tag=~a ~]delivery-tag=~a" (message-consumer-tag message) (message-delivery-tag message)))

(defmethod print-object ((message message) stream)
  (print-unreadable-object (message stream :type t :identity t)
    (print-amqp-object message stream)))

(defmethod print-object ((queue queue) s)
  (print-unreadable-object (queue s :type t :identity t)
    (format s "~s ~@[exclusive~]" (queue-name queue) (queue-exclusive-p queue))))

(defmethod print-object ((exchange exchange) s)
  (print-unreadable-object (exchange s :type t :identity t)
    (format s "~s" (exchange-name exchange))))

(defmethod print-object ((consumer consumer) s)
  (print-unreadable-object (consumer s :type t :identity t)
    (format s "~s queue=~s" (consumer-tag consumer) (queue-name (consumer-queue consumer)))))

(defmethod print-object ((channel channel) s)
  (print-unreadable-object (channel s :type t :identity t)
    (format s "~:[closed~;open~] id=~a mode=~(~a~) consumers=~a" (channel-open-p channel) (channel-id channel) (channel-mode channel) (hash-table-count (channel-consumers channel)))))

(defmethod print-object ((connection connection) s)
  (print-unreadable-object (connection s :type t :identity t)
    (format s "~:[closed~;open~] spec=\"~A\" channels=~a" (connection-open-p connection) (connection-spec connection) (hash-table-count (connection-channels connection)))))
