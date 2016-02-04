(in-package :cl-bunny)

(defclass channel-base ()
  ((connection :type connection
               :initarg :connection
               :reader channel-connection)
   (channel-id :type fixnum
               :initarg :id
               :reader channel-id)
   (state :initform :closed :accessor channel-state)
   ;; method assembler and replies
   (expected-reply :initform nil :accessor channel-expected-reply)
   ;;   (method-queue :initform (lparallel.raw-queue:make-raw-queue) :reader channel-method-queue)
   (method-assembler :initarg :method-assembler :reader channel-method-assembler)))


(defgeneric channel.receive (channel method))

(defgeneric channel-open-p% (channel)
  (:method ((channel channel-base))
    (eq (channel-state channel) :open)))

(defmethod (setf channel-open-p%) (value channel)
  (setf (channel-state channel) (if value t nil)))
