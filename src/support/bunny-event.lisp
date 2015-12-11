(in-package :cl-bunny)

(defparameter *event-executor* nil)

(defclass bunny-event (multi-thread-sink
                       pooled-executor)
  ())

(defmethod invoke-event-handlers ((event bunny-event) &rest args)
  (if *event-executor*
      (invoke-executor *event-executor* event args)
      (call-next-method)))
