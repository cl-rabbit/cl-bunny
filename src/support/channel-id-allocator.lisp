(in-package :cl-bunny)

(defstruct channel-id-allocator
  (int-allocator)
  (mutex))

(defun new-channel-id-allocator (max-channels)
  (make-channel-id-allocator :int-allocator (make-int-allocator :min 1 :max max-channels)
                             :mutex (bt:make-lock "Channel Id Allocator Lock")))

(defun next-channel-id (channel-id-allocator)
  (bt:with-lock-held ((channel-id-allocator-mutex channel-id-allocator))
    (int-allocator-allocate (channel-id-allocator-int-allocator channel-id-allocator))))

(defun release-channel-id (channel-id-allocator channel-id)
  (bt:with-lock-held ((channel-id-allocator-mutex channel-id-allocator))
    (int-allocator-release (channel-id-allocator-int-allocator channel-id-allocator) channel-id)))

(defun channel-id-allocated-p (channel-id-allocator channel-id)
  (bt:with-lock-held ((channel-id-allocator-mutex channel-id-allocator))
    (int-allocator-allocated-p (channel-id-allocator-int-allocator channel-id-allocator) channel-id)))
