(in-package :cl-bunny)

(defstruct int-allocator
  (min)
  (max))

(defun int-allocator-allocate (int-allocator)
  (let ((allocated (random (int-allocator-max int-allocator))))
    (if (< allocated (int-allocator-min int-allocator))
        (int-allocator-allocate int-allocator)
        allocated)))

(defun int-allocator-release (int-allocator allocated))

(defun int-allocator-allocated-p (int-allocator value)
  t)
