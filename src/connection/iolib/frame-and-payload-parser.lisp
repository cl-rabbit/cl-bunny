(in-package :cl-bunny)

(defstruct fap-parser
  (buffer)
  (index)
  (frame)
  (parser)
  (payload-parser))
