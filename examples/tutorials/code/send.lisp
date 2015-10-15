(ql:quickload :cl-bunny.examples)

(in-package :cl-bunny.examples)

(with-connection ("amqp://")
  (with-channel ()
    (let ((x (default-exchange)))
      (queue.declare "hello")
      (publish x "Hello world!" :routing-key "hello")          
      (format t " [x] Sent 'Hello World!'~%"))))
