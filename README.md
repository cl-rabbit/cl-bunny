# CL-BUNNY
CL-BUNNY is a RabbitMQ client. CL-BUNNY is based on [cl-rabbit](https://github.com/lokedhs/cl-rabbit) and inspired by [bunny](https://github.com/ruby-amqp/bunny).
**Please use with caution - work in progress, API is not stable, error-handling almost non-existent**. **Contributions are welcome!**

## Installation Notes
CL-BUNNY as well as some of its dependencies are not included in Quicklisp:
* [safe-queue](https://github.com/deadtrickster/safe-queue)
* [eventfd](https://github.com/deadtrickster/eventfd)

While are working hard to merge our `cl-rabbit` patches to the main repo
please use this [branch](https://github.com/deadtrickster/cl-rabbit/tree/master1).

Also please use `master` branch of [quri](https://github.com/fukamachi/quri)
until all bug fixes not included in Quicklisp release.



## Examples
#### Foreword
To run examples you need to have RabbitMQ installed on `localhost` with
* vhost `/`
* user named `guest` with password `guest`.

If you are new to RabbitMQ you may find the following links useful:
* [Install on Debian/Ubuntu](https://www.rabbitmq.com/install-debian.html)
* [Access Control](https://www.rabbitmq.com/access-control.html)
* [RabbitMQ Management plugin](https://www.rabbitmq.com/management.html)


#### Hello World!

```lisp
(defun hello-world ()
  (with-connection ("amqp://" :one-shot t)
    (with-channel ()
      (let ((x (default-exchange)))
        (->
          (queue.declare "cl-bunny.examples.hello-world" :auto-delete t)
          (subscribe (lambda (message)
                       (log:info "Received ~a"
                                 (babel:octets-to-string (message-body message))))))
        (publish x "Hello world!" :routing-key "cl-bunny.examples.hello-world"))
      (sleep 1))))
```
#### Headers exchange (sugar-free)

```lisp
(with-connection ("amqp://" :one-shot t)
  (with-channel ()
    (let ((x (amqp-exchange-declare "headers" :type "headers"))
          (q1 (amqp-queue-declare "" :exclusive t))
          (q2 (amqp-queue-declare "" :exclusive t)))
      (amqp-queue-bind q1 :exchange x :arguments '(("os" . "linux")
                                                   ("cores" . 8)
                                                   ("x-match" . "all")))
      (amqp-queue-bind q2 :exchange x :arguments '(("os" . "osx")
                                                   ("cores" . 4)
                                                   ("x-match" . "any")))

      (subscribe q1 (lambda (message)
                      (log:info "~a received ~a"
                                q1 (babel:octets-to-string (message-body message)))))
      (subscribe q2 (lambda (message)
                      (log:info "~a received ~a"
                                q2 (babel:octets-to-string (message-body message)))))

      (amqp-basic-publish "8 cores/Linux" :exchange x
                                          :properties '((:headers . (("os" . "linux")
                                                                     ("cores" . 8)))))
      (amqp-basic-publish "8 cores/OS X"  :exchange x
                                          :properties '((:headers . (("os" . "osx")
                                                                     ("cores" . 8)))))
      (amqp-basic-publish "4 cores/Linux" :exchange x
                                          :properties '((:headers . (("os" . "linux")
                                                                     ("cores" . 4)))))
      (log:info "Waiting...")
      (sleep 3)
      (log:info "Disconnecting"))))
```

#### Headers Exchange
```lisp
(with-connection ("amqp://" :one-shot t)
  (with-channel ()
    (let* ((x (headers-exchange "headers" :auto-delete t))
           (q1 (->
                 (queue.declare "" :exclusive t)
                 (queue.bind x :arguments '(("os" . "linux")
                                            ("cores" . 8)
                                            ("x-match" . "all")))))
           (q2 (->
                 (queue.declare "" :exclusive t)
                 (queue.bind x :arguments '(("os" . "osx")
                                            ("cores" . 4)
                                            ("x-match" . "any"))))))
      (subscribe q1 (lambda (message)
                      (log:info "~a received ~a"
                                q1 (babel:octets-to-string (message-body message)))))

      (subscribe q2 (lambda (message)
                      (log:info "~a received ~a"
                                q2 (babel:octets-to-string (message-body message)))))

      (->
        x
        (publish "8 cores/Linux" :properties '((:headers . (("os" . "linux")
                                                            ("cores" . 8)))))
        (publish "8 cores/OS X"  :properties '((:headers . (("os" . "osx")
                                                            ("cores" . 8)))))
        (publish "4 cores/Linux" :properties '((:headers . (("os" . "linux")
                                                            ("cores" . 4))))))
      (log:info "Waiting...")
      (sleep 3)
      (log:info "Disconnecting"))))
```

#### More?
More examples including can be found [here](examples)

## Special Thanks To
* [Elias Mårtenson](https://github.com/lokedhs) for [cl-rabbit](https://github.com/lokedhs/cl-rabbit)
* [Ruby AMQP Team](https://github.com/ruby-amqp) for [bunny](https://github.com/ruby-amqp/bunny) and stuff
* [Christophe Rhodes](http://christophe.rhodes.io/) for [pipe](http://christophe.rhodes.io/notes/blog/posts/2014/code_walking_for_pipe_sequencing/) macro

## Authors
* Ilya Khaprov <ilya.khaprov@publitechs.com>

## Copyright
Copyright (c) 2015 Ilya Khaprov <ilya.khaprov@publitechs.com>

## License
Released under MIT license
