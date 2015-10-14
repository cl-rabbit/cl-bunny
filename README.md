# CL-BUNNY [![Build Status](https://travis-ci.org/cl-rabbit/cl-bunny.svg)](https://travis-ci.org/cl-rabbit/cl-bunny) [![Coverage Status](https://coveralls.io/repos/cl-rabbit/cl-bunny/badge.svg?branch=master&service=github)](https://coveralls.io/github/cl-rabbit/cl-bunny?branch=master)
CL-BUNNY is a RabbitMQ client. CL-BUNNY is based on [cl-rabbit](https://github.com/lokedhs/cl-rabbit) and inspired by [bunny](https://github.com/ruby-amqp/bunny).
**Please use with caution - work in progress, API is not stable, error-handling almost non-existent**. **Contributions are greatly appreciated!**

## Installation Notes
CL-BUNNY as well as some of its dependencies are not included in Quicklisp:
* [safe-queue](https://github.com/deadtrickster/safe-queue)
* [eventfd](https://github.com/deadtrickster/eventfd)

While we are working hard to merge our `cl-rabbit` patches to the main repo
please use this [branch](https://github.com/deadtrickster/cl-rabbit/tree/master1).<br>
Make sure you are using the latest version of [librabbitmq](https://github.com/alanxz/rabbitmq-c). At least Ubuntu dists usually
ship very outdated version. Just build it youself if not sure.

Also please use `master` branch of [quri](https://github.com/fukamachi/quri)
until all bug fixes not included in Quicklisp release.

If you are on sbcl and experiencing something like dsecribed [here](http://stackoverflow.com/questions/32897952/sending-messages-to-rabbit-mq-using-lisp-inside-a-docker-container)
```
CORRUPTION WARNING in SBCL pid 29643(tid 140737353938688):
Memory fault at 0x7ffff1ef80e0 (pc=0x7ffff25b48fd, sp=0x7ffff2dced00)
The integrity of this image is possibly compromised.
Continuing with fingers crossed.

debugger invoked on a SB-SYS:MEMORY-FAULT-ERROR in thread
#<THREAD "main thread" RUNNING {100504E593}>:
  Unhandled memory fault at #x7FFFF1EF80E0.
```
Try to compile sbcl with statically linked librabbitmq first.
To do this you can go to src/runtime/GNUmakefile and
make sure LINKFLAGS line looks like this: 
```
LINKFLAGS = -g  -Wl,--whole-archive <YOUR PATH TO LIBRABBITMQ>/librabbitmq.a -Wl,--no-whole-archive -lcrypto -lssl
```


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

## Copyright
Copyright (c) 2015 Ilya Khaprov <ilya.khaprov@publitechs.com> and [CONTRIBUTORS](CONTRIBUTORS.md)

CL-BUNNY uses a shared copyright model: each contributor holds copyright over their contributions to CL-BUNNY. The project versioning records all such contribution and copyright details.

If a contributor wants to further mark their specific copyright on a particular contribution, they should indicate their copyright solely in the commit message of the change when it is committed. Do not include copyright notices in files for this purpose.

## License
```
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

By contributing to the project, you agree to the license and copyright terms therein and release your contribution under these terms.
