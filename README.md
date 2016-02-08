# CL-BUNNY [![Build Status](https://travis-ci.org/cl-rabbit/cl-bunny.svg?branch=master)](https://travis-ci.org/cl-rabbit/cl-bunny) [![Coverage Status](https://coveralls.io/repos/cl-rabbit/cl-bunny/badge.svg?branch=master&service=github)](https://coveralls.io/github/cl-rabbit/cl-bunny?branch=master)
CL-BUNNY is a RabbitMQ client. CL-BUNNY is based on [IOLib](https://github.com/sionescu/iolib) and inspired by [bunny](https://github.com/ruby-amqp/bunny) and [pika](https://github.com/pika/pika).
**Please use with caution - work in progress, error-handling not so descriptive, API is not always stable**. **Contributions are greatly appreciated!**

## Installation Notes
CL-BUNNY as well as some of its dependencies are not included in Quicklisp:
* [safe-queue](https://github.com/deadtrickster/safe-queue)
* [cl-events](https://github.com/deadtrickster/cl-events)

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
  (with-connection ()
    (with-channel ()
      (let ((x (exchange.default)))
        (->
          (queue.declare :name "cl-bunny.examples.hello-world" :auto-delete t)
          (subscribe (lambda (message)
                       (log:info "Received ~a"
                                 (message-body-string message)))))
        (publish x "Hello world!" :routing-key "cl-bunny.examples.hello-world"))
      (sleep 1))))
```

####  Error handling
```lisp
(with-connection ()
  (with-channel ()
    (handler-case
        (channel.open)
      (connection-closed-error () (print "Can't open already opened channel")))))
-->
<WARN> [17:10:39] bunny iolib-threaded.lisp (channel.receive threaded-iolib-connection amqp-method-connection-close) -
 Connection closed by server: AMQP-ERROR-CHANNEL-ERROR[504, CHANNEL_ERROR - second 'channel.open' seen] in response to AMQP-METHOD-CHANNEL-OPEN
"Can't open already opened channel"
"Can't open already opened channel"
```

#### Headers Exchange
```lisp
(with-connection ()
  (with-channel ()
    (let* ((x (exchange.headers "headers" :auto-delete t))
           (q1 (->
                 (queue.declare :exclusive t)
                 (queue.bind x :arguments '(("os" . "linux")
                                            ("cores" . 8)
                                            ("x-match" . "all")))))
           (q2 (->
                 (queue.declare :exclusive t)
                 (queue.bind x :arguments '(("os" . "osx")
                                            ("cores" . 4)
                                            ("x-match" . "any"))))))
      (subscribe q1 (lambda (message)
                      (log:info "~a received ~a" q1 (message-body-string message))))


      (subscribe q2 (lambda (message)
                      (log:info "~a received ~a" q2 (message-body-string message))))

      (publish x "8 cores/Linux" :properties '(:headers (("os" . "linux")
                                                       ("cores" . 8))))
      (publish x "8 cores/Linux" :properties '(:headers (("os" . "linux")
                                                       ("cores" . 8))))
      (publish x "8 cores/OS X"  :properties '(:headers (("os" . "osx")
                                                       ("cores" . 8))))
      (publish x "4 cores/Linux" :properties '(:headers (("os" . "linux")
                                                       ("cores" . 4))))

      (log:info "Waiting...")
      (sleep 3)
      (log:info "Disconnecting"))))
```
#### Mandatory messages

```lisp
(with-connection ()
  (with-channel ()
    (let* ((x (exchange.default))
           (q (queue.declare :exclusive t)))

      (setf (exchange-on-return-callback x)
            (lambda (returned-message)
              (log:info "Got returned message ~a" (message-body-string returned-message))))

      (subscribe q (lambda (message)                       
                     (log:info "~a received ~a" q (message-body-string message))))

      (publish x "This will NOT be returned" :mandatory t :routing-key q)
      (publish x "This will be returned" :mandatory t
                                         :routing-key (format nil "wefvvtrw~a" (random 10)))        

      (log:info "Waiting...")
      (sleep 3)
      (log:info "Disconnecting"))))
```

#### More?
More examples can be found [here](examples)

## Special Thanks To
* [Elias Mårtenson](https://github.com/lokedhs) for [cl-rabbit](https://github.com/lokedhs/cl-rabbit)
* [Ruby AMQP Team](https://github.com/ruby-amqp) for [bunny](https://github.com/ruby-amqp/bunny) and stuff
* [Christophe Rhodes](http://christophe.rhodes.io/) for [pipe](http://christophe.rhodes.io/notes/blog/posts/2014/code_walking_for_pipe_sequencing/) macro

## Copyright
Copyright (c) 2015,2016 Ilya Khaprov <ilya.khaprov@publitechs.com> and [CONTRIBUTORS](CONTRIBUTORS.md)

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
