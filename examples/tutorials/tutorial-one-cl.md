<!--
Copyright (C) 2007-2015 Pivotal Software, Inc. 

All rights reserved. This program and the accompanying materials
are made available under the terms of the under the Apache License, 
Version 2.0 (the "License”); you may not use this file except in compliance 
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# RabbitMQ tutorial - "Hello World!"

## Introduction

RabbitMQ is a message broker. In essence, it accepts messages from
_producers_, and delivers them to _consumers_. In-between, it can
route, buffer, and persist the messages according to rules you give
it.

RabbitMQ, and messaging in general, uses some jargon.

 * _Producing_ means nothing more than sending. A program that sends messages
   is a _producer_. We'll draw it like that, with "P":

   ![P](http://i.imgur.com/mPTaxSl.png)

 * _A queue_ is the name for a mailbox. It lives inside
   RabbitMQ. Although messages flow through RabbitMQ and your
   applications, they can be stored only inside a _queue_. A _queue_
   is not bound by any limits, it can store as many messages as you
   like - it's essentially an infinite buffer. Many _producers_ can send
   messages that go to one queue - many _consumers_ can try to
   receive data from one _queue_. A queue will be drawn like this, with
   its name above it:

   ![{||||}](http://i.imgur.com/6dDlW1g.png)

 * _Consuming_ has a similar meaning to receiving. A _consumer_ is a program
   that mostly waits to receive messages. On our drawings it's shown with "C":
   
   ![C](http://i.imgur.com/U9mJa0I.png)

Note that the producer, consumer, and  broker do not have to reside on
the same machine; indeed in most applications they don't.

## "Hello World"
### (using the cl-bunny Common Lisp Client)

In this part of the tutorial we'll write two small programs in Lisp; a
producer that sends a single message, and a consumer that receives
messages and prints them out.  We'll gloss over some of the detail in
the [cl-bunny](http://cl-rabbit.io/cl-bunny) API, concentrating on this very simple thing just to get
started. It's a "Hello World" of messaging.

In the diagram below, "P" is our producer and "C" is our consumer. The
box in the middle is a queue - a message buffer that RabbitMQ keeps
on behalf of the consumer.

![(P) -> [|||] -> (C)](http://i.imgur.com/VjRoMDS.png)

> #### The cl-bunny client library
>
> RabbitMQ speaks AMQP 0.9.1, which is an open,
> general-purpose protocol for messaging. There are a number of clients
> for RabbitMQ in [many different
> languages](/devtools.html). We'll
> use the cl-bunny client in this tutorial.
>
> First, install cl-bunny using [official webpage](http://cl-rabbit.io/cl-bunny):
>
>     :::bash
>     $ gem install bunny --version ">= 1.6.0"
>

Now we have cl-bunny installed, we can write some
code.

### Sending

![(P) -> [|||]](http://i.imgur.com/ph26szM.png)

We'll call our message sender `send.lisp` and our message receiver
`receive.lisp`.  The sender will connect to RabbitMQ, send a single message,
then exit.

In
[`send.lisp`](code/send.lisp),
we need to require the library first:

    :::lisp

    (ql:quickload 'cl-bunny)

then connect to RabbitMQ server

    :::lisp
    (with-connection ("amqp://")
      ...
    )

The connection abstracts the socket connection, and takes care of
protocol version negotiation and authentication and so on for us. Here
we connect to a broker on the local machine with all default settings.

If we wanted to connect to a broker on a different
machine we'd simply specify its name or IP address using the `:hostname`
option:

    :::lisp
    (with-connection ("amqp://rabbit.local")
      ...
    )

Next we create a channel, which is where most of the API for getting
things done resides:

    :::lisp
    (with-channel ()
      ...
    )

To send, we must declare a queue for us to send to; then we can publish a message
to the queue:

    :::lisp
	(with-connection ("amqp://")
	  (with-channel ()
	    (let ((x (default-exchange)))
	      (publish x "Hello world!" :routing-key "hello")          
	      (format t " [x] Sent 'Hello World!'~%"))))

Declaring a queue is idempotent - it will only be created if it doesn't
exist already. The message content is a byte array, so you can encode
whatever you like there.

Lastly, we close the connection;

Thanks to `with-connection` we don't have to close connection. It does this automatically

[Here's the whole send.lisp script](code/send.lisp).

> #### Sending doesn't work!
>
> If this is your first time using RabbitMQ and you don't see the "Sent"
> message then you may be left scratching your head wondering what could
> be wrong. Maybe the broker was started without enough free disk space
> (by default it needs at least 1Gb free) and is therefore refusing to
> accept messages. Check the broker logfile to confirm and reduce the
> limit if necessary. The <a
> href="http://www.rabbitmq.com/configure.html#config-items">configuration
> file documentation</a> will show you how to set `disk_free_limit`.


### Receiving

That's it for our sender.  Our receiver is pushed messages from
RabbitMQ, so unlike the sender which publishes a single message, we'll
keep it running to listen for messages and print them out.

![[|||] -> (C)](http://i.imgur.com/3teOytn.png)

The code (in [`receive.lisp`](code/receive.lisp)) has the same require as `send.lisp`:

    :::lisp
    (ql:quickload 'cl-bunny)


Setting up is the same as the sender; we open a connection and a
channel, and declare the queue from which we're going to consume.
Note this matches up with the queue that `send` publishes to.

    :::lisp
    (with-connection ("amqp://")
      (with-channel ()
        (let ((x (default-exchange)))          
          (format t " [*] Waiting for messages in queue 'hello'. To exit type (exit)~%")
          (queue.declare "hello" :auto-delete t)
          ...
    ))


Note that we declare the queue here, as well. Because we might start
the receiver before the sender, we want to make sure the queue exists
before we try to consume messages from it.

We're about to tell the server to deliver us the messages from the
queue. Since it will push us messages asynchronously, we provide a
callback that will be executed when RabbitMQ pushes messages to
our consumer. This is what `subscribe` does.

    :::lisp
	(with-connection ("amqp://")
	  (with-channel ()
	    (let ((q (queue.declare "hello" :auto-delete t)))
	      (format t " [*] Waiting for messages in queue 'hello'. To exit type (exit)~%")
	      (subscribe q (lambda (message)
	                     (let ((body (babel:octets-to-string (message-body message))))
	                       (format t " [x] Received ~a~%" body)))
	                 :type :sync)
	      (consume :one-shot t))))

`subscribe` is used with the `:type :sync` option that makes it
block the calling thread (we don't want the script to finish running immediately!).

[Here's the whole receive.lisp script](code/receive.lisp).

### Putting it all together

Now we can run both scripts. In a terminal, run the sender:

    :::bash
    $ sbcl --non-interactive --load send.lisp

then, run the receiver:

    :::bash
    $ sbcl --non-interactive --load receive.lisp

The receiver will print the message it gets from the sender via
RabbitMQ. The receiver will keep running, waiting for messages (Use function `cl-user::exit` to stop it), so try running
the sender from another terminal.

If you want to check on the queue, try using `rabbitmqctl list_queues`.

Hello World!

Time to move on to [part 2](tutorial-two-cl.md) and build a simple _work queue_.
