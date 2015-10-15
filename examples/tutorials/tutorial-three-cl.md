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
# RabbitMQ tutorial - Publish/Subscribe SUPPRESS-RHS

## Publish/Subscribe
### (using [cl-bunny](http://cl-rabbit.io/cl-bunny))

In the [previous tutorial](tutorial-two-cl.md) we created a work
queue. The assumption behind a work queue is that each task is
delivered to exactly one worker. In this part we'll do something
completely different -- we'll deliver a message to multiple
consumers. This pattern is known as "publish/subscribe".

To illustrate the pattern, we're going to build a simple logging
system. It will consist of two programs -- the first will emit log
messages and the second will receive and print them.

In our logging system every running copy of the receiver program will
get the messages. That way we'll be able to run one receiver and
direct the logs to disk; and at the same time we'll be able to run
another receiver and see the logs on the screen.

Essentially, published log messages are going to be broadcast to all
the receivers.

Exchanges
---------

In previous parts of the tutorial we sent and received messages to and
from a queue. Now it's time to introduce the full messaging model in
Rabbit.

Let's quickly go over what we covered in the previous tutorials:

 * A _producer_ is a user application that sends messages.
 * A _queue_ is a buffer that stores messages.
 * A _consumer_ is a user application that receives messages.

The core idea in the messaging model in RabbitMQ is that the producer
never sends any messages directly to a queue. Actually, quite often
the producer doesn't even know if a message will be delivered to any
queue at all.

Instead, the producer can only send messages to an _exchange_. An
exchange is a very simple thing. On one side it receives messages from
producers and the other side it pushes them to queues. The exchange
must know exactly what to do with a message it receives. Should it be
appended to a particular queue? Should it be appended to many queues?
Or should it get discarded. The rules for that are defined by the
_exchange type_.

![exchange](http://i.imgur.com/zsxeGrA.png)

There are a few exchange types available: `direct`, `topic`, `headers`
and `fanout`. We'll focus on the last one -- the fanout. Let's create
an exchange of this type, and call it `logs`:

    :::lisp
    (amqp-exchange-declare "logs" :type "fanout")


The fanout exchange is very simple. As you can probably guess from the
name, it just broadcasts all the messages it receives to all the
queues it knows. And that's exactly what we need for our logger.


> #### Listing exchanges
>
> To list the exchanges on the server you can run the ever useful `rabbitmqctl`:
>
>     $ sudo rabbitmqctl list_exchanges
>     Listing exchanges ...
>             direct
>     amq.direct      direct
>     amq.fanout      fanout
>     amq.headers     headers
>     amq.match       headers
>     amq.rabbitmq.log        topic
>     amq.rabbitmq.trace      topic
>     amq.topic       topic
>     logs    fanout
>     ...done.
>
> In this list there are some `amq.*` exchanges and the default (unnamed)
> exchange. These are created by default, but it is unlikely you'll need to
> use them at the moment.


> #### Nameless exchange
>
> In previous parts of the tutorial we knew nothing about exchanges,
> but still were able to send messages to queues. That was possible
> because we were using a default exchange, which is identified by the empty string (`""`).
>
> Recall how we published a message before:
>
>	  :::lisp
>	  (publish (default_exchange) "hello" :routing-key "hello")
>
> Here we use the default or _nameless_ exchange: messages are
> routed to the queue with the name specified by `:routing-key`, if it exists.

Now, we can publish to our named exchange instead:

    :::lisp
    (let ((x (amqp-exchange-declare "logs" :type "fanout")))
	    (publish x msg))


Temporary queues
----------------

As you may remember previously we were using queues which had a
specified name (remember `hello` and `task_queue`?). Being able to name
a queue was crucial for us -- we needed to point the workers to the
same queue.  Giving a queue a name is important when you
want to share the queue between producers and consumers.

But that's not the case for our logger. We want to hear about all
log messages, not just a subset of them. We're
also interested only in currently flowing messages not in the old
ones. To solve that we need two things.

Firstly, whenever we connect to Rabbit we need a fresh, empty queue.
To do this we could create a queue with a random name, or,
even better - let the server choose a random queue name for us.

Secondly, once we disconnect the consumer the queue should be
automatically deleted.

In the [cl-bunny](http://cl-rabbit.io/cl-bunny) client, when we supply queue name
as an empty string, we create a non-durable queue with a generated name:

    :::lisp
    (queue.declare "")


When the method returns, the queue instance contains a random queue name
generated by RabbitMQ. For example it may look like `amq.gen-JzTY20BRgKO-HjmUJj0wLg`.

When the connection that declared it closes, the queue will be deleted
because it is declared as exclusive.


Bindings
--------

![binding](http://i.imgur.com/yD3ARG8.png)

We've already created a fanout exchange and a queue. Now we need to
tell the exchange to send messages to our queue. That relationship
between exchange and a queue is called a _binding_.

    :::lisp
    (amqp-queue-bind q :exchange x)

From now on the `logs` exchange will append messages to our queue.

> #### Listing bindings
>
> You can list existing bindings using, you guessed it,
> `rabbitmqctl list_bindings`.


Putting it all together
-----------------------

![exchange-queues](http://i.imgur.com/T2dIpCt.png)


The producer program, which emits log messages, doesn't look much
different from the previous tutorial. The most important change is that
we now want to publish messages to our `logs` exchange instead of the
nameless one. We need to supply a `routingKey` when sending, but its
value is ignored for `fanout` exchanges. Here goes the code for
`emit_log.lisp` script:

	:::lisp
	(with-connection ("amqp://")
	  (with-channel ()
	    (let ((msg (format nil "~{~a ~}" (cdr sb-ext:*posix-argv*)))
	          (x (amqp-exchange-declare "logs" :type "fanout")))
	      (publish x msg)
	      (format t " [x] Sent '~a'~%" msg)
	      (sleep 1))))


[(emit_log.lisp source)](code/emit_logs.lisp)

As you see, after establishing the connection we declared the
exchange. This step is neccesary as publishing to a non-existing
exchange is forbidden.

The messages will be lost if no queue is bound to the exchange yet,
but that's okay for us; if no consumer is listening yet we can safely discard the message.

The code for `receive_logs.lisp`:

    :::lisp
	(with-connection ("amqp://")
	  (with-channel ()
	    (let ((n 1)
	          (x (amqp-exchange-declare "logs" :type "fanout"))
	          (q (queue.declare "" :auto-delete t)))
	      (amqp-queue-bind q :exchange x)
	      (format t " [*] Waiting for logs. To exit type (exit)~%")
	      (subscribe q (lambda (message)
	                     (let ((body (babel:octets-to-string (message-body message))))
	                       (format t " [x] #~a~%" body)))
	                 :type :sync)
	      (consume))))

[(receive_logs.lisp source)](code/receive_logs.lisp)


If you want to save logs to a file, just open a console and type:

    :::bash
    $ sbcl --non-interactive --load receive_logs.lisp > logs_from_rabbit.log

If you wish to see the logs on your screen, spawn a new terminal and run:

    :::bash
    $ sbcl --non-interactive --load receive_logs.lisp

And of course, to emit logs type:

    :::bash
    $ sbcl --non-interactive --load emit_log.lisp


Using `rabbitmqctl list_bindings` you can verify that the code actually
creates bindings and queues as we want. With two `receive_logs.lisp`
programs running you should see something like:

    :::bash
    $ sudo rabbitmqctl list_bindings
    Listing bindings ...
    logs    exchange        amq.gen-JzTY20BRgKO-HjmUJj0wLg  queue           []
    logs    exchange        amq.gen-vso0PVvyiRIL2WoV3i48Yg  queue           []
    ...done.

The interpretation of the result is straightforward: data from
exchange `logs` goes to two queues with server-assigned names. And
that's exactly what we intended.

To find out how to listen for a subset of messages, let's move on to
[tutorial 4](tutorial-four-cl.md)
