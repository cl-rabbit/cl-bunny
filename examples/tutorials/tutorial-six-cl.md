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
# RabbitMQ tutorial - Remote procedure call (RPC)

## Remote procedure call (RPC)
### (using [cl-bunny](http://cl-rabbit.io/cl-bunny))

In the [second tutorial](tutorial-two-cl.md) we learned how to
use _Work Queues_ to distribute time-consuming tasks among multiple
workers.

But what if we need to run a function on a remote computer and wait for
the result?  Well, that's a different story. This pattern is commonly
known as _Remote Procedure Call_ or _RPC_.

In this tutorial we're going to use RabbitMQ to build an RPC system: a
client and a scalable RPC server. As we don't have any time-consuming
tasks that are worth distributing, we're going to create a dummy RPC
service that returns Fibonacci numbers.

### Client interface

To illustrate how an RPC service could be used we're going to
create a simple client. It's going to be a function named `start-client`
which sends an RPC request and blocks until the answer is received:

    :::lisp
    (defun start-client (n)
      (with-connection ("amqp://")
	    (with-channel ()
	    ...
	    )))

    (start-client 30)


> #### A note on RPC
>
> Although RPC is a pretty common pattern in computing, it's often criticised.
> The problems arise when a programmer is not aware
> whether a function call is local or if it's a slow RPC. Confusions
> like that result in an unpredictable system and adds unnecessary
> complexity to debugging. Instead of simplifying software, misused RPC
> can result in unmaintainable spaghetti code.
>
> Bearing that in mind, consider the following advice:
>
>  * Make sure it's obvious which function call is local and which is remote.
>  * Document your system. Make the dependencies between components clear.
>  * Handle error cases. How should the client react when the RPC server is
>    down for a long time?
>
> When in doubt avoid RPC. If you can, you should use an asynchronous
> pipeline - instead of RPC-like blocking, results are asynchronously
> pushed to a next computation stage.


### Callback queue

In general doing RPC over RabbitMQ is easy. A client sends a request
message and a server replies with a response message. In order to
receive a response we need to send a 'callback' queue address with the
request. We can use the default queue.
Let's try it:


    :::lisp
    (let ((q (queue.declare "" :auto-delete t))
    	  (x (default-exchange)))
	  (publish x
               message
               :routing-key "rpc_queue"
               :properties `((:reply-to . ,reply-queue))))

    # ... then code to read a response message from the callback_queue ...


> #### Message properties
>
> The AMQP protocol predefines a set of 14 properties that go with
> a message. Most of the properties are rarely used, with the exception of
> the following:
>
> * `:persistent`: Marks a message as persistent (with a value of `true`)
>    or transient (`false`). You may remember this property
>    from [the second tutorial](tutorial-two-cl.md).
> * `:content-type`: Used to describe the mime-type of the encoding.
>    For example for the often used JSON encoding it is a good practice
>    to set this property to: `application/json`.
> * `:reply-to`: Commonly used to name a callback queue.
> * `:correlation-id`: Useful to correlate RPC responses with requests.

### Correlation Id

In the method presented above we suggest creating a callback queue for
every RPC request. That's pretty inefficient, but fortunately there is
a better way - let's create a single callback queue per client.

That raises a new issue, having received a response in that queue it's
not clear to which request the response belongs. That's when the
`:correlation-id` property is used. We're going to set it to a unique
value for every request. Later, when we receive a message in the
callback queue we'll look at this property, and based on that we'll be
able to match a response with a request. If we see an unknown
`:correlation-id` value, we may safely discard the message - it
doesn't belong to our requests.

You may ask, why should we ignore unknown messages in the callback
queue, rather than failing with an error? It's due to a possibility of
a race condition on the server side. Although unlikely, it is possible
that the RPC server will die just after sending us the answer, but
before sending an acknowledgment message for the request. If that
happens, the restarted RPC server will process the request again.
That's why on the client we must handle the duplicate responses
gracefully, and the RPC should ideally be idempotent.

### Summary

![RPC client and server structure](http://i.imgur.com/SwxhQcm.png)

Our RPC will work like this:

  * When the Client starts up, it creates an anonymous exclusive
    callback queue.
  * For an RPC request, the Client sends a message with two properties:
    `:reply-to`, which is set to the callback queue and `:correlation-id`,
    which is set to a unique value for every request.
  * The request is sent to an `rpc-queue` queue.
  * The RPC worker (aka: server) is waiting for requests on that queue.
    When a request appears, it does the job and sends a message with the
    result back to the Client, using the queue from the `:reply-to` field.
  * The client waits for data on the callback queue. When a message
    appears, it checks the `:correlation-id` property. If it matches
    the value from the request it returns the response to the
    application.

Putting it all together
-----------------------

The Fibonacci task:

    :::lisp
    (defun fibonacci (n)
	    (if (< n 3)
	        1
	        (+ (fibonacci (- n 1)) (fibonacci (- n 2))) ))


We declare our fibonacci function. It assumes only valid positive integer input.
(Don't expect this one to work for big numbers,
and it's probably the slowest recursive implementation possible).


The code for our RPC server [rpc_server.lisp](code/rpc_server.lisp) looks like this:

    :::lisp
	(with-connection ("amqp://")
	  (with-channel ()
	    (let ((x (default-exchange))
	          (q (queue.declare "rpc_queue" :auto-delete t)))
	      (format t " [x] Awaiting RPC requests~%")
	      (subscribe q (lambda (message)                     
	                     (let ((n (nibbles:sb32ref/le (coerce (message-body message) '(vector (unsigned-byte 8))) 0)))
	                       (publish x
	                                (int32-to-octet (fibonacci n ))
	                                :routing-key (alexandria::assoc-value (message-properties message) :reply-to)
	                                :properties `((:correlation-id . ,(alexandria::assoc-value (message-properties message) :correlation-id))))))
	                 :type :sync)
	      (consume))))





The server code is rather straightforward:

  * As usual we start by establishing the connection, channel and declaring
    the queue.
  * We might want to run more than one server process. In order
    to spread the load equally over multiple servers we need to set the
    `prefetch` setting on channel.
  * We use `subscribe` to consume messages from the queue. Then we enter the while loop in which
    we wait for request messages, do the work and send the response back.


The code for our RPC client [rpc_client.lisp](code/rpc_client.lisp):

    :::lisp
	(defun start-client (n)
	  (with-connection ("amqp://")
	    (with-channel ()
	      (let ((x (default-exchange))
	            (server-queue "rpc_queue")
	            (reply-queue (queue.declare "" :auto-delete t))
	            (lock (bt:make-lock))
	            (condition (bt:make-condition-variable))
	            (result nil))
	        (format t " [x] Requesting fib(~a)" n)
	        (bt:with-lock-held (lock)
	          (subscribe reply-queue (lambda (message)
	                                   (bt:with-lock-held (lock)
	                                     (setf result (nibbles:sb32ref/le (coerce (message-body message) '(vector (unsigned-byte 8))) 0))
	                                     (bt:condition-notify condition))))
	          (publish x
	                   (int32-to-octet n)
	                   :routing-key server-queue
	                   :properties `((:correlation-id . ,(format nil "~a~a~a" (random 100) (random 100) (random 100)))
	                                 (:reply-to . ,reply-queue)))
	          (bt:condition-wait condition lock)
	          (format t " [.] Got ~a~%" result)
	          result)))))



Now is a good time to take a look at our full example source code (which includes basic exception handling) for
[rpc_client.lisp](code/rpc_client.lisp) and [rpc_server.lisp](code/rpc_server.lisp).


Our RPC service is now ready. We can start the server:

    :::bash
    $ sbcl --non-interactive --load rpc_server.lisp
     [x] Awaiting RPC requests

To request a fibonacci number run the client:

    :::bash
    $ sbcl --non-interactive --load rpc_client.lisp
	 [x] Requesting fib(0)
	 [.] Got 1
	 [x] Requesting fib(1)
	 [.] Got 1
	 [x] Requesting fib(2)
	 [.] Got 1
	 [x] Requesting fib(3)
	 [.] Got 2
	 [x] Requesting fib(4)
	 [.] Got 3
	 [x] Requesting fib(5)
	 [.] Got 5


The design presented here is not the only possible implementation of a RPC
service, but it has some important advantages:

 * If the RPC server is too slow, you can scale up by just running
   another one. Try running a second `rpc_server.lisp` in a new console.
 * On the client side, the RPC requires sending and
   receiving only one message. No synchronous calls like `queue`
   are required. As a result the RPC client needs only one network
   round trip for a single RPC request.

Our code is still pretty simplistic and doesn't try to solve more
complex (but important) problems, like:

 * How should the client react if there are no servers running?
 * Should a client have some kind of timeout for the RPC?
 * If the server malfunctions and raises an exception, should it be
   forwarded to the client?
 * Protecting against invalid incoming messages
   (eg checking bounds, type) before processing.
