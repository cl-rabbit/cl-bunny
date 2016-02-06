#!/usr/bin/env sh

#first clone Quicklisp stuff
git clone https://github.com/deadtrickster/cl-events.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/cl-events
git clone https://github.com/deadtrickster/safe-queue.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/safe-queue
git clone https://github.com/deadtrickster/eventfd.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/eventfd
git clone https://github.com/fukamachi/quri.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/quri
git clone https://github.com/cl-rabbit/cl-amqp.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/cl-amqp
git clone https://github.com/pkhuong/string-case.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/string-case
git clone https://github.com/sionescu/bordeaux-threads.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/bordeaux-threads
