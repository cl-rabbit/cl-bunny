#!/usr/bin/env sh

#first clone Quicklisp stuff
git clone https://github.com/deadtrickster/safe-queue.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/safe-queue
git clone https://github.com/deadtrickster/eventfd.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/eventfd
git clone https://github.com/fukamachi/quri.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/quri
git clone -b master1 https://github.com/deadtrickster/cl-rabbit.git /home/travis/.roswell/impls/ALL/ALL/quicklisp/local-projects/cl-rabbit

#clone and build latest librabbitmq
git clone https://github.com/alanxz/rabbitmq-c.git ~/rabbitmq-c
mkdir ~/rabbitmq-c/build
cd ~/rabbitmq-c/build
cmake ..
sudo cmake --build . --target install
sudo cp /usr/local/lib/x86_64-linux-gnu/librabbitmq.so.4.1.4 /usr/lib/librabbitmq.so
