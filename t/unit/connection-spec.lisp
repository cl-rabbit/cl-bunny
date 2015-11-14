(in-package :cl-bunny.test)

(plan 2)

(subtest "Connection string parser tests [without additional params]"
  ;; this should comply with https://www.rabbitmq.com/uri-spec.html
  ;; tests based on Apendix A
  (subtest "amqp://user:pass@host:10000/vhost"
    (let ((spec (bunny::make-connection-spec "amqp://user:pass@host:10000/vhost")))
      (is (connection-spec-login spec) "user")
      (is (connection-spec-password spec) "pass")
      (is (connection-spec-host spec) "host")
      (is (connection-spec-port spec) 10000)
      (is (connection-spec-vhost spec) "vhost")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://user:pass@host:10000/vhost")))

  (subtest "amqp://user%61:%61pass@ho%61st:10000/v%2fhost"
    (let ((spec (bunny::make-connection-spec "amqp://user%61:%61pass@ho%61st:10000/v%2fhost")))
      (is (connection-spec-login spec) "usera")
      (is (connection-spec-password spec) "apass")
      (is (connection-spec-host spec) "hoast")
      (is (connection-spec-port spec) 10000)
      (is (connection-spec-vhost spec) "v/host")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://usera:apass@hoast:10000/v%2Fhost")))

  (subtest "amqp://"
    (let ((spec (bunny::make-connection-spec "amqp://")))
      (is (connection-spec-login spec) "guest")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "localhost")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "/")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://")))

  (subtest "amqps://"
    (let ((spec (bunny::make-connection-spec "amqps://")))
      (is (connection-spec-login spec) "guest")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "localhost")
      (is (connection-spec-port spec) 5671)
      (is (connection-spec-vhost spec) "/")
      (is (connection-spec-use-tls-p spec) t "Do use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqps://")))

  (subtest "amqp://:@/"
    (let ((spec (bunny::make-connection-spec "amqp://:@/")))
      (is (connection-spec-login spec) "")
      (is (connection-spec-password spec) "")
      (is (connection-spec-host spec) "localhost")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://:@/")))

  (subtest "amqp://user@"
    (let ((spec (bunny::make-connection-spec "amqp://user@")))
      (is (connection-spec-login spec) "user")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "localhost")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "/")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://user@")))

  (subtest "amqp://user:pass@"
    (let ((spec (bunny::make-connection-spec "amqp://user:pass@")))
      (is (connection-spec-login spec) "user")
      (is (connection-spec-password spec) "pass")
      (is (connection-spec-host spec) "localhost")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "/")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://user:pass@")))

  (subtest "amqp://host"
    (let ((spec (bunny::make-connection-spec "amqp://host")))
      (is (connection-spec-login spec) "guest")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "host")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "/")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://host")))

  (subtest "amqp://:10000"
    (let ((spec (bunny::make-connection-spec "amqp://:10000")))
      (is (connection-spec-login spec) "guest")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "localhost")
      (is (connection-spec-port spec) 10000)
      (is (connection-spec-vhost spec) "/")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://:10000")))

  (subtest "amqp:///vhost"
    (let ((spec (bunny::make-connection-spec "amqp:///vhost")))
      (is (connection-spec-login spec) "guest")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "localhost")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "vhost")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp:///vhost")))

  (subtest "amqp://host/"
    (let ((spec (bunny::make-connection-spec "amqp://host/")))
      (is (connection-spec-login spec) "guest")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "host")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://host/")))

  (subtest "amqp://host/%2f"
    (let ((spec (bunny::make-connection-spec "amqp://host/%2f")))
      (is (connection-spec-login spec) "guest")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "host")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "/")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://host")))

  (subtest "amqp://[::1]"
    (let ((spec (bunny::make-connection-spec "amqp://[::1]")))
      (is (connection-spec-login spec) "guest")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "::1")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "/")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) t "Address is IPv6")
      (is (actually-print-connection-spec spec) "amqp://[::1]"))))

(subtest "Connection list parser tests [without additional params]"
  (subtest "NIL"
    (let ((spec (bunny::make-connection-spec nil)))
      (is (connection-spec-login spec) "guest")
      (is (connection-spec-password spec) "guest")
      (is (connection-spec-host spec) "localhost")
      (is (connection-spec-port spec) 5672)
      (is (connection-spec-vhost spec) "/")
      (is (connection-spec-use-tls-p spec) nil "Do not use TLS")
      (is (connection-spec-use-ipv6-p spec) nil "Address is not IPv6")
      (is (actually-print-connection-spec spec) "amqp://"))))

(finalize)
