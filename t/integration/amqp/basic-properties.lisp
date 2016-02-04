(in-package :cl-bunny.test)

(plan 1)

(wu-decimal:enable-reader-macro)

(defmethod mw-equiv:object-constituents ((type (eql 'local-time:timestamp)))
  (list (lambda (timestamp)
          (with-output-to-string (stream)
            (local-time:format-rfc1123-timestring stream timestamp)))))

(subtest "Basic class properties tests"
  (subtest "Consumer"
    (with-connection ()
      (with-channel ()
        (let* ((q (queue.declare :name "basic.consume1" :auto-delete t))
               (now (local-time:now)))

          (subscribe-sync q)

          (queue.put q "abc" :properties `(:content-type "text/plain"
                                           :content-encoding "utf-8"
                                           :headers (("coordinates" . (("lat" . 59.35)
                                                                       ("lng" . 18.066667d0)))
                                                     ("time" . ,now)
                                                     ("participants" . 11)
                                                     ("i64_field" . 99999999999)
                                                     ("true_field" . t)
                                                     ("false_field" . nil)
                                                     ("void_field" . :void)
                                                     ("array_field" . #(1 2 3))
                                                     ("decimal_field" . #$1.2))
                                           :persistent t
                                           :priority 8
                                           :correlation-id "r-1"
                                           :reply-to "a.sender"
                                           :expiration "2000"
                                           :message-id "m-1"
                                           :timestamp ,now
                                           :type "dog-or-cat?"
                                           :user-id "guest"
                                           :app-id "cl-bunny.tests"
                                           :cluster-id "qwe"))
          (let* ((message (consume :one-shot t :timeout 10)))
            (is (message-content-type message) "text/plain")
            (is (message-content-encoding message) "utf-8")
            (is (message-headers message) `(("coordinates" . (("lat" . 59.35)
                                                              ("lng" . 18.066667d0)))
                                            ("time" . ,now)
                                            ("participants" . 11)
                                            ("i64_field" . 99999999999)
                                            ("true_field" . t)
                                            ("false_field" . nil)
                                            ("void_field" . :void)
                                            ("array_field" . #(1 2 3))
                                            ("decimal_field" . #$1.2))
                :test (lambda (x y)
                        (mw-equiv:object= x y t)))
            (is (message-persistent-p message) t)
            (is (message-priority message) 8)
            (is (message-correlation-id message) "r-1")
            (is (message-reply-to message) "a.sender")
            (is (message-expiration message) "2000")
            (is (message-message-id message) "m-1")
            (is (message-timestamp message) now :test (lambda (x y)
                                                        (mw-equiv:object= x y t)))
            (is (message-type message) "dog-or-cat?")
            (is (message-user-id message) "guest")
            (is (message-app-id message) "cl-bunny.tests")
            (is (message-cluster-id message) "qwe")
            (is (message-body-string message) "abc"))))))

  (subtest "Basic.get"
    (with-connection ()
      (with-channel ()
        (let* ((q (queue.declare-temp))
               (now (local-time:now)))

          (queue.put q "abc" :properties `(:content-type "text/plain"
                                           :content-encoding "utf-8"
                                           :headers (("coordinates" . (("lat" . 59.35)
                                                                       ("lng" . 18.066667d0)))
                                                     ("time" . ,now)
                                                     ("participants" . 11)
                                                     ("i64_field" . 99999999999)
                                                     ("true_field" . t)
                                                     ("false_field" . nil)
                                                     ("void_field" . :void)
                                                     ("array_field" . #(1 2 3))
                                                     ("decimal_field" . #$1.2))
                                           :persistent t
                                           :priority 8
                                           :correlation-id "r-1"
                                           :reply-to "a.sender"
                                           :expiration "2000"
                                           :message-id "m-1"
                                           :timestamp ,now
                                           :type "dog-or-cat?"
                                           :user-id "guest"
                                           :app-id "cl-bunny.tests"
                                           :cluster-id "qwe"))
          (sleep 0.5)
          (let* ((message (queue.get q)))
            (is (message-content-type message) "text/plain")
            (is (message-content-encoding message) "utf-8")
            (is (message-headers message) `(("coordinates" . (("lat" . 59.35)
                                                              ("lng" . 18.066667d0)))
                                            ("time" . ,now)
                                            ("participants" . 11)
                                            ("i64_field" . 99999999999)
                                            ("true_field" . t)
                                            ("false_field" . nil)
                                            ("void_field" . :void)
                                            ("array_field" . #(1 2 3))
                                            ("decimal_field" . #$1.2))
                :test (lambda (x y)
                        (mw-equiv:object= x y t)))
            (is (message-persistent-p message) t)
            (is (message-priority message) 8)
            (is (message-correlation-id message) "r-1")
            (is (message-reply-to message) "a.sender")
            (is (message-expiration message) "2000")
            (is (message-message-id message) "m-1")
            (is (message-timestamp message) now :test (lambda (x y)
                                                        (mw-equiv:object= x y t)))
            (is (message-type message) "dog-or-cat?")
            (is (message-user-id message) "guest")
            (is (message-app-id message) "cl-bunny.tests")
            (is (message-cluster-id message) "qwe")
            (is (message-body-string message) "abc")))))))

(finalize)
