(in-package :cl-bunny.test)

(plan 1)

(wu-decimal:enable-reader-macro)

(defmethod mw-equiv:object-constituents ((type (eql 'local-time:timestamp)))
  (list #'local-time:to-rfc1123-timestring))

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
                                                     ;; ("array_field" . #(1 2 3))
                                                     ("decimal_field" . #$1.2))
                                           :persistent t
                                           :priority 8
                                           :correlation-id "r-1"
                                           :reply-to "a.sender"
                                           :expiration "2"
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
                                            ;; ("array_field" . #(1 2 3))
                                            ("decimal_field" . #$1.2))
                :test (lambda (x y)
                        (mw-equiv:object= x y t)))
            (is (message-persistent-p message) t)
            (is (message-body-string message) "abc")
            (is (message-app-id message) "cl-bunny.tests")
            (is (message-priority message) 8))))))

  (subtest "Basic.get"
    (with-connection ()
      (with-channel ()
        (let* ((q (queue.declare :name "basic.consume1" :auto-delete t))
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
                                                     ;; ("array_field" . #(1 2 3))
                                                     ("decimal_field" . #$1.2))
                                           :persistent nil
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
            (is (message-body-string message) "abc")
            (is (message-app-id message) "cl-bunny.tests")
            (is (message-priority message) 8)))))))

(finalize 1)
