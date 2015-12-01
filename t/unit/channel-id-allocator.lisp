(in-package :cl-bunny.test)

(plan 1)

(subtest "Channel Id Allocator tests"
  (let ((a (bunny::new-channel-id-allocator 4)))
    (is (length (bunny::int-allocator-bits (bunny::channel-id-allocator-int-allocator a))) 4 "Four bit for Four channels")
    (is (bunny::next-channel-id a) 1)
    (is (bunny::next-channel-id a) 2)
    (is (bunny::next-channel-id a) 3)
    (is (bunny::next-channel-id a) 4)
    (is (bunny::next-channel-id a) nil)
    (bunny::release-channel-id a 3)
    (is (bunny::next-channel-id a) 3)
    (is (bunny::next-channel-id a) nil)))

(finalize)
