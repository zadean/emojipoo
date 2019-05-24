emojipoo &#128169;
=====

Pure Erlang, Log-Structured Merge-Tree

&#128169; is a refactoring  and simplification of [HanoiDB]("https://github.com/krestenkrab/hanoidb").

It uses the same concept of doubling level sizes, but here they are layers on the pile...

Newer keys are slapped onto the top of the pile, and **it rolls down-hill.

**Changes**

* The bloom filter was removed
* The external `plain_fsm` dependency was replaced with `gen_statem` in OTP
* All external dependencies removed
* Fold functions removed
* range and prefix functions added 
* range and prefix functions return iterators that `qlc` would expect

