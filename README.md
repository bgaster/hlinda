Overview
========

A Haskell implementation of distributed Linda-style tuple spaces, based on the CML
implementation by John Reppy; from his book Concurrent Programming in ML.

The implementation is built on STM and has a number of things that could
be done better!

It is worth noting that while it would have been possible to build an
abstraction on top of STM channels to support CML style events the
implementation passes channels explicitly. This has the disadvantage
of having to explictly read/write from these channels in interfaces
that could simply sync on an event in Reppy's implementation. The advantage
is that we can use STM channels directly without an additonal layer of
abstraction. It would be nice to see an event abstraction provided as
part of STM, not simply in the CML package, but as a first class part
of the STM implementation.

Building
========

cabal configure

cabal build

Usage
=====

Still need to add an example...
