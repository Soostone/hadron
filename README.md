# Hadron - Hadoop MapReduce in Haskell [![Build Status](https://travis-ci.org/Soostone/hadron.svg?branch=master)](https://travis-ci.org/Soostone/hadron)

Hadron aims to bring Haskell's type-safety to the complex and delicate
world of Hadoop Streaming MapReduce.

## Features

* Ties into Hadoop via the Streaming interface

* Orchestrates multi-step Hadoop jobs so you don't have to manually
  call Hadoop at all.

* Provides typed interactions with your input/output data on hdfs, s3
  or any other system that Hadoop supports.

* Every Map-Reduce step is fully typed both on input and output,
  making a long, sophisticated sequence of jobs much easier to design
  and maintain.

* Built-in support for multi-way map-side joins. Disparate data
  sources are each mapped to a common, monoidal type which then gets
  `mconcat`ed during reduce by join key. We support both required
  (a-la inner) and optional (a-la outer) joins. Current shortcoming
  here is the loss of input typing; only Tap ByteString can be used on
  input in order to support multiple datasets.
  
* Various convenience combinators in the Controller module, covering
  common tasks.
  
  
## Shortcomings and Issues

Hadoop seems to be terrible at constantly changing little details,
program flags and behavior across major releases. While we try to make
this package as sound as possible, you may be forced to do some
debugging due to a difference in the way Hadoop works on the version
you are running.

This library has been most commonly tested on Amazon's EMR offering
and Cloudera's local demo VM.

## Status

hadron is used extensively by Soostone to process datasets with rows
in the billions. Improvement opportunities exist, but it is very much
functional.

## Modules

### Hadron.Basic

This module exposes low level functionality for constructing a single
MapReduce step. Not recommended for direct use in most cases.


### Hadron.Controller

High level module for automated orchestration of multi-stage MapReduce
jobs. 

(More docs and examples to be added)

### Hadron.Protocol

Defines data encode/decode strategies via the Protocol type.



## TODO

See TODO.org.


## Release Notes


## Version 0.5

- Hadoop operations are now put behind a Hadron.Run interface.

- Hadron.Run.Local now implements a basic form of Hadoop mimickery,
  allowing a wide variety of Controller-based MapReduce applications
  to be run locally on the development machine.

- Several interface improvements to working with local and HDFS based
  files during a Controller app.

- runOnce combinator allows running an "IO a" action in the central
  node and having the same value appear on the remote (worker) nodes.


