# Hadron - Hadoop MapReduce in Haskell

Hadron aims to bring Haskell's type-safety to construction of
compley and delicate world of Hadoop Streaming MapReduce.

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
  the most common tasks.
  
  
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

### Hadoop.Streaming

This module exposes low level functionality for constructing a single
MapReduce step. Not recommended for direct use in most scenarios.

(More docs and examples to be added)

### Hadoop.Streaming.Controller

High level module for automated orchestration of multi-stage MapReduce
jobs. 

(More docs and examples to be added)


## TODO

  - Allow specifying per-task re-run strategy (instead of global
    setting)
  
  - Re-think the Controller design to enable:
    - Parallel execution of non-dependent computation paths, like the
      cabal parallel build graph
    - Ability to chain-design MapReduce tasks before it's time to
      supply them with Taps via connect
    
  - Probably rename Hadoop.Streaming.Controller to Hadron.

  - Is there an easier way to express strongly typed multi-way joins
    instead of the current best of "Tap (Either (Either a b) c)"?

  - Escape newlines instead of all-out Base64 encoding in internal
    binary protocol (i.e. emit 0xff 0x0a for newline, and 0xff 0xff
    for 0xff). (gregorycollins)
    
  - Hand-roll a parseLine function break on newlines and tab
    characters using elemIndex. (gregorycollins)

  - Make launchHadoop logging real-time

  - Make a better CLI interface that can specify common hadoop
    settings (e.g. EMR vs. Cloudera)

  - Use MVector buffering with mutable growth (like in palgos) in
    joins instead of linked lists

  - Make a local orchestrate function for a non-hadoop, single-machine
    backend via 'sort'?

  - (?) Add support for parsing file locations, other settings from
    a config file
    

