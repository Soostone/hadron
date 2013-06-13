# Hadron - Hadoop MapReduce in Haskell For Massive Win

If you're a Haskell fan, Hadron makes working with Hadoop a breeze.

## Features

* Ties into Hadoop via the Streaming interface

* Orchestrates multi-step Hadoop jobs so you don't have to manually
  call Hadoop *at all*.

* Provides typed interactions with your input/output data on hdfs, s3
  or any other system that Hadoop supports.

* Every Map-Reduce step is fully typed both on input and output,
  making a long, sophisticated sequence of jobs much easier to reason
  with.

* Built-in support for JOINs mapper-side. Disparate data sources are
  each mapped to a common, monoidal type which then get `mconcat`ed
  during reduce by join key. We support both required (a-la inner) and
  optional (a-la outer) joins.

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

  - Escape newlines instead of all-out Base64 encoding in internal
    binary protocol (i.e. emit 0xff 0x0a for newline, and 0xff 0xff
    for 0xff). (gregorycollins)
    
  - Hand-roll a parseLine function break on newlines and tab
    characters using elemIndex. (gregorycollins)

  - Add a `loadTap` function to support loading small files directly
    into memory, as opposed to a full-blown join. bytestring-mmap?

  - Make launchHadoop logging real-time

  - Make and expose some common MROptions

  - Make a better CLI interface that can specify common hadoop
    settings (e.g. EMR vs. Cloudera)

  - Use MVector buffering with mutable growth (like in palgos) in
    joins instead of linked lists

  - Add built-in/common reducers, mappers, etc.

  - Make a local orchestrate function for a non-hadoop, single-machine
    backend via 'sort'?

  - (?) Add support for parsing file locations, other settings from
    a config file
    

