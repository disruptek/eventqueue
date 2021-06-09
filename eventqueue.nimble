version = "0.0.6"
author = "disruptek"
description = "async i/o dispatcher for cps"
license = "MIT"

when not defined(release):
  requires "https://github.com/disruptek/balls > 3.0.0 & < 4.0.0"
  requires "https://github.com/disruptek/criterion < 1.0.0"

requires "https://github.com/disruptek/cps < 0.1.0"

task test, "run tests for ci":
  when defined(windows):
    exec "balls.cmd"
  else:
    exec findExe"balls"

task demo, "generate the demos":
  exec """demo docs/tock.svg "nim c -d:danger -d:cpsDebug --gc:arc --out=\$1 tests/tock.nim""""
  exec """demo docs/teventqueue.svg "nim c --gc:arc --out=\$1 tests/test.nim""""
