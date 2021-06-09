import balls

import cps except trampoline
import eventqueue except trampoline

type
  InfiniteLoop = CatchableError

var jumps: int

proc trampoline(c: Cont) =
  jumps = 0
  var c: Continuation = c
  while c != nil and c.fn != nil:
    c = c.fn(c)
    inc jumps
    if jumps > 1000:
      raise newException(InfiniteLoop, $jumps & " iterations")

suite "eventqueue ball pit":

  var r = 0

  block:
    ## yield is a primitive that enters the dispatcher
    r = 0
    proc foo() {.cps: Cont.} =
      inc r
      coop()
      inc r
    trampoline: whelp foo()
    check r == 1
    run()
    check r == 2

  block:
    ## sleep is a primitive that rests in the dispatcher
    r = 0
    const q = 3
    proc foo() {.cps: Cont.} =
      var i = 0
      while i < q:
        inc r
        sleep(i + 1)
        inc r
        inc i
      check i == q
    spawn: whelp foo()
    run()
    check r == q * 2

  block:
    ## semaphores
    var sem = newSemaphore()
    var success = false

    proc signalSleeper(ms: int) {.cps: Cont.} =
      sleep ms
      signal sem

    proc signalWaiter() {.cps: Cont.} =
      wait sem
      success = true

    trampoline: whelp signalSleeper(10)
    trampoline: whelp signalWaiter()
    run()
    check success, "signal failed"

  block:
    ## fork
    when not defined(fork):
      skip"fork() not declared"
    r = 0
    proc foo() {.cps: Cont.} =
      inc r
      fork()
      inc r
    trampoline: whelp foo()
    check r == 3

  block:
    ## the sluggish yield test
    skip"low priority, since coop works fine"
    when defined(release):
      skip"too slow for release mode"
    const
      start = 2
      tiny = 0
      big = start * 4
    var count = start

    proc higher(ms: int) {.cps: Cont.} =
      while count < big and count > tiny:
        inc count
        sleep(ms)
        coop()
        coop()
        coop()
        coop()
        coop()
        coop()

    proc lower(ms: int) {.cps: Cont.} =
      while count < big and count > tiny:
        dec count
        sleep(ms)
        coop()

    spawn: whelp higher(1)
    spawn: whelp lower(1)
    run()

    check count != tiny, "you're a terrible coder"
