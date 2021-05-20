import std/strutils
import std/macros
import std/os
import std/selectors
import std/monotimes
import std/nativesockets
import std/tables
import std/times
import std/deques

import cps

import eventqueue/semaphore
export Semaphore, semaphore.`==`, semaphore.`<`, semaphore.hash
export Event

const
  eqDebug {.booldefine, used.} = false   ## emit extra debugging output
  eqPoolSize {.intdefine, used.} = 64    ## expected pending continuations
  eqTraceSize {.intdefine, used.} = 1000 ## limit the traceback

type
  Readiness = enum
    Unready = "the default state, pre-initialized"
    Stopped = "we are outside an event loop but available for queuing events"
    Running = "we're in a loop polling for events and running continuations"
    Stopping = "we're tearing down the dispatcher and it will shortly stop"

  Clock = MonoTime
  Id = distinct int  ## semaphore registration
  Fd = distinct int

  Waiting = seq[Cont]
  Pending = Table[Semaphore, Cont]

  EventQueue = object
    state: Readiness              ## dispatcher readiness
    lastId: Id                    ## id of last-issued registration
    pending: Pending              ## maps pending semaphores to Conts
    waiting: Waiting              ## maps waiting selector Fds to Conts
    selector: Selector[Cont]      ## watches selectable stuff
    yields: Deque[Cont]           ## continuations ready to run
    waiters: int                  ## a count of selector listeners

    manager: Selector[Clock]      ## monitor polling, wake-ups
    timer: Fd                     ## file-descriptor of polling timer
    wake: SelectEvent             ## wake-up event for queue actions

  Cont* = ref object of RootObj
    fn*: proc(c: Cont): Cont {.nimcall.}
    when eqDebug:
      clock: Clock                  ## time of latest poll loop
      delay: Duration               ## polling overhead
      fd: Fd                        ## our last file-descriptor
    when cpsTrace:
      filename: string
      line: int
      column: int
      identity: string

when cpsTrace:
  type
    Frame = object
      c: Cont
      e: ref CatchableError
    Stack = Deque[Frame]

const
  invalidId = Id(0)
  invalidFd = Fd(-1)
  oneMs = initDuration(milliseconds = 1)

var eq {.threadvar.}: EventQueue

template now(): Clock = getMonoTime()

proc `$`(id: Id): string {.used.} = "{" & system.`$`(id.int) & "}"
proc `$`(fd: Fd): string {.used.} = "[" & system.`$`(fd.int) & "]"
proc `$`(c: Cont): string {.used.} =
  when cpsTrace:
    # quality poor!
    #"$1($2) $3" % [ c.filename, $c.line, c.identity ]
    c.identity
  else:
    "&" & $cast[uint](c)

proc `<`(a, b: Id): bool {.borrow, used.}
proc `<`(a, b: Fd): bool {.borrow, used.}
proc `==`(a, b: Id): bool {.borrow, used.}
proc `==`(a, b: Fd): bool {.borrow, used.}

proc put(w: var Waiting; fd: int | Fd; c: Cont) =
  if not c.isNil:
    while fd.int >= w.len:
      setLen(w, w.len * 2)
    w[fd.int] = c
    inc eq.waiters
    assert eq.waiters > 0

proc get(w: var Waiting; fd: int | Fd): Cont =
  swap(result, w[fd.int])
  dec eq.waiters

method clone(c: Cont): Cont {.base.} =
  ## copy the continuation for the purposes of, eg. fork
  new result
  result[] = c[]

proc init() {.inline.} =
  ## initialize the event queue to prepare it for requests
  if eq.state == Unready:
    # create a new manager
    eq.timer = invalidFd
    eq.manager = newSelector[Clock]()
    eq.wake = newSelectEvent()
    eq.selector = newSelector[Cont]()
    eq.waiters = 0

    # make sure we have a decent amount of space for registrations
    if len(eq.waiting) < eqPoolSize:
      eq.waiting = newSeq[Cont](eqPoolSize).Waiting

    # the manager wakes up when triggered to do so
    registerEvent(eq.manager, eq.wake, now())

    eq.lastId = invalidId
    eq.yields = initDeque[Cont]()
    eq.state = Stopped

proc nextId(): Id {.inline.} =
  ## generate a new registration identifier
  init()
  # rollover is pretty unlikely, right?
  when sizeof(eq.lastId) < 8:
    if (unlikely) eq.lastId == high(eq.lastId):
      eq.lastId = succ(invalidId)
    else:
      inc eq.lastId
  else:
    inc eq.lastId
  result = eq.lastId

proc newSemaphore*(): Semaphore =
  ## Create a new Semaphore.
  result.init nextId().int

template wakeUp() =
  if eq.state == Unready:
    init()

template wakeAfter(body: untyped): untyped =
  ## wake up the dispatcher after performing the following block
  init()
  try:
    body
  finally:
    wakeUp()

proc len*(eq: EventQueue): int =
  ## The number of pending continuations.
  result = len(eq.yields) + len(eq.pending) + eq.waiters

proc `[]=`(eq: var EventQueue; s: var Semaphore; c: Cont) =
  ## put a semaphore into the queue with its registration
  assert not s.isReady
  eq.pending[s] = c

proc stop*() =
  ## Tell the dispatcher to stop, discarding all pending continuations.
  if eq.state == Running:
    eq.state = Stopping

    # tear down the manager
    assert not eq.manager.isNil
    eq.manager.unregister eq.wake
    close(eq.wake)
    if eq.timer != invalidFd:
      eq.manager.unregister eq.timer.int
      eq.timer = invalidFd
    close(eq.manager)

    # discard the current selector to dismiss any pending events
    close(eq.selector)

    # discard the contents of the semaphore cache
    eq.pending = initTable[Semaphore, Cont](eqPoolSize)

    # re-initialize the queue
    eq.state = Unready
    init()

proc init*(c: Cont): Cont =
  result = c

when cpsTrace:
  import std/strformat

  proc init*(c: Cont; identity: static[string];
             file: static[string]; row, col: static[int]): Cont =
    result = init c
    result.identity = identity
    result.filename = file
    result.line = row
    result.column = col

  proc addFrame(stack: var Stack; c: Cont) =
    if c.state != Dismissed:
      while stack.len >= eqTraceSize:
        popFirst stack
      stack.addLast Frame(c: c)

  proc formatDuration(d: Duration): string =
    ## format a duration to a nice string
    let
      n = d.inNanoseconds
      ss = (n div 1_000_000_000) mod 1_000
      ms = (n div 1_000_000) mod 1_000
      us = (n div 1_000) mod 1_000
      ns = (n div 1) mod 1_000
    try:
      result = fmt"{ss:>3}s {ms:>3}ms {us:>3}μs {ns:>3}ns"
    except:
      result = [$ss, $ms, $us, $ns].join(" ")

  proc `$`(f: Frame): string =
    result = $f.c
    when eqDebug:
      let took = formatDuration(f.c.delay)
      result.add "\n"
      result.add took.align(20) & " delay"

  proc writeStackTrace(stack: Stack) =
    if stack.len == 0:
      writeLine(stderr, "no stack recorded")
    else:
      writeLine(stderr, "noroutine stack: (most recent call last)")
      when eqDebug:
        var prior = stack[0].c.clock
        for i, frame in stack.pairs:
          let took = formatDuration(frame.c.clock - prior)
          prior = frame.c.clock
          writeLine(stderr, $frame)
          writeLine(stderr, took.align(20) & " total")
      else:
        for frame in stack.items:
          writeLine(stderr, $frame)

else:
  proc writeStackTrace(c: Cont): Cont =
    result = c
    warning "--define:cpsTrace:on to output traces"

proc trampoline*(c: Cont) =
  ## Run the supplied continuation until it is complete.
  var c = c
  when cpsTrace:
    var stack = initDeque[Frame](eqTraceSize)
  while c.state == State.Running:
    when eqDebug:
      echo "🎪tramp ", c, " at ", c.clock
    try:
      c = c.fn(c)
      when cpsTrace:
        addFrame(stack, c)
    except CatchableError:
      when cpsTrace:
        writeStackTrace stack
      raise

proc poll*() =
  ## See what continuations need running and run them.
  if eq.state != Running: return

  if eq.waiters > 0:
    when eqDebug:
      let clock = now()

    # ready holds the ready file descriptors and their events.
    let ready = select(eq.selector, -1)
    for event in ready.items:
      # stop listening on this fd
      unregister(eq.selector, event.fd)
      # get the pending continuation
      let cont = eq.waiting.get(event.fd)
      when eqDebug:
        cont.clock = clock
        cont.delay = now() - clock
        cont.fd = event.fd.Fd
        echo "💈delay ", cont.delay
      trampoline cont

  if len(eq.yields) > 0:
    # run no more than the current number of ready continuations
    for index in 1 .. len(eq.yields):
      trampoline:
        popFirst eq.yields

  # if there are no pending continuations,
  if len(eq) == 0:
    # and there is no polling timer setup,
    if eq.timer == invalidFd:
      # then we'll stop the dispatcher now.
      stop()
    else:
      when eqDebug:
        echo "💈"
      # else wait until the next polling interval or signal
      for ready in eq.manager.select(-1):
        # if we get any kind of error, all we can reasonably do is stop
        if ready.errorCode.int != 0:
          stop()
          raiseOSError(ready.errorCode, "eventqueue error")
        break

proc run*(interval: Duration = DurationZero) =
  ## The dispatcher runs with a maximal polling interval; an `interval` of
  ## `DurationZero` causes the dispatcher to return when the queue is empty.

  # make sure the eventqueue is ready to run
  init()
  assert eq.state in {Running, Stopped}, $eq.state
  if interval.inMilliseconds == 0:
    discard "the dispatcher returns after emptying the queue"
  else:
    # the manager wakes up repeatedly, according to the provided interval
    eq.timer = registerTimer(eq.manager,
                             timeout = interval.inMilliseconds.int,
                             oneshot = false, data = now()).Fd
  # the dispatcher is now running
  eq.state = Running
  while eq.state == Running:
    poll()

proc coop*(c: Cont): Cont {.cpsMagic.} =
  ## Pass control to other pending continuations in the dispatcher before
  ## continuing; effectively a cooperative yield.
  wakeAfter:
    addLast(eq.yields, c)

proc sleep*(c: Cont; interval: Duration): Cont {.cpsMagic.} =
  ## Sleep for `interval` before continuing.
  if interval < oneMs:
    raise newException(ValueError, "intervals < 1ms unsupported")
  else:
    wakeAfter:
      let fd = registerTimer(eq.selector,
                             timeout = interval.inMilliseconds.int,
                             oneshot = true, data = c)
      eq.waiting.put(fd, c)
      when eqDebug:
        echo "⏰timer ", fd.Fd

proc sleep*(c: Cont; ms: int): Cont {.cpsMagic.} =
  ## Sleep for `ms` milliseconds before continuing.
  let interval = initDuration(milliseconds = ms)
  sleep(c, interval)

proc sleep*(c: Cont; secs: float): Cont {.cpsMagic.} =
  ## Sleep for `secs` seconds before continuing.
  sleep(c, (1_000 * secs).int)

proc dismiss*(c: Cont): Cont {.cpsMagic.} =
  ## Discard the current continuation.
  discard

proc noop*(c: Cont): Cont {.cpsMagic.} =
  ## A primitive that merely sheds scope.
  result = c

template signalImpl(s: Semaphore; body: untyped): untyped =
  ## run the body when when semaphore is NOT found in the queue
  var c: Cont
  if take(eq.pending, s, c):
    wakeUp()
    addLast(eq.yields, c)
  else:
    body

proc signal*(s: var Semaphore) =
  ## Signal the given Semaphore `s`, causing the first waiting continuation
  ## to be queued for execution in the dispatcher; control remains in
  ## the calling procedure.
  semaphore.signal s
  withReady s:
    init()
    signalImpl s:
      discard

proc signalAll*(s: var Semaphore) =
  ## Signal the given Semaphore `s`, causing all waiting continuations
  ## to be queued for execution in the dispatcher; control remains in
  ## the calling procedure.
  semaphore.signal s
  if s.isReady:
    init()
    while true:
      signalImpl s:
        break

proc wait*(c: Cont; s: var Semaphore): Cont {.cpsMagic.} =
  ## Queue the current continuation pending readiness of the given
  ## Semaphore `s`.
  if s.isReady:
    wakeUp()
    addLast(eq.yields, c)
  else:
    eq.pending[s] = c

proc fork*(c: Cont): Cont {.cpsMagic.} =
  ## Duplicate the current continuation.
  result = c
  wakeAfter:
    addLast(eq.yields, clone c)

proc spawn*(c: Cont) =
  ## Queue the supplied continuation `c`; control remains in the calling
  ## procedure.
  wakeAfter:
    addLast(eq.yields, c)

proc iowait*(c: Cont; file: int | SocketHandle;
             events: set[Event]): Cont {.cpsMagic.} =
  ## Continue upon any of `events` on the given file-descriptor or
  ## SocketHandle.
  if len(events) == 0:
    raise newException(ValueError, "no events supplied")
  else:
    wakeAfter:
      registerHandle(eq.selector, file, events = events, data = c)
      eq.waiting.put(file.int, c)
      when eqDebug:
        echo "📂file ", $Fd(file)
