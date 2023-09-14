# (c) 2023 Andreas Rumpf

import std / [atomics, tasks, times]
from std / os import sleep

import std / isolation
export isolation


type
  Master* = object ## Masters can spawn new tasks inside an `awaitAll` block.
    constrained: bool
    error:       Atomic[string]
    lock:        Atomic[bool]
    running:     Atomic[int]
    stop:        Atomic[bool]
    timeout:     Time

proc `=copy`(dest: var Master; src: Master) {.error.}
proc `=sink`(dest: var Master; src: Master) {.error.}

proc createMaster*(timeout = default(Duration)): Master =
  result = default(Master)
  result.error.store "",   moRelaxed
  result.lock.store false, moRelaxed
  result.stop.store false, moRelaxed
  if timeout != default(Duration):
    result.constrained = true
    result.timeout     = getTime() + timeout

proc cancel*(m: var Master) =
  ## Try to stop all running tasks immediately.
  ## This cannot fail but it might take longer than desired.
  m.stop.store true, moRelaxed

proc cancelled*(m: var Master): bool {.inline.} =
  m.stop.load moRelaxed

proc taskCreated(m: var Master) {.inline.} =
  m.running.atomicInc 1

proc taskCompleted(m: var Master) {.inline.} =
  m.running.atomicDec 1

proc stillHaveTime*(m: Master): bool {.inline.} =
  not m.constrained or getTime() < m.timeout

const ThrSleepInMs   {.intdefine.} = 10

proc waitForCompletions(m: var Master) =
  var timeoutErr = false
  if not m.constrained:
    while m.running.load(moRelaxed) > 1:
      discard
  else:
    while true:
      let success = m.running.load(moRelaxed) == 0
      if success: break
      if getTime() > m.timeout:
        timeoutErr = true
        break
      sleep(ThrSleepInMs) # XXX maybe make more precise
  let err = m.error.load(moRelaxed)
  if err.len > 0:
    raise newException(ValueError, err)
  elif timeoutErr:
    m.cancel()
    raise newException(ValueError, "'awaitAll' timeout")

# thread pool independent of the 'master':

const
  ThreadPoolSize {.intdefine.} = 8

type
  ThreadState = enum
    tsEmpty
    tsFeeding
    tsToDo
    tsWip
    tsDone
    tsFail
    tsCancel

  PoolTask = object      ## a task for the thread pool
    m:      ptr Master   ## who is waiting for us
    t:      Task         ## what to do
    result: pointer      ## where to store the potential result

  FixedChan = object ## channel of a fixed size
    L:     Atomic[bool]
    board: array[ThreadPoolSize, Atomic[ThreadState]]
    data:  array[ThreadPoolSize, PoolTask]

var
  thr:             array[ThreadPoolSize, Thread[int]]
  chan:            FixedChan
  globalStopToken: Atomic[bool]
  busyThreads:     Atomic[int]
  curretThread:    Atomic[int]


iterator threads(): int {.inline.} =
  # The default algorithm is roud-robin
  # is more likelly to keep all threads awake
  # is faster but uses more energy
  when defined lowEnergy:
    var n = high(thr)
    while true:
      yield n
      dec n
      if n < 0:
        n = high(thr)
  else:
    var n: int
    while true:
      n = curretThread.fetchSub(1)
      if n < 0:
        n = high(thr)
        curretThread.store(n - 1, moRelaxed)
      yield n

proc send(item: sink PoolTask) =
  var 
    done  = tsDone
    empty = tsEmpty
    ackchyually = 0

  for i in threads():
    # Test
    if chan.board[i].load(moRelaxed) notin [done, empty]:
      cpuRelax()
      continue

    # Test and Set
    if chan.board[i].compareExchange(done,
        tsFeeding, moAcquire, moRelease):
      ackchyually = i
      break

    # Test and Set
    if chan.board[i].compareExchange(empty,
        tsFeeding, moAcquire, moRelease):
      ackchyually = i
      break
    cpuRelax()

  chan.data[ackchyually] = item
  chan.board[ackchyually].store(tsToDo, moRelease)

proc worker(i: int) {.thread.} =
  var
    item: PoolTask
    todo = tsToDo
    wip  = tsWip

  when defined lowEnergy:
    let maxIdleTime = ThrSleepInMs / 100.0
    var lastWorkAt  = cpuTime()

  while not globalStopToken.load(moRelaxed):
    when defined lowEnergy:
      var now = cpuTime()
      if lastWorkAt - now > maxIdleTime:
        sleep(ThrSleepInMs)

    # Test
    if chan.board[i].load(moRelaxed) != todo:
      cpuRelax()
      continue
    # Test and Set
    if not chan.board[i].compareExchange(todo,
        wip, moRelaxed, moRelease):
      cpuRelax()
      continue

    when defined lowEnergy:
      lastWorkAt = now

    item = move chan.data[i]

    if not item.m.stop.load(moRelaxed):
      try:
        atomicInc busyThreads
        item.t.invoke(item.result)
        atomicDec busyThreads
        discard chan.board[i].compareExchange(wip,
          tsDone, moRelease, moRelease)
      except:
        discard chan.board[i].compareExchange(wip,
          tsFail, moRelease, moRelease)
        if item.m.error.load(moRelaxed).len == 0:
          let e = getCurrentException()
          item.m.error.store(
            "SPAWN FAILURE: [" & $e.name & "] " & e.msg & "\n" & getStackTrace(e),
            moRelaxed
          )

    # but mark it as completed either way!
    taskCompleted item.m[]

proc setup() =
  chan.L.store           false, moRelaxed
  globalStopToken.store  false, moRelaxed
  curretThread.store high(thr), moRelaxed
  for i in 0..high(thr):
    chan.board[i].store tsEmpty, moRelaxed
    createThread[int](thr[i], worker, i)

proc panicStop*() =
  ## Stops all threads.
  globalStopToken.store(true, moRelaxed)
  joinThreads(thr)

template spawnImplRes[T](master: var Master; fn: typed; res: T) =
  if stillHaveTime(master):
    if busyThreads.load(moRelaxed) < ThreadPoolSize:
      taskCreated master
      send PoolTask(m: addr(master), t: toTask(fn), result: addr res)
    else:
      res = fn

template spawnImplNoRes(master: var Master; fn: typed) =
  if stillHaveTime(master):
    if busyThreads.load(moRelaxed) < ThreadPoolSize:
      taskCreated master
      send PoolTask(m: addr(master), t: toTask(fn), result: nil)
    else:
      fn

import std / macros

macro spawn*(a: Master; b: untyped) =
  if b.kind in nnkCallKinds and b.len == 3 and b[0].eqIdent("->"):
    result = newCall(bindSym"spawnImplRes", a, b[1], b[2])
  else:
    result = newCall(bindSym"spawnImplNoRes", a, b)

macro checkBody(body: untyped): untyped =
  # We check here for dangerous "too early" access of memory locations that
  # are "already" gone.
  # For example:
  #
  # m.awaitAll:
  #    m.spawn g(i+1) -> resA
  #    m.spawn g(i+1) -> resA # <-- store into the same location without protection!

  const DeclarativeNodes = {nnkTypeSection, nnkFormalParams, nnkGenericParams,
    nnkMacroDef, nnkTemplateDef, nnkConstSection, nnkConstDef,
    nnkIncludeStmt, nnkImportStmt,
    nnkExportStmt, nnkPragma, nnkCommentStmt,
    nnkTypeOfExpr, nnkMixinStmt, nnkBindStmt}

  const BranchingNodes = {nnkIfStmt, nnkCaseStmt}

  proc isSpawn(n: NimNode): bool =
    n.eqIdent("spawn") or (n.kind == nnkDotExpr and n[1].eqIdent("spawn"))

  proc check(n: NimNode; exprs: var seq[NimNode]; withinLoop: bool) =
    if n.kind in nnkCallKinds and isSpawn(n[0]):
      let b = n[^1]
      for i in 1 ..< n.len:
        check n[i], exprs, withinLoop
      if b.kind in nnkCallKinds and b.len == 3 and b[0].eqIdent("->"):
        let dest = b[2]
        exprs.add dest
        if withinLoop and dest.kind in {nnkSym, nnkIdent}:
          error("re-use of expression '" & $dest & "' before 'awaitAll' completed", dest)

    elif n.kind in DeclarativeNodes:
      discard "declarative nodes are not interesting"
    else:
      let withinLoopB = withinLoop or n.kind in {nnkWhileStmt, nnkForStmt}
      if n.kind in BranchingNodes:
        let preExprs = exprs[0..^1]
        for child in items(n):
          var branchExprs = preExprs
          check child, branchExprs, withinLoopB
          exprs.add branchExprs[preExprs.len..^1]
      else:
        for child in items(n): check child, exprs, withinLoopB
        for i in 0..<exprs.len:
          # `==` on NimNode checks if nodes are structurally equivalent.
          # Which is exactly what we need here:
          if exprs[i] == n and n.kind in {nnkSym, nnkIdent}:
            error("re-use of expression '" & repr(n) & "' before 'awaitAll' completed", n)

  var exprs: seq[NimNode] = @[]
  check body, exprs, false
  result = body

template awaitAll*(master: var Master; body: untyped) =
  try:
    checkBody body
  finally:
    waitForCompletions(master)

when not defined(maleSkipSetup):
  setup()

include ".." / malebolgia / masterhandles
