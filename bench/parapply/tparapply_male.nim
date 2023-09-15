## The objective of this benchmark is check how much time it looses
## from running some operation in other thread, instead of using same.
##
## This may be better as a tool to mesure malebolgia evolution than as
## benchmark with other similar frameworks
##
## Flaws:
## parApply has some setup


import std/[times, monotimes]


when defined spinDoctors:
  import malebolgia_spin_doctors
else:
  import malebolgia


const
  bulkSize   {.intdefine.} = 2
  operations {.intdefine.} = 3 * bulkSize
  runs       {.intdefine.} = 1000
  separeator {.strdefine.} = "\t"

var 
  bigbang = getMonoTime()
  epoch   = getMonoTime()


template `@!`[T](data: openArray[T]; i: int): untyped =
  cast[ptr UncheckedArray[T]](addr data[i])

template parApply[T](data: var openArray[T]; bulkSize: int; op: untyped) =
  ##[ Applies `op` on every element of `data`, `op(data[i)` in parallel.
  `bulkSize` specifies how many elements are processed sequentially
  and not in parallel because sending a task to a different thread
  is expensive. `bulkSize` should probably be larger than you think but
  it depends on how expensive the operation `op` is.]##
  proc worker(a: ptr UncheckedArray[T]; until: int) =
    for i in 0..<until: op a[i]

  var m = createMaster()
  m.awaitAll:
    var i = 0
    ## We reset epoch get values without createMaster/waitAll interference
    epoch   = getMonoTime()
    ##
    while i+bulkSize <= data.len:
      m.spawn worker(data@!i, bulkSize)
      i += bulkSize
    if i < data.len:
      m.spawn worker(data@!i, data.len-i)

proc ifmt(i: int): string =
  if i < 10:
    return "000" & $i
  if i < 100:
    return "00" & $i
  if i < 1000:
    return "0" & $i
  return $i

template benchParApply*(body: untyped) =
  proc doIt(i: var MonoTime) {.inline.} =
    body
    i = getMonoTime()
  
  let last = bulkSize - 1
  let sep = separeator
  var ops: array[operations, MonoTime]
  var header = "Setup" & sep & "Serial" & sep & "Spwn" & 0.ifmt & "E0 - Epoch"
  
  for i in 1..(high(ops) div bulkSize):
    header &= sep & "Spwn" & i.ifmt  & "E0 - Epoch"
  
  for i in 1..(high(ops) div bulkSize):
    header &= sep & "Spwn" & i.ifmt  & "E0 - Spwn" & ifmt(i - 1) & "E" & $last
  
  echo $header
  
  for i in 0..<runs:
    for i in 0..high(ops):
      ops[i] = epoch
  
    bigbang = getMonoTime()
    parApply ops, bulkSize, doIt
  
    var row =    $inNanoseconds(epoch  - bigbang)   # SETUP
    row &= sep & $inNanoseconds(ops[1] - ops[0])    # how fast we perform in serial
    row &= sep & $inNanoseconds(ops[0] - epoch)     # from start
  
    for i in 1..(high(ops) div bulkSize):
      let curr = i * bulkSize
      row &= sep & $inNanoseconds(ops[curr] - epoch    )  # from epoch op
    for i in 1..(high(ops) div bulkSize):
      let curr = i * bulkSize
      let prev = curr - 1
      row &= sep & $inNanoseconds(ops[curr] - ops[prev])  # from prev  op
  
    echo row
 
when isMainModule:
  benchParApply:
    # not so heavy workload
    discard
