switch("path", "$projectDir/../../src")
switch("path", "$projectDir/../../src/experiment")

var binPath = "$projectDir/bin/"

const
  bulkSize       {.intdefine.} = 2
  operations     {.intdefine.} = 3 * bulkSize
  runs           {.intdefine.} = 1000
  ThreadPoolSize {.intdefine.} = 8

let spawns = operations div bulkSize

binPath = binPath &
  $operations     & "ops_"     &
  $spawns         & "spawns_"  &
  $ThreadPoolSize & "threads_" &
  $runs           & "times"

binPath = binPath & "/parApply"

when defined spinDoctors:
  binPath = binPath & "SpinDoctors"
  when defined lowEnergy:
    binPath = binPath & "LowEnergy"


switch("out", binPath)
