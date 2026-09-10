/** Maintenance is a bounded set of exact-key transactions, never a namespace scan or TTL deletion. */
export const MAINTENANCE_SCRIPT = String.raw`
  if request.op=='gcDefinition' then
    local definition=read('definition')
    if not definition.canonical then
      if next(definition) then fail('STORAGE_INCONSISTENT') end
      if redis.call('ZSCORE',key('definitions'),request.definitionIdentity) or redis.call('ZSCORE',key('gcDefinitions'),request.definitionIdentity) then fail('INDEX_INCONSISTENT') end
      return {changed=false}
    end
    local runRefs=integer(definition.runRefs); local runtimeRefs=integer(definition.runtimeRefs)
    if runRefs>0 or runtimeRefs>0 then
      write('ZREM','gcDefinitions',request.definitionIdentity)
      return {changed=false}
    end
    if redis.call('ZCARD',key('due'))~=0 or redis.call('ZCARD',key('blocked'))~=0
      or redis.call('ZCARD',key('definitionMembers'))~=0 then fail('STORAGE_INCONSISTENT') end
    local score=redis.call('ZSCORE',key('definitions'),request.definitionIdentity)
    if not score or tonumber(score)~=integer(definition.catalogSequence) then fail('INDEX_INCONSISTENT') end
    capacity.definitionCount=add(capacity.definitionCount,-1); capacity.chargedBytes=add(capacity.chargedBytes,-16384)
    write('DEL','definition'); write('ZREM','definitions',request.definitionIdentity); write('ZREM','gcDefinitions',request.definitionIdentity)
    saveCapacity(); return {changed=true}
  end
  if request.op=='gcRun' or request.op=='maintainRun' then
    local run=runread()
    if not run then return {changed=false} end
    if run.definitionIdentity~=request.definitionIdentity or run.taskName~=request.taskName then fail('READ_CONFLICT') end
    local definition=read('definition')
    if definition.canonical~=run.definitionCanonical or integer(definition.runRefs)<1 then fail('STORAGE_INCONSISTENT') end
    integer(definition.runtimeRefs); integer(definition.catalogSequence)
    if request.op=='maintainRun' then
      if run.status~='pending' and run.status~='retrying' and run.status~='blocked' then return {changed=false} end
      if run.reason=='recovery_exhausted' then return {changed=false} end
      if run.token~='' or run.reservationBytes~=0 or run.reservationEvents~=0 then fail('STORAGE_INCONSISTENT') end
      local reason=admissionReason(run)
      local status=run.status
      if reason~='' then status='blocked'
      elseif status=='blocked' then status=(run.batchFailures>0 or run.consecutiveRecoveries>0 or run.dueAt>now) and 'retrying' or 'pending' end
      if run.status==status and run.reason==reason then return {changed=false} end
      transition(run,status,reason,run.dueAt,0); sethash('run',run)
      return {changed=true}
    end
    local terminal=run.status=='success' or run.status=='failed' or run.status=='cancelled'
    if not terminal or now<add(run.terminalAt,protocol.retention.runMs) then return {changed=false} end
    if run.eventRefCount>0 then
      -- Retained Events protect this Run, but must not monopolize every GC page's head.
      write('ZADD','gcRuns',add(now,protocol.lease.heartbeatMs),run.runId)
      return {changed=false}
    end
    if run.token~='' or run.reservationBytes~=0 or run.reservationEvents~=0
      or redis.call('EXISTS',key('eventLock'))~=0 or redis.call('ZCARD',key('events'))~=0 then fail('STORAGE_INCONSISTENT') end
    for _,role in ipairs({'due','blocked','leases'}) do if redis.call('ZSCORE',key(role),run.runId) then fail('INDEX_INCONSISTENT') end end
    for _,role in ipairs({'runs','taskRuns','status_'..run.status,'taskStatus_'..run.status}) do
      local score=redis.call('ZSCORE',key(role),run.runId)
      if not score or tonumber(score)~=run.createdSequence then fail('INDEX_INCONSISTENT') end
      write('ZREM',role,run.runId)
    end
    if run.idempotencyKey~='' then
      if not request.keys.idem or key('idem')~=run.idempotencyKey or redis.call('GET',key('idem'))~=run.runId then fail('STORAGE_INCONSISTENT') end
      write('DEL','idem')
    end
    definition.runRefs=add(integer(definition.runRefs),-1); sethash('definition',definition)
    if definition.runRefs==0 and integer(definition.runtimeRefs)==0 then write('ZADD','gcDefinitions',now,run.definitionIdentity) end
    capacity.runCount=add(capacity.runCount,-1)
    capacity.chargedBytes=add(capacity.chargedBytes,-98304-#run.query-#run.state-#run.error)
    write('DEL','run'); write('DEL','events'); write('DEL','eventLock'); write('ZREM','gcRuns',run.runId)
    saveCapacity(); return {changed=true}
  end
`;
