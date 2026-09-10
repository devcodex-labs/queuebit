/** Shared mutation primitives keep execution, operator and convenience cancellation on one protocol. */
export const RUN_MUTATION_HELPERS = String.raw`
  local function transition(run,status,reason,dueAt,terminalAt)
    write('ZREM','status_'..run.status,run.runId); write('ZREM','taskStatus_'..run.status,run.runId)
    write('ZADD','status_'..status,run.createdSequence,run.runId); write('ZADD','taskStatus_'..status,run.createdSequence,run.runId)
    write('ZREM','due',run.runId); write('ZREM','blocked',run.runId)
    if status=='pending' or status=='retrying' then write('ZADD','due',dueAt,run.runId) end
    if status=='blocked' then write('ZADD','blocked',run.createdSequence,run.runId) end
    if terminalAt>0 then write('ZADD','gcRuns',add(terminalAt,protocol.retention.runMs),run.runId) end
    run.status=status; run.reason=reason; run.dueAt=dueAt; run.terminalAt=terminalAt; run.revision=add(run.revision,1)
  end
  local function releaseReservation(run)
    capacity.reservedBytes=add(capacity.reservedBytes,-run.reservationBytes)
    capacity.reservedObjects=add(capacity.reservedObjects,-run.reservationEvents)
    capacity.reservedEvents=add(capacity.reservedEvents,-run.reservationEvents)
    run.reservationBytes=0; run.reservationEvents=0; run.token=''; run.attemptTimeoutAt=0
    write('ZREM','leases',run.runId)
  end
  local function cancelRun(run)
    capacity.nonterminalRunCount=add(capacity.nonterminalRunCount,-1)
    releaseReservation(run); transition(run,'cancelled','',0,now)
  end
  local function admissionReason(run)
    local declaration=decode(run.definitionCanonical); local events={}
    for _,kind in ipairs(declaration.events) do events[kind]=true end
    local slots=(events.batchSettled and 1 or 0)+((events.success or events.failure) and 1 or 0)
    if redis.call('ZCOUNT',key('definitionMembers'),'('..decimal(now),'+inf')==0 then return 'definition_unavailable' end
    if capacity.chargedBytes+capacity.reservedBytes-run.reservationBytes+131072+slots*131072>businessBytes
      or capacity.runCount+capacity.eventCount+capacity.reservedObjects-run.reservationEvents+slots>limits.objectMax
      or capacity.unfinishedEventCount+capacity.reservedEvents-run.reservationEvents+slots>limits.unfinishedEventMax then return 'capacity' end
    return ''
  end
  local function controlHistory(run,control)
    if type(control)~='table' or encode(control)~=request.controlCanonical
      or type(control.reason)~='string' or type(control.commandId)~='string' then fail('STORAGE_INCONSISTENT') end
    local ring=decode(run.controlRing); local active={}
    if type(ring)~='table' or #ring>32 then fail('STORAGE_INCONSISTENT') end
    for _,receipt in ipairs(ring) do
      if type(receipt)~='table' or type(receipt.request)~='table' or type(receipt.result)~='table'
        or #encode(receipt)>2048 or integer(receipt.expiresAt)-integer(receipt.recordedAt)~=86400000 then fail('STORAGE_INCONSISTENT') end
      if receipt.expiresAt>now then
        active[#active+1]=receipt
        if receipt.request.commandId==control.commandId then
          if encode(receipt.request)~=request.controlCanonical then fail('COMMAND_CONFLICT') end
          return active,receipt.result
        end
      end
    end
    return active,nil
  end
  local function recordControl(run,control,active,result)
    local receipt={request=control,result=result,recordedAt=now,expiresAt=add(now,86400000)}
    if #encode(receipt)>2048 then fail('CONTROL_RECORD_TOO_LARGE') end
    active[#active+1]=receipt; if #active>32 then table.remove(active,1) end
    local audit=decode(run.audit)
    if type(audit)~='table' or #audit>8 then fail('STORAGE_INCONSISTENT') end
    local entry={operation=control.operation,revision=result.revision,recordedAt=now,reason=request.auditReason,truncated=request.auditTruncated}
    if #encode(entry)>1024 then fail('STORAGE_INCONSISTENT') end
    audit[#audit+1]=entry; if #audit>8 then table.remove(audit,1) end
    run.controlRing=encode(active); run.audit=encode(audit)
  end
`;

/** Full-text ring lookup precedes CAS. Nothing in this fragment writes until the enclosing plan succeeds. */
export const CONTROL_SCRIPT = String.raw`
  if request.op=='control' then
    local run=runread()
    if not run then return {kind='not_found',id=request.runId} end
    if run.definitionIdentity~=request.definitionIdentity or run.taskName~=request.taskName then fail('READ_CONFLICT') end
    local control=request.control
    if type(control)~='table' or control.runId~=run.runId or encode(control)~=request.controlCanonical
      or type(control.reason)~='string' or type(control.commandId)~='string' then fail('STORAGE_INCONSISTENT') end
    local active,previous=controlHistory(run,control)
    if previous then return previous end
    if integer(control.expectedRevision)~=run.revision then fail('REVISION_CONFLICT') end
    local terminal=run.status=='success' or run.status=='failed' or run.status=='cancelled'
    local status=run.status; local reason=run.reason; local dueAt=run.dueAt
    local changed=false; local release=false
    local valid=run.token~='' and now<integer(decode(run.token).deadline)
    if not terminal then
      if control.operation=='cancel' then cancelRun(run); changed=true
      elseif control.operation=='pause' then
        status=valid and 'pausing' or 'paused'; reason=''
        changed=status~=run.status or run.reason~=''; release=not valid and run.token~=''
      elseif control.operation=='resume' then
        if run.status=='paused' or run.status=='pausing' or run.status=='blocked' then
          if valid then status='running'; reason=''
          else
            release=run.token~=''
            status=(run.batchFailures>0 or run.consecutiveRecoveries>0 or run.dueAt>now) and 'retrying' or 'pending'; reason=''
            if run.reason=='recovery_exhausted' then run.consecutiveRecoveries=0; changed=true end
            reason=admissionReason(run)
            if reason~='' then status='blocked' end
          end
          changed=changed or status~=run.status or reason~=run.reason or release
        end
      else fail('STORAGE_INCONSISTENT') end
      if control.operation~='cancel' and changed then
        if release then releaseReservation(run) end
        transition(run,status,reason,dueAt,0)
      end
    end
    local result={kind=changed and 'applied' or 'noop',id=run.runId,revision=run.revision,status=run.status,changed=changed}
    recordControl(run,control,active,result)
    sethash('run',run)
    if changed then saveCapacity() end
    return result
  end
`;
