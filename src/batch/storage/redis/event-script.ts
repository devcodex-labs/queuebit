/** Callback grants and settlement share one Run lock, but never the Run's business attempt budget. */
export const EVENT_SCRIPT = String.raw`
  if request.op=='getEvent' or request.op=='claimEvent' or request.op=='renewEvent' or request.op=='settleEvent' or request.op=='recoverEvent'
    or request.op=='replayEvent' or request.op=='gcEvent' then
    local run=runread(); local active
    if request.op=='replayEvent' then
      if not run then return {kind='not_found',id=request.eventId} end
      if type(request.control)~='table' or request.control.operation~='replay' or request.control.eventId~=request.eventId then fail('STORAGE_INCONSISTENT') end
      local previous; active,previous=controlHistory(run,request.control)
      if previous then return previous end
    end
    local event=read('event')
    if not event.eventId then
      if next(event) then fail('STORAGE_INCONSISTENT') end
      for _,role in ipairs({'events','dueEvents','dueReplays','leases','gcEvents','dead','taskDead'}) do
        if redis.call('ZSCORE',key(role),request.eventId) then fail('INDEX_INCONSISTENT') end
      end
      if request.op=='renewEvent' or request.op=='settleEvent' then fail('LEASE_LOST') end
      if request.op=='recoverEvent' then return {changed=false} end
      if request.op=='replayEvent' then return {kind='not_found',id=request.eventId} end
      if request.op=='gcEvent' then
        return {changed=false}
      end
      return cjson.null
    end
    if not run or event.schema~='batch-v1' or event.eventId~=request.eventId or event.runId~=run.runId
      or event.queryRef~=run.runId or event.taskName~=run.taskName or event.version~=run.version
      or event.definitionIdentity~=run.definitionIdentity or event.batchId~=run.runId..':'..decimal(request.page)
      or event.kind~=request.eventKind then fail('STORAGE_INCONSISTENT') end
    local definition=read('definition')
    if definition.canonical~=run.definitionCanonical then fail('STORAGE_INCONSISTENT') end
    for _,field in ipairs({'sequence','timestamp','revision','deliveryAttempt','replayGeneration','firstDeadAt',
      'deadLetterExpiresAt','firstDeadLetterSequence','replayDrainDeadline','deliveredAt','dueAt','leaseRevision','attemptTimeoutAt'}) do event[field]=integer(event[field]) end
    for _,field in ipairs({'token','lateReplay','transportReceipt','deliveryError'}) do if type(event[field])~='string' then fail('STORAGE_INCONSISTENT') end end
    local statuses={pending=true,retrying=true,delivering=true,delivered=true,dead_letter=true}
    if not statuses[event.status] or event.sequence<1 or event.sequence>run.eventSequence or event.revision<1
      or (event.lateReplay~='0' and event.lateReplay~='1') then fail('STORAGE_INCONSISTENT') end
    checkBusinessJson(event.state,65536); checkBusinessJson(event.error,32768); checkBusinessJson(event.deliveryError,2048)
    local lock=read('eventLock')
    if next(lock) and (type(lock.eventId)~='string' or type(lock.token)~='string') then fail('STORAGE_INCONSISTENT') end
    if request.op=='getEvent' then return {event=event,query=run.query} end
    local replay=event.replayGeneration>0
    local unfinished=event.status=='pending' or event.status=='retrying' or event.status=='delivering'
    local dueRole=replay and 'dueReplays' or 'dueEvents'
    if request.op=='replayEvent' then
      if integer(request.control.expectedRevision)~=event.revision then fail('REVISION_CONFLICT') end
      local changed=event.status=='dead_letter' and now<event.deadLetterExpiresAt
      local result={kind='noop',id=event.eventId,revision=event.revision,status=event.status,changed=false}
      if changed then
        if event.token~='' then fail('STORAGE_INCONSISTENT') end
        event.replayGeneration=add(event.replayGeneration,1); event.deliveryAttempt=0; event.revision=add(event.revision,1)
        event.status='pending'; event.lateReplay='1'; event.dueAt=now; event.replayDrainDeadline=0; event.deliveryError='null'
        run.callbackDeadLetters=add(run.callbackDeadLetters,-1); run.callbackPending=add(run.callbackPending,1)
        capacity.unfinishedEventCount=add(capacity.unfinishedEventCount,1); checkCapacity()
        result={kind='applied',id=event.eventId,eventId=event.eventId,revision=event.revision,status='pending',changed=true,replayGeneration=event.replayGeneration}
      end
      recordControl(run,request.control,active,result)
      if changed then
        sethash('event',event); write('ZADD','dueReplays',now,event.eventId); saveCapacity()
      end
      sethash('run',run); return result
    end
    if request.op=='gcEvent' then
      local expired=event.firstDeadAt>0 and now>=event.deadLetterExpiresAt
      local deliveredDue=event.status=='delivered' and now>=add(event.deliveredAt,protocol.retention.deliveredEventMs)
      if not expired and not deliveredDue then return {changed=false} end
      if not expired and unfinished then return {changed=false} end
      if event.token~='' then
        local token=decode(event.token)
        if event.status~='delivering' or lock.eventId~=event.eventId or lock.token~=event.token then fail('STORAGE_INCONSISTENT') end
        if now<integer(token.deadline) and now<event.attemptTimeoutAt then
          if not replay or event.replayDrainDeadline~=integer(token.deadline) then fail('STORAGE_INCONSISTENT') end
          write('ZADD','gcEvents',event.replayDrainDeadline,event.eventId); return {changed=false}
        end
        write('DEL','eventLock')
      elseif lock.eventId==event.eventId then fail('STORAGE_INCONSISTENT') end
      local sequence=redis.call('ZSCORE',key('events'),event.eventId)
      if not sequence or tonumber(sequence)~=event.sequence then fail('INDEX_INCONSISTENT') end
      if not redis.call('ZSCORE',key('gcEvents'),event.eventId) then fail('INDEX_INCONSISTENT') end
      local function exactIndex(role,expected)
        local score=redis.call('ZSCORE',key(role),event.eventId)
        if expected then
          if not score or tonumber(score)~=expected then fail('INDEX_INCONSISTENT') end
        elseif score then fail('INDEX_INCONSISTENT') end
      end
      exactIndex('dueEvents',not replay and event.dueAt>0 and event.dueAt or nil)
      exactIndex('dueReplays',replay and event.dueAt>0 and event.dueAt or nil)
      exactIndex('leases',event.token~='' and integer(decode(event.token).deadline) or nil)
      local deadScore=event.firstDeadAt>0 and event.status~='delivered' and event.firstDeadLetterSequence or nil
      exactIndex('dead',deadScore); exactIndex('taskDead',deadScore)
      if unfinished then
        run.callbackPending=add(run.callbackPending,-1); capacity.unfinishedEventCount=add(capacity.unfinishedEventCount,-1)
      elseif event.status=='delivered' then run.callbackDelivered=add(run.callbackDelivered,-1)
      else run.callbackDeadLetters=add(run.callbackDeadLetters,-1) end
      run.eventRefCount=add(run.eventRefCount,-1); capacity.eventCount=add(capacity.eventCount,-1); capacity.chargedBytes=add(capacity.chargedBytes,-131072)
      for _,role in ipairs({'events','dueEvents','dueReplays','leases','gcEvents','dead','taskDead'}) do write('ZREM',role,event.eventId) end
      write('DEL','event'); sethash('run',run); saveCapacity(); return {changed=true}
    end
    if event.transportReceipt~='' then
      local previous=decode(event.transportReceipt)
      if previous.commandId==request.commandId then
        if previous.op~=request.op or previous.requestDigest~=redis.sha1hex(raw) then fail('COMMAND_CONFLICT') end
        if request.op=='claimEvent' or request.op=='renewEvent' then
          if event.token~=previous.result.token or event.token=='' or now>=integer(decode(event.token).deadline)
            or lock.eventId~=event.eventId or lock.token~=event.token then fail('LEASE_LOST') end
          if request.op=='claimEvent' then return {token=event.token,event=event,query=run.query,now=now} end
        end
        if request.op=='renewEvent' then
          if replay and now>=event.deadLetterExpiresAt then fail('LEASE_LOST') end
          return {token=event.token,now=now,replayDrainDeadline=event.replayDrainDeadline}
        end
        return previous.result
      end
    end
    local function receipt(result)
      local encoded=encode({commandId=request.commandId,op=request.op,requestDigest=redis.sha1hex(raw),result=result})
      if #encoded>2048 then fail('STORAGE_INCONSISTENT') end
      sethash('event',{transportReceipt=encoded}); return result
    end
    local function release()
      if lock.eventId~=event.eventId or lock.token~=event.token then fail('STORAGE_INCONSISTENT') end
      event.token=''; event.attemptTimeoutAt=0
      write('DEL','eventLock'); write('ZREM','leases',event.eventId)
    end
    local function finish(status)
      if not replay then
        if event.sequence~=run.normalEventCursor+1 then fail('STORAGE_INCONSISTENT') end
        run.normalEventCursor=event.sequence
      end
      run.callbackPending=add(run.callbackPending,-1)
      capacity.unfinishedEventCount=add(capacity.unfinishedEventCount,-1)
      event.status=status; event.dueAt=0
      write('ZREM',dueRole,event.eventId)
      if status=='delivered' then
        event.deliveredAt=now; run.callbackDelivered=add(run.callbackDelivered,1)
        local gcAt=add(now,protocol.retention.deliveredEventMs)
        if replay then gcAt=math.min(gcAt,event.deadLetterExpiresAt) end
        write('ZADD','gcEvents',gcAt,event.eventId); write('ZREM','dead',event.eventId); write('ZREM','taskDead',event.eventId)
      else
        run.callbackDeadLetters=add(run.callbackDeadLetters,1)
        if event.firstDeadAt==0 then
          event.firstDeadAt=now; event.deadLetterExpiresAt=add(now,protocol.retention.deadLetterMs)
          meta.deadLetterSequence=add(integer(meta.deadLetterSequence),1); event.firstDeadLetterSequence=meta.deadLetterSequence
          sethash('meta',{deadLetterSequence=meta.deadLetterSequence})
        end
        write('ZADD','dead',event.firstDeadLetterSequence,event.eventId); write('ZADD','taskDead',event.firstDeadLetterSequence,event.eventId)
        write('ZADD','gcEvents',event.deadLetterExpiresAt,event.eventId)
      end
    end
    if request.op=='claimEvent' then
      if event.status~='pending' and event.status~='retrying' then return cjson.null end
      if event.dueAt>now or (not replay and event.sequence~=run.normalEventCursor+1) or next(lock)
        or (replay and now>=event.deadLetterExpiresAt) then return cjson.null end
      if event.deliveryAttempt>=protocol.callback.attempts then fail('STORAGE_INCONSISTENT') end
      local runtime=read('runtime')
      local membership=redis.call('ZSCORE',key('definitionMembers'),request.runtimeId)
      if runtime.runtimeId~=request.runtimeId or integer(runtime.generation)~=request.generation or runtime.ready~='1'
        or integer(runtime.deadline)<=now or not membership or tonumber(membership)<=now then fail('LEASE_LOST') end
      local registered=false
      for _,identity in ipairs(decode(runtime.registered)) do if identity==run.definitionIdentity then registered=true end end
      if not registered then fail('LEASE_LOST') end
      event.deliveryAttempt=add(event.deliveryAttempt,1); event.leaseRevision=add(event.leaseRevision,1); event.revision=add(event.revision,1)
      event.attemptTimeoutAt=add(now,protocol.callback.timeoutMs)
      local deadline=math.min(add(now,protocol.lease.leaseMs),event.attemptTimeoutAt)
      event.replayDrainDeadline=0
      if replay and deadline>=event.deadLetterExpiresAt then event.replayDrainDeadline=deadline end
      event.token=encode({eventId=event.eventId,runId=run.runId,definitionIdentity=run.definitionIdentity,runtimeId=request.runtimeId,
        generation=request.generation,nonce=request.nonce,leaseRevision=event.leaseRevision,deadline=deadline})
      event.status='delivering'; event.dueAt=0
      sethash('event',event); sethash('eventLock',{eventId=event.eventId,token=event.token})
      write('ZREM',dueRole,event.eventId); write('ZADD','leases',deadline,event.eventId)
      receipt({token=event.token}); return {token=event.token,event=event,query=run.query,now=now}
    end
    if event.token=='' then
      if request.op=='recoverEvent' then return {changed=false} end
      fail('LEASE_LOST')
    end
    local token=decode(event.token)
    if event.status~='delivering' or lock.eventId~=event.eventId or lock.token~=event.token then fail('STORAGE_INCONSISTENT') end
    if request.op=='recoverEvent' then
      if replay and now>=event.deadLetterExpiresAt then return {changed=false} end
      if now<integer(token.deadline) then return {changed=false} end
    elseif event.token~=request.token or now>=integer(token.deadline) then fail('LEASE_LOST') end
    if request.op=='renewEvent' then
      if replay and now>=event.deadLetterExpiresAt then fail('LEASE_LOST') end
      event.leaseRevision=add(event.leaseRevision,1); token.leaseRevision=event.leaseRevision
      token.deadline=event.replayDrainDeadline>0 and event.replayDrainDeadline or math.min(add(now,protocol.lease.leaseMs),event.attemptTimeoutAt)
      if token.deadline<=now then fail('LEASE_LOST') end
      if replay and event.replayDrainDeadline==0 and token.deadline>=event.deadLetterExpiresAt then event.replayDrainDeadline=token.deadline end
      event.token=encode(token)
      sethash('event',{token=event.token,leaseRevision=event.leaseRevision,replayDrainDeadline=event.replayDrainDeadline})
      sethash('eventLock',{eventId=event.eventId,token=event.token}); write('ZADD','leases',token.deadline,event.eventId)
      receipt({token=event.token}); return {token=event.token,now=now,replayDrainDeadline=event.replayDrainDeadline}
    end
    local success=request.op=='settleEvent' and request.kind=='success' and now<event.attemptTimeoutAt
    if request.op=='settleEvent' and request.kind~='success' and request.kind~='failure' and request.kind~='timeout' then fail('STORAGE_INCONSISTENT') end
    if success then event.deliveryError='null'; finish('delivered')
    else
      event.deliveryError=(now>=event.attemptTimeoutAt or request.kind=='timeout') and request.timeoutError or request.error
      checkBusinessJson(event.deliveryError,2048)
      if event.deliveryAttempt>=protocol.callback.attempts or (replay and now>=event.deadLetterExpiresAt) then finish('dead_letter')
      else event.status='retrying'; event.dueAt=add(now,integer(request.retryDelay)); write('ZADD',dueRole,event.dueAt,event.eventId) end
    end
    event.revision=add(event.revision,1); release(); sethash('event',event); sethash('run',run); saveCapacity()
    return receipt({changed=true,status=event.status,revision=event.revision})
  end
`;
