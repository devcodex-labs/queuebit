/** Read only bounded metadata fields: listing never fetches the 256 KiB query or business snapshots. */
export const QUERY_SCRIPT = String.raw`
  if request.op=='eventParent' then
    local fields=redis.call('HMGET',key('parent'),'schema','runId','taskName','definitionIdentity')
    if fields[1] then
      if fields[1]~='batch-v1' or fields[2]~=request.runId or not fields[3] or not fields[4] then fail('STORAGE_INCONSISTENT') end
      return {taskName=fields[3],definitionIdentity=fields[4]}
    end
    if redis.call('EXISTS',key('parent'))~=0 then fail('STORAGE_INCONSISTENT') end
    if redis.call('EXISTS',key('event'))~=0 then fail('STORAGE_INCONSISTENT') end
    for _,role in ipairs({'events','dueEvents','dueReplays','leases','gcEvents','dead'}) do
      if redis.call('ZSCORE',key(role),request.eventId) then fail('INDEX_INCONSISTENT') end
    end
    return cjson.null
  end
  if request.op=='deadGet' or request.op=='deadPrepare' or request.op=='deadCheck' then
    local fields={'schema','eventId','runId','batchId','taskName','version','kind','status','sequence','timestamp','revision',
      'deliveryAttempt','replayGeneration','firstDeadAt','deadLetterExpiresAt','firstDeadLetterSequence','replayDrainDeadline',
      'dueAt','deliveredAt','leaseRevision','attemptTimeoutAt','token','lateReplay'}
    local function metadata(role,id)
      local values=redis.call('HMGET',key(role),unpack(fields)); local event={}
      if not values[1] then
        if redis.call('EXISTS',key(role))~=0 then fail('STORAGE_INCONSISTENT') end
        return nil
      end
      for i,field in ipairs(fields) do if not values[i] then fail('STORAGE_INCONSISTENT') end; event[field]=values[i] end
      local runId,page,kind=string.match(id,'^(%x+):(%d+):(%a+)$')
      local statuses={pending=true,retrying=true,delivering=true,delivered=true,dead_letter=true}
      if not runId or #runId~=32 or event.schema~='batch-v1' or event.eventId~=id or event.runId~=runId
        or event.batchId~=runId..':'..page or event.kind~=kind or not statuses[event.status]
        or (event.lateReplay~='0' and event.lateReplay~='1') then fail('STORAGE_INCONSISTENT') end
      for i=9,21 do event[fields[i]]=integer(event[fields[i]]) end
      if event.sequence<1 or event.revision<1 then fail('STORAGE_INCONSISTENT') end
      local deadline=event.token~='' and integer(decode(event.token).deadline) or cjson.null
      return {eventId=id,runId=runId,batchId=event.batchId,taskName=event.taskName,version=event.version,kind=kind,
        status=event.status,sequence=event.sequence,timestamp=event.timestamp,revision=event.revision,
        deliveryAttempt=event.deliveryAttempt,replayGeneration=event.replayGeneration,lateReplay=event.lateReplay=='1',
        firstDeadAt=event.firstDeadAt,deadLetterExpiresAt=event.deadLetterExpiresAt,firstDeadLetterSequence=event.firstDeadLetterSequence,
        replayDrainDeadline=event.replayDrainDeadline>0 and event.replayDrainDeadline or cjson.null,
        dueAt=event.dueAt>0 and event.dueAt or cjson.null,deliveredAt=event.deliveredAt>0 and event.deliveredAt or cjson.null,
        lease={revision=event.leaseRevision,deadline=deadline,attemptTimeoutAt=event.attemptTimeoutAt>0 and event.attemptTimeoutAt or cjson.null}}
    end
    if request.op=='deadGet' then
      local event=metadata('event',request.eventId)
      return event and event.firstDeadAt>0 and event or cjson.null
    end
    if request.expiresAt and now>=integer(request.expiresAt) then fail('CURSOR_EXPIRED') end
    local upper=request.upperSequence or integer(meta.deadLetterSequence)
    local maximum=request.lastSequence and '('..decimal(request.lastSequence) or decimal(upper)
    if request.op=='deadPrepare' then
      local head=redis.call('ZREVRANGE',key('listIndex'),0,0,'WITHSCORES')
      if #head>0 and (not tonumber(head[2]) or tonumber(head[2])>integer(meta.deadLetterSequence)) then fail('INDEX_INCONSISTENT') end
      local flat=redis.call('ZREVRANGEBYSCORE',key('listIndex'),maximum,'-inf','WITHSCORES','LIMIT',0,request.limit*4+1)
      local candidates={}
      for i=1,#flat,2 do
        local score=tonumber(flat[i+1])
        if not score or score<1 or score>MAX or score~=math.floor(score) then fail('INDEX_INCONSISTENT') end
        candidates[#candidates+1]={id=flat[i],score=score}
      end
      return {candidates=candidates,upperSequence=upper,expiresAt=request.expiresAt or add(now,900000)}
    end
    if #request.candidates>request.limit*4+1 then fail('STORAGE_INCONSISTENT') end
    local items={}; local last=request.lastSequence or 0; local previous=nil; local examined=0
    for i,candidate in ipairs(request.candidates) do
      if candidate.score>upper or (previous and candidate.score>=previous) then fail('INDEX_INCONSISTENT') end
      previous=candidate.score; last=candidate.score; examined=i
      local score=redis.call('ZSCORE',key('listIndex'),candidate.id)
      if score then
        local current=integer(tonumber(score)); local event=metadata('listEvent_'..i,candidate.id)
        if not event or event.firstDeadAt<1 or event.firstDeadLetterSequence~=current or current~=candidate.score
          or event.status=='delivered' or (request.filter.taskName and event.taskName~=request.filter.taskName) then fail('INDEX_INCONSISTENT') end
        if event.deadLetterExpiresAt>now then
          items[#items+1]=event; if #items>=request.limit then break end
        end
      end
    end
    local more=examined<#request.candidates
    if not more and last>0 then more=redis.call('ZCOUNT',key('listIndex'),'-inf','('..decimal(last))>0 end
    return {items=items,lastSequence=last,hasMore=more,upperSequence=upper,expiresAt=request.expiresAt}
  end
  if request.op=='eventCandidates' then
    if #request.candidates>protocol.maintenance.batchSize then fail('STORAGE_INCONSISTENT') end
    local matches={}
    for i,candidate in ipairs(request.candidates) do
      local score=redis.call('ZSCORE',key('candidateIndex'),candidate.id)
      if score then
        local fields=redis.call('HMGET',key('candidateEvent_'..i),'schema','eventId','definitionIdentity','taskName','status','dueAt','replayGeneration')
        if fields[1]~='batch-v1' or fields[2]~=candidate.id or not fields[3] or not fields[4]
          or (fields[5]~='pending' and fields[5]~='retrying') or integer(fields[6])~=tonumber(score)
          or (integer(fields[7])>0)~=request.replay then fail('INDEX_INCONSISTENT') end
        if tonumber(score)<=now then matches[#matches+1]={eventId=candidate.id,definitionIdentity=fields[3],taskName=fields[4]} end
      end
    end
    return {candidates=matches}
  end
  if request.op=='verifyAbsentCandidate' then
    if request.indexCount<1 or request.indexCount>2 then fail('STORAGE_INCONSISTENT') end
    if redis.call('EXISTS',key('absentObject'))~=0 then
      if request.missingField and not redis.call('HGET',key('absentObject'),request.missingField) then fail('STORAGE_INCONSISTENT') end
      fail('READ_CONFLICT')
    end
    for i=1,request.indexCount do
      if redis.call('ZSCORE',key('absentIndex_'..i),request.id) then fail('INDEX_INCONSISTENT') end
    end
    return {absent=true}
  end
  if request.op=='capacitySnapshot' or request.op=='healthSample' then
    local ledger=read('capacity')
    for _,field in ipairs(counterFields) do ledger[field]=integer(ledger[field]) end
    local limits=protocol.limits; local partition=limits.memberMax*98304; local business=limits.totalBytes-partition
    if ledger.latch>1 or ledger.nonterminalRunCount>ledger.runCount or ledger.unfinishedEventCount>ledger.eventCount
      or ledger.runCount>limits.runMax or ledger.nonterminalRunCount>limits.nonterminalRunMax or ledger.memberCount>limits.memberMax
      or ledger.definitionCount>limits.definitionMax or ledger.chargedBytes+ledger.reservedBytes>business
      or ledger.runCount+ledger.eventCount+ledger.reservedObjects>limits.objectMax
      or ledger.unfinishedEventCount+ledger.reservedEvents>limits.unfinishedEventMax then fail('STORAGE_INCONSISTENT') end
    local capacity={origin='redis',scope='namespace',sampledAt=now,revision=ledger.revision,newStarts=ledger.latch==1 and 'closed' or 'open',
      counts={runs=ledger.runCount,nonterminalRuns=ledger.nonterminalRunCount,events=ledger.eventCount,unfinishedEvents=ledger.unfinishedEventCount,
        definitions=ledger.definitionCount,members=ledger.memberCount,reservedObjects=ledger.reservedObjects,reservedEvents=ledger.reservedEvents},
      bytes={charged=ledger.chargedBytes,reserved=ledger.reservedBytes,businessLimit=business,memberPartition=partition,memberUsed=ledger.memberCount*98304,totalLimit=limits.totalBytes}}
    if request.op=='capacitySnapshot' then return capacity end
    local sampled=0; local missing=0
    if #request.definitions>protocol.maintenance.batchSize then fail('STORAGE_INCONSISTENT') end
    for i,identity in ipairs(request.definitions) do
      local score=redis.call('ZSCORE',key('definitions'),identity)
      if score then
        local definition=read('sampleDef_'..i)
        if not definition.canonical or integer(definition.catalogSequence)~=tonumber(score) then fail('INDEX_INCONSISTENT') end
        integer(definition.runRefs); integer(definition.runtimeRefs)
        sampled=sampled+1
        if redis.call('ZCOUNT',key('sampleMembers_'..i),'('..decimal(now),'+inf')==0 then missing=missing+1 end
      end
    end
    return {capacity=capacity,definitions={sampled=sampled,withoutMember=missing,complete=sampled==ledger.definitionCount},
      backlog={expiredLeases=redis.call('ZCOUNT',key('leases'),'-inf',decimal(now)),runGcDue=redis.call('ZCOUNT',key('gcRuns'),'-inf',decimal(now)),
        eventGcDue=redis.call('ZCOUNT',key('gcEvents'),'-inf',decimal(now)),pendingEvents=ledger.unfinishedEventCount}}
  end
  if request.op=='getMetadata' or request.op=='listPrepare' or request.op=='listCheck' then
    local fields={'schema','runId','taskName','version','status','reason','revision','page','dispatchCount',
      'businessFailures','scheduledRetries','recoveries','batchFailures','consecutiveRecoveries','createdAt',
      'dueAt','terminalAt','leaseRevision','reservationBytes','reservationEvents','token','createdSequence'}
    local function metadata(role,id)
      local values=redis.call('HMGET',key(role),unpack(fields))
      if not values[1] then
        if redis.call('EXISTS',key(role))~=0 then fail('STORAGE_INCONSISTENT') end
        return nil
      end
      local run={}
      for i,field in ipairs(fields) do if not values[i] then fail('STORAGE_INCONSISTENT') end; run[field]=values[i] end
      if run.schema~='batch-v1' or run.runId~=id then fail('STORAGE_INCONSISTENT') end
      local allowed={pending=true,running=true,retrying=true,blocked=true,pausing=true,paused=true,success=true,failed=true,cancelled=true}
      if not allowed[run.status] then fail('STORAGE_INCONSISTENT') end
      for i=7,20 do run[fields[i]]=integer(run[fields[i]]) end
      local sequence=integer(run.createdSequence)
      if sequence<1 or run.revision<1 or run.page<1 then fail('STORAGE_INCONSISTENT') end
      local deadline=cjson.null
      if run.token~='' then deadline=integer(decode(run.token).deadline) end
      return {runId=id,taskName=run.taskName,version=run.version,status=run.status,reason=run.reason~='' and run.reason or cjson.null,
        revision=run.revision,page=run.page,batchId=id..':'..decimal(run.page),dispatchCount=run.dispatchCount,
        businessFailures=run.businessFailures,scheduledRetries=run.scheduledRetries,recoveries=run.recoveries,
        batchFailures=run.batchFailures,consecutiveRecoveries=run.consecutiveRecoveries,createdAt=run.createdAt,
        dueAt=run.dueAt>0 and run.dueAt or cjson.null,terminalAt=run.terminalAt>0 and run.terminalAt or cjson.null,
        lease={revision=run.leaseRevision,deadline=deadline},budget={reservedBytes=run.reservationBytes,reservedEvents=run.reservationEvents}},sequence
    end
    if request.op=='getMetadata' then return metadata('run',request.runId) or cjson.null end
    if request.expiresAt and now>=integer(request.expiresAt) then fail('CURSOR_EXPIRED') end
    local upper=request.upperSequence or integer(meta.createdSequence)
    local maximum=request.lastSequence and '('..decimal(request.lastSequence) or decimal(upper)
    if request.op=='listPrepare' then
      -- A malformed score beyond the namespace fence must not be hidden as an empty page.
      local head=redis.call('ZREVRANGE',key('listIndex'),0,0,'WITHSCORES')
      if #head>0 and (not tonumber(head[2]) or tonumber(head[2])>integer(meta.createdSequence)) then fail('INDEX_INCONSISTENT') end
      local flat=redis.call('ZREVRANGEBYSCORE',key('listIndex'),maximum,'-inf','WITHSCORES','LIMIT',0,request.limit*4+1)
      local candidates={}
      for i=1,#flat,2 do
        local score=tonumber(flat[i+1])
        if not score or score<1 or score>MAX or score~=math.floor(score) then fail('INDEX_INCONSISTENT') end
        candidates[#candidates+1]={id=flat[i],score=score}
      end
      return {candidates=candidates,upperSequence=upper,expiresAt=request.expiresAt or add(now,900000)}
    end
    if #request.candidates>request.limit*4+1 then fail('STORAGE_INCONSISTENT') end
    local items={}; local last=request.lastSequence or 0; local previous=nil; local examined=0
    for i,candidate in ipairs(request.candidates) do
      if candidate.score>upper or (previous and candidate.score>=previous) then fail('INDEX_INCONSISTENT') end
      previous=candidate.score; last=candidate.score; examined=i
      local score=redis.call('ZSCORE',key('listIndex'),candidate.id)
      if score then
        local current=integer(tonumber(score))
        local run,sequence=metadata('listRun_'..i,candidate.id)
        if not run or sequence~=current or current~=candidate.score
          or (request.filter.taskName and run.taskName~=request.filter.taskName)
          or (request.filter.status and run.status~=request.filter.status) then fail('INDEX_INCONSISTENT') end
        items[#items+1]=run
        if #items>=request.limit then break end
      end
    end
    local more=examined<#request.candidates
    if not more and last>0 then more=redis.call('ZCOUNT',key('listIndex'),'-inf','('..decimal(last))>0 end
    return {items=items,lastSequence=last,hasMore=more,upperSequence=upper,expiresAt=request.expiresAt}
  end
`;
