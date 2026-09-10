import { createHash } from 'node:crypto';
import { CONTROL_SCRIPT, RUN_MUTATION_HELPERS } from './control-script.js';
import { QUERY_SCRIPT } from './query-script.js';
import { MAINTENANCE_SCRIPT } from './maintenance-script.js';
import { EVENT_SCRIPT } from './event-script.js';

/** Static batch-v1 script. Plan all mutations after type, schema, identity and capacity reads. */
export const BATCH_SCRIPT = String.raw`
local raw = ARGV[1]
local ok, request = pcall(cjson.decode, raw)
local MAX = 9007199254740991
local writes = {}
local plannedHashes = {}
local function fail(code) error(code, 0) end
local function integer(value)
  local number = tonumber(value)
  if not number or number < 0 or number > MAX or number ~= math.floor(number) then fail('STORAGE_INCONSISTENT') end
  if type(value)=='string' and string.format('%.0f',number)~=value then fail('STORAGE_INCONSISTENT') end
  return number
end
local function decimal(value) return string.format('%.0f',integer(value)) end
-- cjson's default 14 significant digits and Lua tostring lose safe integer identity.
-- Only protocol integers pass here; business JSON remains an untouched canonical string.
local function encode(value)
  if type(value)=='number' then return decimal(value) end
  if type(value)=='string' then return (string.gsub(cjson.encode(value),'\\/','/')) end
  if type(value)~='table' then return cjson.encode(value) end
  local count=0; local array=true; local fields={}
  for field,_ in pairs(value) do
    count=count+1
    if type(field)~='number' or field<1 or field~=math.floor(field) then array=false end
    fields[#fields+1]=field
  end
  local parts={}
  if array then
    for i=1,count do if value[i]==nil then fail('STORAGE_INCONSISTENT') end; parts[i]=encode(value[i]) end
    return '['..table.concat(parts,',')..']'
  end
  table.sort(fields)
  for _,field in ipairs(fields) do parts[#parts+1]=cjson.encode(field)..':'..encode(value[field]) end
  return '{'..table.concat(parts,',')..'}'
end
local function add(value, delta)
  if delta>0 and value>MAX-delta then fail('SEQUENCE_EXHAUSTED') end
  local result = value + delta
  if result < 0 or result > MAX or result ~= math.floor(result) then fail('SEQUENCE_EXHAUSTED') end
  return result
end
local function key(role)
  local index = request.keys[role]
  if type(index) ~= 'number' or not KEYS[index] then fail('STORAGE_INCONSISTENT') end
  return KEYS[index]
end
local function read(role)
  local flat = redis.call('HGETALL', key(role))
  local result = {}
  for i=1,#flat,2 do result[flat[i]] = flat[i+1] end
  return result
end
local function sethash(role, fields)
  local args = {'HSET', key(role)}
  local merged=plannedHashes[role] or read(role)
  for field,value in pairs(fields) do
    if type(value) ~= 'string' and type(value) ~= 'number' then fail('STORAGE_INCONSISTENT') end
    args[#args+1] = field
    args[#args+1] = type(value)=='number' and decimal(value) or value
    merged[field]=args[#args]
  end
  plannedHashes[role]=merged
  local bytes=0
  local isEvent=role=='event' or string.match(role,'^event_')
  for field,value in pairs(merged) do
    if isEvent and (field=='state' or field=='error') then bytes=bytes+#field
    elseif role~='run' or (field~='query' and field~='state' and field~='error' and field~='controlRing' and field~='audit') then
      bytes=bytes+#field+#value
    end
  end
  if role=='run' then
    if bytes>16384 then fail('STORAGE_INCONSISTENT') end
    local ring=merged.controlRing or '[]'; local audit=merged.audit or '[]'
    if #ring>32*2048+33 or #audit>8*1024+9 then fail('STORAGE_INCONSISTENT') end
    -- Count actual textual index identities/scores and reserve a bounded event-lock header.
    local indexes=1024
    for _,indexRole in ipairs({'runs','taskRuns','status_'..merged.status,'taskStatus_'..merged.status,'due','blocked','leases','gcRuns','events','eventLock','idem'}) do
      if request.keys[indexRole] then indexes=indexes+#key(indexRole)+32+16 end
    end
    if bytes+#ring+#audit+indexes>98304 then fail('STORAGE_INCONSISTENT') end
  elseif isEvent then
    local indexes=0
    for _,indexRole in ipairs({'events','dueEvents','dueReplays','leases','gcEvents','dead','taskDead'}) do
      if request.keys[indexRole] then indexes=indexes+#key(indexRole)+#(merged.eventId or '')+16 end
    end
    if bytes+indexes>32768 or not merged.state or #merged.state>65536 or not merged.error or #merged.error>32768 then fail('STORAGE_INCONSISTENT') end
  elseif role=='eventLock' then
    if bytes>1024 then fail('STORAGE_INCONSISTENT') end
  elseif role=='definition' or string.match(role,'^registrationDef_') then
    if bytes>16384 or (merged.canonical and #merged.canonical>8192) then fail('STORAGE_INCONSISTENT') end
  elseif role=='runtime' then
    if bytes>65536 or (merged.manifest and #merged.manifest>65536) then fail('STORAGE_INCONSISTENT') end
  end
  writes[#writes+1] = args
end
local function write(command, role, ...)
  local args={command,key(role),...}
  for i=3,#args do if type(args[i])=='number' then args[i]=decimal(args[i]) end end
  writes[#writes+1] = args
end
local function decode(value)
  if type(value) ~= 'string' then fail('STORAGE_INCONSISTENT') end
  local success, parsed = pcall(cjson.decode, value)
  if not success then fail('STORAGE_INCONSISTENT') end
  return parsed
end
local function checkBusinessJson(value, limit)
  if type(value)~='string' or #value>limit then fail('STORAGE_INCONSISTENT') end
  -- JavaScript JSON preserves escaped UTF-16 units; Redis cjson rejects lone surrogates.
  -- This is a validation-only copy. Persisted query/state/error bytes are never rewritten.
  -- Even an escaped literal backslash remains a valid string after this substitution.
  decode((string.gsub(value,'\\u[dD][89aAbBcCdDeEfF]%x%x','\\ufffd')))
end
local counterFields = {'runCount','nonterminalRunCount','eventCount','unfinishedEventCount','definitionCount',
  'memberCount','chargedBytes','reservedBytes','reservedObjects','reservedEvents','latch','revision'}
local runCounters = {'revision','page','dispatchCount','businessFailures','scheduledRetries','recoveries',
  'batchFailures','consecutiveRecoveries','createdAt','dueAt','terminalAt','eventRefCount','createdSequence',
  'leaseRevision','reservationBytes','reservationEvents','eventSequence','attemptTimeoutAt',
  'normalEventCursor','callbackPending','callbackDelivered','callbackDeadLetters'}
local function runread()
  local run = read('run')
  if not run.runId then return nil end
  if run.runId ~= request.runId or run.schema ~= 'batch-v1' then fail('STORAGE_INCONSISTENT') end
  for _,field in ipairs(runCounters) do run[field] = integer(run[field]) end
  for _,field in ipairs({'taskName','version','definitionIdentity','definitionCanonical','policyCanonical','query','state','error','status','reason','token','idempotencyKey'}) do
    if type(run[field]) ~= 'string' then fail('STORAGE_INCONSISTENT') end
  end
  if not request.keys['status_' .. run.status] then fail('STORAGE_INCONSISTENT') end
  checkBusinessJson(run.query,262144); checkBusinessJson(run.state,65536); checkBusinessJson(run.error,32768); decode(run.policyCanonical)
  return run
end
local function execute()
  if not ok or type(request) ~= 'table' or type(request.keys) ~= 'table' or type(request.types) ~= 'table' then fail('STORAGE_INCONSISTENT') end
  if #KEYS > 1024 or #raw > 2097152 then fail('STORAGE_INCONSISTENT') end
  for i,redisKey in ipairs(KEYS) do
    local expected = request.types[i]
    local actual = redis.call('TYPE', redisKey).ok
    if expected ~= 'hash' and expected ~= 'zset' and expected ~= 'string' then fail('STORAGE_INCONSISTENT') end
    if actual ~= 'none' and actual ~= expected then
      if request.op=='listPrepare' or request.op=='listCheck' or request.op=='deadPrepare' or request.op=='deadCheck' or request.op=='eventCandidates' then fail('INDEX_INCONSISTENT') end
      fail('STORAGE_INCONSISTENT')
    end
  end
  local time = redis.call('TIME')
  local now = integer(time[1]) * 1000 + math.floor(integer(time[2]) / 1000)
  local meta = read('meta')
  if request.op == 'init' then
    if meta.schema then
      if meta.schema ~= 'batch-v1' or meta.protocolCanonical ~= request.protocolCanonical then fail('SCHEMA_MISMATCH') end
      if meta.status ~= 'ready' then fail('STORAGE_INCONSISTENT') end
      local capacity = read('capacity')
      for _,field in ipairs(counterFields) do integer(capacity[field]) end
      integer(meta.createdSequence); integer(meta.deadLetterSequence); integer(meta.definitionSequence)
      return {kind='already_applied'}
    end
    if next(meta) or redis.call('EXISTS', key('capacity')) ~= 0 then fail('NAMESPACE_ORPHANED') end
    sethash('meta', {schema='batch-v1', protocolCanonical=request.protocolCanonical, protocolDigest=request.protocolDigest,
      status='ready', createdSequence=0, deadLetterSequence=0, definitionSequence=0})
    local capacity = {}
    for _,field in ipairs(counterFields) do capacity[field] = 0 end
    sethash('capacity', capacity)
    return {kind='applied'}
  end
  if meta.schema ~= 'batch-v1' or meta.protocolCanonical ~= request.protocolCanonical then fail('SCHEMA_MISMATCH') end
  if meta.status ~= 'ready' then fail('STORAGE_INCONSISTENT') end
  local protocol = decode(meta.protocolCanonical)
${QUERY_SCRIPT}
  local capacity = read('capacity')
  for _,field in ipairs(counterFields) do capacity[field] = integer(capacity[field]) end
  if capacity.latch > 1 then fail('STORAGE_INCONSISTENT') end
  local limits = protocol.limits
  local businessBytes = limits.totalBytes - limits.memberMax * 98304
  local function checkCapacity()
    if capacity.runCount > limits.runMax or capacity.nonterminalRunCount > limits.nonterminalRunMax
      or capacity.runCount + capacity.eventCount + capacity.reservedObjects > limits.objectMax
      or capacity.unfinishedEventCount + capacity.reservedEvents > limits.unfinishedEventMax
      or capacity.definitionCount > limits.definitionMax or capacity.memberCount > limits.memberMax
      or capacity.chargedBytes + capacity.reservedBytes > businessBytes then fail('CAPACITY_EXCEEDED') end
  end
  local function saveCapacity()
    checkCapacity()
    local usage = {capacity.runCount, capacity.nonterminalRunCount,
      capacity.runCount+capacity.eventCount+capacity.reservedObjects,
      capacity.unfinishedEventCount+capacity.reservedEvents, capacity.definitionCount, capacity.chargedBytes+capacity.reservedBytes}
    local bounds = {limits.runMax, limits.nonterminalRunMax, limits.objectMax, limits.unfinishedEventMax, limits.definitionMax, businessBytes}
    local high = false
    local low = true
    for i=1,#usage do
      if usage[i] >= math.floor(bounds[i]*0.9) then high = true end
      if usage[i] > math.floor(bounds[i]*0.8) then low = false end
    end
    if high then capacity.latch = 1 elseif low then capacity.latch = 0 end
    capacity.revision = add(capacity.revision, 1)
    sethash('capacity', capacity)
  end
  -- A pre-existing invalid ledger is never repaired by guessing missing or negative fields.
  checkCapacity()
${RUN_MUTATION_HELPERS}
${CONTROL_SCRIPT}
${EVENT_SCRIPT}
${MAINTENANCE_SCRIPT}
  if request.op == 'register' or request.op == 'renewRuntime' or request.op == 'unregister' or request.op=='gcRuntime' then
    local runtime = read('runtime')
    local registered = {}
    if runtime.runtimeId then
      if runtime.runtimeId ~= request.runtimeId or integer(runtime.generation) ~= request.generation then fail('COMMAND_CONFLICT') end
      registered = decode(runtime.registered)
      if runtime.manifest ~= request.manifest then fail('COMMAND_CONFLICT') end
      if request.op=='gcRuntime' and integer(runtime.deadline)>now then return {changed=false} end
      if request.op ~= 'unregister' and request.op~='gcRuntime' and integer(runtime.deadline) <= now then fail('LEASE_LOST') end
    elseif next(runtime) then fail('STORAGE_INCONSISTENT')
    elseif request.op == 'unregister' or request.op=='gcRuntime' then return {changed=false}
    elseif request.op ~= 'register' then fail('LEASE_LOST')
    end
    if request.op == 'register' then
      if not runtime.runtimeId then
        capacity.memberCount = add(capacity.memberCount,1)
        runtime = {runtimeId=request.runtimeId,generation=request.generation,manifest=request.manifest,ready='0'}
      end
      local seen = {}
      for _,identity in ipairs(registered) do seen[identity] = true end
      local sequence = integer(meta.definitionSequence)
      local deadline = add(now,protocol.lease.leaseMs)
      for i,declaration in ipairs(request.definitions) do
        local role = 'registrationDef_'..i
        local definition = read(role)
        if definition.canonical then
          if definition.canonical ~= declaration.canonical then fail('DEFINITION_HASH_CONFLICT') end
          definition.runRefs=integer(definition.runRefs); definition.runtimeRefs=integer(definition.runtimeRefs)
          integer(definition.catalogSequence)
        else
          if next(definition) then fail('STORAGE_INCONSISTENT') end
          sequence=add(sequence,1)
          definition={canonical=declaration.canonical,runRefs=0,runtimeRefs=0,catalogSequence=sequence}
          capacity.definitionCount=add(capacity.definitionCount,1)
          capacity.chargedBytes=add(capacity.chargedBytes,16384)
          write('ZADD','definitions',sequence,declaration.identity)
        end
        if not seen[declaration.identity] then
          definition.runtimeRefs=add(definition.runtimeRefs,1)
          registered[#registered+1]=declaration.identity; seen[declaration.identity]=true
        end
        sethash(role,definition)
        if request.complete then write('ZADD','registrationMembers_'..i,deadline,request.runtimeId) end
      end
      local manifest=decode(request.manifest)
      if #registered > 128 or #manifest > 128 then fail('STORAGE_INCONSISTENT') end
      if request.complete then
        if #registered ~= #manifest then fail('STORAGE_INCONSISTENT') end
        for _,identity in ipairs(manifest) do if not seen[identity] then fail('STORAGE_INCONSISTENT') end end
        runtime.ready='1'
      end
      runtime.deadline=deadline; runtime.registered=encode(registered)
      sethash('meta',{definitionSequence=sequence}); sethash('runtime',runtime)
      write('ZADD','members',deadline,request.runtimeId); saveCapacity()
      return {deadline=deadline,ready=runtime.ready=='1'}
    end
    local manifest=decode(request.manifest)
    if #registered ~= #request.definitions then fail('READ_CONFLICT') end
    for i,identity in ipairs(registered) do
      if identity ~= request.definitions[i].identity then fail('READ_CONFLICT') end
      if request.op == 'unregister' or request.op=='gcRuntime' then
        local definition=read('registrationDef_'..i)
        definition.runtimeRefs=add(integer(definition.runtimeRefs),-1)
        integer(definition.runRefs); integer(definition.catalogSequence)
        sethash('registrationDef_'..i,definition)
        write('ZREM','registrationMembers_'..i,request.runtimeId)
        if definition.runtimeRefs==0 and integer(definition.runRefs)==0 then write('ZADD','gcDefinitions',now,identity) end
      end
    end
    if request.op == 'unregister' or request.op=='gcRuntime' then
      capacity.memberCount=add(capacity.memberCount,-1); saveCapacity()
      write('ZREM','members',request.runtimeId); write('DEL','runtime')
      return {changed=true}
    end
    if runtime.ready~='1' then fail('LEASE_LOST') end
    local deadline=add(now,protocol.lease.leaseMs)
    for i=1,#registered do write('ZADD','registrationMembers_'..i,deadline,request.runtimeId) end
    sethash('runtime',{deadline=deadline}); write('ZADD','members',deadline,request.runtimeId)
    return {deadline=deadline,ready=true}
  end
  if request.op == 'start' then
    local existing = runread()
    if request.hasIdempotency then
      local id = redis.call('GET', key('idem'))
      if id then
        if id ~= request.runId then fail('READ_CONFLICT') end
        if not existing then fail('STORAGE_INCONSISTENT') end
      elseif existing then fail('STORAGE_INCONSISTENT') end
    end
    if existing then
      if existing.definitionCanonical ~= request.definitionCanonical or existing.query ~= request.query
        or existing.policyCanonical ~= request.policyCanonical or existing.version ~= request.version then fail('IDEMPOTENCY_CONFLICT') end
      return {runId=existing.runId,created=existing.startCommandId==request.commandId}
    end
    if capacity.latch == 1 then fail('CAPACITY_EXCEEDED') end
    local definition = read('definition')
    if definition.canonical and definition.canonical ~= request.definitionCanonical then fail('DEFINITION_HASH_CONFLICT') end
    if not definition.canonical and next(definition) then fail('STORAGE_INCONSISTENT') end
    local sequence = add(integer(meta.createdSequence), 1)
    local defSequence = integer(meta.definitionSequence)
    if not definition.canonical then
      defSequence = add(defSequence, 1)
      definition = {canonical=request.definitionCanonical,runRefs=0,runtimeRefs=0,catalogSequence=defSequence}
      capacity.definitionCount = add(capacity.definitionCount, 1)
      capacity.chargedBytes = add(capacity.chargedBytes, 16384)
      write('ZADD','definitions',defSequence,request.definitionIdentity)
    else
      definition.runRefs = integer(definition.runRefs)
      definition.runtimeRefs = integer(definition.runtimeRefs)
      definition.catalogSequence = integer(definition.catalogSequence)
    end
    definition.runRefs = add(definition.runRefs, 1)
    capacity.runCount = add(capacity.runCount, 1)
    capacity.nonterminalRunCount = add(capacity.nonterminalRunCount, 1)
    capacity.chargedBytes = add(capacity.chargedBytes, 98304 + #request.query + 8)
    saveCapacity()
    sethash('definition', definition)
    sethash('meta',{createdSequence=sequence,definitionSequence=defSequence})
    sethash('run',{schema='batch-v1',runId=request.runId,taskName=request.taskName,version=request.version,
      definitionIdentity=request.definitionIdentity,definitionCanonical=request.definitionCanonical,policyCanonical=request.policyCanonical,
      query=request.query,state='null',error='null',status='pending',reason='',revision=1,page=1,
      dispatchCount=0,businessFailures=0,scheduledRetries=0,recoveries=0,batchFailures=0,consecutiveRecoveries=0,
      createdAt=now,dueAt=now,terminalAt=0,eventRefCount=0,createdSequence=sequence,leaseRevision=0,token='',
      reservationBytes=0,reservationEvents=0,eventSequence=0,attemptTimeoutAt=0,idempotencyKey=request.idempotencyKey,
      normalEventCursor=0,callbackPending=0,callbackDelivered=0,callbackDeadLetters=0,
      startCommandId=request.commandId,cancelReceipt='',transportReceipt='',controlRing='[]',audit='[]'})
    if request.hasIdempotency then write('SET','idem',request.runId) end
    write('ZADD','runs',sequence,request.runId)
    write('ZADD','taskRuns',sequence,request.runId)
    write('ZADD','status_pending',sequence,request.runId)
    write('ZADD','taskStatus_pending',sequence,request.runId)
    write('ZADD','due',now,request.runId)
    return {runId=request.runId,created=true}
  end
  if request.op == 'claim' or request.op == 'renew' or request.op == 'settle' or request.op == 'recover' then
    local run=runread()
    if not run then return cjson.null end
    if run.definitionIdentity ~= request.definitionIdentity or run.taskName ~= request.taskName then fail('READ_CONFLICT') end
    local definition=read('definition')
    if definition.canonical ~= run.definitionCanonical then fail('STORAGE_INCONSISTENT') end
    local declaration=decode(run.definitionCanonical)
    local policy=decode(run.policyCanonical)
    -- Private command IDs are generated once and transport repeats the exact canonical bytes.
    -- One bounded receipt is only a recent transport outcome, not the public operator ring.
    if type(run.transportReceipt)~='string' then fail('STORAGE_INCONSISTENT') end
    if run.transportReceipt~='' then
      local receipt=decode(run.transportReceipt)
      if receipt.commandId==request.commandId then
        if receipt.op~=request.op or receipt.requestDigest~=redis.sha1hex(raw) then fail('COMMAND_CONFLICT') end
        if request.op=='claim' or request.op=='renew' then
          if run.token~=receipt.result.token or now>=integer(decode(run.token).deadline) then fail('LEASE_LOST') end
          if request.op=='claim' then return {token=run.token,run=run,now=now} end
        end
        return receipt.result
      end
    end
    local function receipt(result)
      local encoded=encode({commandId=request.commandId,op=request.op,requestDigest=redis.sha1hex(raw),result=result})
      if #encoded>2048 then fail('STORAGE_INCONSISTENT') end
      sethash('run',{transportReceipt=encoded})
      return result
    end
    local events={}
    for _,event in ipairs(declaration.events) do events[event]=true end
    if request.op == 'claim' then
      if run.status~='pending' and run.status~='retrying' then return cjson.null end
      if run.dueAt>now then return cjson.null end
      local runtime=read('runtime')
      if runtime.runtimeId~=request.runtimeId or integer(runtime.generation)~=request.generation
        or runtime.ready~='1' or integer(runtime.deadline)<=now then fail('LEASE_LOST') end
      local matched=false
      for _,identity in ipairs(decode(runtime.registered)) do if identity==run.definitionIdentity then matched=true end end
      if not matched or not redis.call('ZSCORE',key('definitionMembers'),request.runtimeId) then fail('LEASE_LOST') end
      local slots=(events.batchSettled and 1 or 0)+((events.success or events.failure) and 1 or 0)
      local reservation=131072+slots*131072
      if capacity.chargedBytes+capacity.reservedBytes+reservation>businessBytes
        or capacity.runCount+capacity.eventCount+capacity.reservedObjects+slots>limits.objectMax
        or capacity.unfinishedEventCount+capacity.reservedEvents+slots>limits.unfinishedEventMax then
        transition(run,'blocked','capacity',run.dueAt,0); sethash('run',run); return cjson.null
      end
      capacity.reservedBytes=add(capacity.reservedBytes,reservation)
      capacity.reservedObjects=add(capacity.reservedObjects,slots); capacity.reservedEvents=add(capacity.reservedEvents,slots)
      run.reservationBytes=reservation; run.reservationEvents=slots
      run.leaseRevision=add(run.leaseRevision,1); run.dispatchCount=add(run.dispatchCount,1)
      run.attemptTimeoutAt=add(now,integer(policy.timeoutMs))
      local deadline=add(now,protocol.lease.leaseMs)
      run.token=encode({runId=run.runId,batchId=run.runId..':'..decimal(run.page),definitionIdentity=run.definitionIdentity,
        runtimeId=request.runtimeId,generation=request.generation,nonce=request.nonce,leaseRevision=run.leaseRevision,deadline=deadline})
      transition(run,'running','',0,0); sethash('run',run); saveCapacity(); write('ZADD','leases',deadline,run.runId)
      receipt({token=run.token})
      return {token=run.token,run=run,now=now}
    end
    if run.token=='' then
      if request.op=='recover' then return {changed=false} end
      fail('LEASE_LOST')
    end
    local token=decode(run.token)
    if request.op == 'recover' then
      if now<integer(token.deadline) then return {changed=false} end
      run.recoveries=add(run.recoveries,1); run.consecutiveRecoveries=add(run.consecutiveRecoveries,1)
      local dueAt=add(now,integer(request.retryDelay))
      local status='retrying'; local reason=''
      if run.status=='pausing' then status='paused'
      elseif run.consecutiveRecoveries>=protocol.lease.recoveryLimit then status='blocked'; reason='recovery_exhausted' end
      releaseReservation(run); transition(run,status,reason,dueAt,0); sethash('run',run); saveCapacity()
      return receipt({changed=true,status=status})
    end
    if run.token~=request.token or now>=integer(token.deadline) then fail('LEASE_LOST') end
    if run.status~='running' and run.status~='pausing' then fail('LEASE_LOST') end
    if request.op == 'renew' then
      run.leaseRevision=add(run.leaseRevision,1); token.leaseRevision=run.leaseRevision
      token.deadline=add(now,protocol.lease.leaseMs); run.token=encode(token)
      sethash('run',{token=run.token,leaseRevision=run.leaseRevision}); write('ZADD','leases',token.deadline,run.runId)
      return receipt({token=run.token})
    end
    local kind=request.kind
    local state=run.state; local envelope='null'
    if kind~='next' and kind~='end' and kind~='business' and kind~='timeout' and kind~='contract' then fail('STORAGE_INCONSISTENT') end
    if now>=run.attemptTimeoutAt and (kind=='next' or kind=='end') then kind='timeout' end
    if kind=='timeout' then envelope=request.timeoutError
    elseif kind=='business' or kind=='contract' then envelope=request.error
    elseif kind=='next' then state=request.state end
    if type(state)~='string' or #state>65536 or type(envelope)~='string' or #envelope>32768 then fail('STORAGE_INCONSISTENT') end
    checkBusinessJson(state,65536); checkBusinessJson(envelope,32768)
    local oldBytes=#run.state+#run.error
    local final=false; local outcome; local terminalAt=0; local dueAt=0
    if kind=='next' then
      outcome=run.status=='pausing' and 'paused' or 'pending'; dueAt=now
    elseif kind=='end' then outcome='success'; final=true; terminalAt=now
    elseif kind=='contract' then outcome='failed'; final=true; terminalAt=now
    else
      run.businessFailures=add(run.businessFailures,1); run.batchFailures=add(run.batchFailures,1)
      if run.batchFailures>=integer(policy.attempts) then outcome='failed'; final=true; terminalAt=now
      else
        outcome=run.status=='pausing' and 'paused' or 'retrying'
        dueAt=add(now,integer(request.retryDelay)); run.scheduledRetries=add(run.scheduledRetries,1)
      end
    end
    local emitted={}
    if kind=='next' or final then
      if events.batchSettled then emitted[#emitted+1]='batchSettled' end
      local terminalEvent=outcome=='failed' and 'failure' or 'success'
      if final and events[terminalEvent] then emitted[#emitted+1]=terminalEvent end
    end
    if #emitted>run.reservationEvents then fail('STORAGE_INCONSISTENT') end
    for _,eventKind in ipairs(emitted) do
      local role='event_'..eventKind
      if redis.call('EXISTS',key(role))~=0 then fail('STORAGE_INCONSISTENT') end
      run.eventSequence=add(run.eventSequence,1); run.eventRefCount=add(run.eventRefCount,1)
      local eventId=run.runId..':'..decimal(run.page)..':'..eventKind
      sethash(role,{schema='batch-v1',eventId=eventId,runId=run.runId,batchId=run.runId..':'..decimal(run.page),kind=eventKind,
        taskName=run.taskName,version=run.version,definitionIdentity=run.definitionIdentity,sequence=run.eventSequence,timestamp=now,
        queryRef=run.runId,state=state,error=envelope,status='pending',revision=1,deliveryAttempt=0,replayGeneration=0,lateReplay='0',
        firstDeadAt=0,deadLetterExpiresAt=0,firstDeadLetterSequence=0,replayDrainDeadline=0,deliveredAt=0,dueAt=now,token='',leaseRevision=0,
        attemptTimeoutAt=0,deliveryError='null',transportReceipt=''})
      write('ZADD','events',run.eventSequence,eventId); write('ZADD','dueEvents',now,eventId)
    end
    capacity.eventCount=add(capacity.eventCount,#emitted); capacity.unfinishedEventCount=add(capacity.unfinishedEventCount,#emitted)
    run.callbackPending=add(run.callbackPending,#emitted)
    capacity.chargedBytes=add(capacity.chargedBytes,#state+#envelope-oldBytes+#emitted*131072)
    if final then capacity.nonterminalRunCount=add(capacity.nonterminalRunCount,-1) end
    run.state=state; run.error=envelope
    if kind=='next' then run.page=add(run.page,1); run.batchFailures=0; run.consecutiveRecoveries=0 end
    releaseReservation(run); transition(run,outcome,'',dueAt,terminalAt); sethash('run',run); saveCapacity()
    return receipt({status=outcome,revision=run.revision,page=run.page})
  end
  if request.op == 'cancel' then
    local run = runread()
    if not run then return {found=false,runId=request.runId} end
    if run.definitionIdentity ~= request.definitionIdentity or run.taskName ~= request.taskName then fail('READ_CONFLICT') end
    if run.cancelReceipt ~= '' then
      local receipt = decode(run.cancelReceipt)
      if receipt.commandId == request.commandId then return receipt.result end
    end
    local terminal = run.status == 'success' or run.status == 'failed' or run.status == 'cancelled'
    local result = {found=true,runId=run.runId,status=run.status,revision=run.revision,changed=false}
    if terminal then return result end
    cancelRun(run)
    result.status=run.status; result.revision=run.revision; result.changed=true
    saveCapacity()
    run.cancelReceipt=encode({commandId=request.commandId,result=result}); sethash('run',run)
    return result
  end
  fail('STORAGE_INCONSISTENT')
end
local planned, result = pcall(execute)
if not planned then return encode({error=tostring(result)}) end
local reply=encode(result)
for _,command in ipairs(writes) do redis.call(unpack(command)) end
return reply
`;

export const BATCH_SCRIPT_SHA = createHash('sha1').update(BATCH_SCRIPT).digest('hex');
