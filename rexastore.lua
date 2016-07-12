local _USAGE = {
  'KEYS[1] - graph key name',
  'KEYS[2] - command. Can be:',
  '  drop          - drops an index',
  '  get           - returns item if exists',
  '  put           - puts new item to graph',
  '  delete or del - deletes item from graph',
  '  query         - query using graph-query syntax',
}

local function drop()
  redis.call('DEL', KEYS[1])
end

local function keys(t)
  return {
    t['subject'],
    t['predicate'],
    t['object']
  }
end

local function _hexkeys(query)
  local subject = query['subject']
  local predicate = query['predicate']
  local object = query['object']
  return {
    spo = subject .. ':' .. predicate .. ':' .. object,
    sop = subject .. ':' .. object .. ':' .. predicate,
    ops = object .. ':' .. predicate .. ':' .. subject,
    osp = object .. ':' .. subject .. ':' .. predicate,
    pso = predicate .. ':' .. subject .. ':' .. object,
    pos = predicate .. ':' .. object .. ':' .. subject
  }
end

local function parse(type, spo)
  if not spo then
    return nil
  end
  local v = string.gmatch(spo, '([^:]+)')
  local t = type
  if t == nil then
    t = v()
  end
  local subject
  local predicate
  local object
  if t == 'spo' then
    subject = v()
    predicate = v()
    object = v()
  elseif t == 'pos' then
    predicate = v()
    object = v()
    subject = v()
  elseif t == 'osp' then
    object = v()
    subject = v()
    predicate = v()
  elseif t == 'sop' then
    subject = v()
    object = v()
    predicate = v()
  elseif t == 'ops' then
    object = v()
    predicate = v()
    subject = v()
  elseif t == 'pso' then
    predicate = v()
    subject = v()
    object = v()
  end
  local res = {}
  if subject then
    res['subject'] = subject
  end
  if predicate then
    res['predicate'] = predicate
  end
  if object then
    res['object'] = object
  end
  return (res)
end

local function put(query)
  local data = _hexkeys(query)
  redis.call('ZADD', KEYS[1], 0, 'spo:' .. data['spo'])
  redis.call('ZADD', KEYS[1], 0, 'sop:' .. data['sop'])
  redis.call('ZADD', KEYS[1], 0, 'ops:' .. data['ops'])
  redis.call('ZADD', KEYS[1], 0, 'osp:' .. data['osp'])
  redis.call('ZADD', KEYS[1], 0, 'pso:' .. data['pso'])
  redis.call('ZADD', KEYS[1], 0, 'pos:' .. data['pos'])
end

local function delete(query)
  local data = _hexkeys(query)
  redis.call('ZREM', KEYS[1], 0, 'spo:' .. data['spo'])
  redis.call('ZREM', KEYS[1], 0, 'sop:' .. data['sop'])
  redis.call('ZREM', KEYS[1], 0, 'ops:' .. data['ops'])
  redis.call('ZREM', KEYS[1], 0, 'osp:' .. data['osp'])
  redis.call('ZREM', KEYS[1], 0, 'pso:' .. data['pso'])
  redis.call('ZREM', KEYS[1], 0, 'pos:' .. data['pos'])
end

local function save_spo(key, items)
  for _, p in pairs(items) do
    redis.call('SADD', key, table.concat(keys(p), ':'))
  end
end

local function save_items(key, items)
  for _, p in pairs(items) do
    redis.call('SADD', key, p)
  end
end

local function get_items(key)
  return redis.call('SMEMBERS', key)
end

local function get(data)
  local t = ''
  local q = ''
  local subject = data['subject']
  local predicate = data['predicate']
  local object = data['object']
  if subject then
    t = t .. 's'
    q = q .. ':' .. subject
  end
  if predicate then
    t = t .. 'p'
    q = q .. ':' .. predicate
  end
  if object then
    t = t .. 'o'
    q = q .. ':' .. object
  end
  local tl = #t
  if #t == 1 then
    if t == 's' then
      t = t .. 'po'
    end
    if t == 'p' then
      t = t .. 'os'
    end
    if t == 'o' then
      t = t .. 'sp'
    end
  end
  if #t == 2 then
    if t == 'sp' then
      t = t .. 'o'
    end
    if t == 'po' then
      t = t .. 's'
    end
    if t == 'so' then
      t = t .. 'p'
    end
  end
  local s = t .. q
  if tl < 3 then
    s = s .. ':'
  end
  local results = redis.call(
    'ZRANGEBYLEX', KEYS[1], '[' .. s, '[' .. s .. '\255'
  )
  local items = {}
  for _, p in pairs(results) do
    table.insert(items, parse(nil, p))
  end

  return(items)
end

local function parse_var(v)
  if v:match(':') then
    return nil
  end
  local t = v:sub(1, 1)
  if t == '<' or t == '>' then
    return {
      type = t,
      name = v:sub(2)
    }
  else
    return false
  end
end

local function query(q)
  local res = {}
  for i, p in pairs(q) do
    local GKEY = 'GRAPH_QUERY:ITER_' .. i
    local var = parse_var(p)
    if var then
      res = get_items('GRAPH_QUERY:VAR_' .. var['name'])
      break
    end
    local d = parse('spo', p)
    local s_var = parse_var(d['subject'])
    local p_var = parse_var(d['predicate'])
    local o_var = parse_var(d['object'])
    local s_iter = {}
    local p_iter = {}
    local o_iter = {}
    if s_var and s_var['type'] == '<' then
      s_iter = get_items('GRAPH_QUERY:VAR_' .. s_var['name'])
    else
      s_iter = {d['subject']}
    end
    if p_var and p_var['type'] == '<' then
      p_iter = get_items('GRAPH_QUERY:VAR_' .. p_var['name'])
    else
      p_iter = {d['predicate']}
    end
    if o_var and o_var['type'] == '<' then
      o_iter = get_items('GRAPH_QUERY:VAR_' .. o_var['name'])
    else
      o_iter = {d['object']}
    end

    for _, s in pairs(s_iter) do
      for _, p in pairs(p_iter) do
        for _, o in pairs(o_iter) do
          local s_var = parse_var(s)
          local p_var = parse_var(p)
          local o_var = parse_var(o)
          local q = {}
          if not s_var then
            q['subject'] = s
          end
          if not p_var then
            q['predicate'] = p
          end
          if not o_var then
            q['object'] = o
          end

          local items = get(q)
          local s_items = {}
          local p_items = {}
          local o_items = {}
          for _, p in pairs(items) do
            if s_var then
              table.insert(s_items, p['subject'])
            end
            if p_var then
              table.insert(p_items, p['predicate'])
            end
            if o_var then
              table.insert(o_items, p['object'])
            end
          end
          if #s_items > 0 then
            save_items('GRAPH_QUERY:VAR_' .. s_var['name'], s_items)
            redis.call('SADD', 'GRAPH_KEYS_TO_DELETE', 'GRAPH_QUERY:VAR_' .. s_var['name'])
          end
          if #p_items > 0 then
            save_items('GRAPH_QUERY:VAR_' .. p_var['name'], p_items)
            redis.call('SADD', 'GRAPH_KEYS_TO_DELETE', 'GRAPH_QUERY:VAR_' .. p_var['name'])
          end
          if #o_items > 0 then
            save_items('GRAPH_QUERY:VAR_' .. o_var['name'], o_items)
            redis.call('SADD', 'GRAPH_KEYS_TO_DELETE', 'GRAPH_QUERY:VAR_' .. o_var['name'])
          end
        end
      end
    end

    redis.call('SADD', 'GRAPH_KEYS_TO_DELETE', GKEY)
    redis.call('SADD', 'GRAPH_KEYS_TO_DELETE', 'GRAPH_KEYS_TO_DELETE')
  end

  local v_a = redis.call('SMEMBERS', 'GRAPH_QUERY:VAR_A')
  local v_b = redis.call('SMEMBERS', 'GRAPH_QUERY:VAR_B')

  local keys_to_delete = redis.call('SMEMBERS', 'GRAPH_KEYS_TO_DELETE')
  for _, k in pairs(keys_to_delete) do
    redis.call('DEL', k)
  end

  return res
end

local function test()
  drop()

  put({subject = 'Alice', predicate = 'friend', object = 'Bob'})

  local len = redis.call('ZCARD', KEYS[1])
  if len ~= 6 then
    return({err = 'Wrong number of graph items'})
  end

  put({subject = 'Alice', predicate = 'friend', object = 'Carol'})
  put({subject = 'Carol', predicate = 'friend', object = 'Alice'})

  local friends = get({subject = 'Alice', predicate = 'friend'})
  if #friends ~= 2 then
    return({err = 'Wrong number of friends of Alice'})
  end
  if friends[1]['object'] ~= 'Bob' then
    return({err = 'Wrong name of Alice\'s friend'})
  end

  delete({subject = 'Alice', predicate = 'friend', object = 'Bob'})
  local len = redis.call('ZCARD', KEYS[1])
  if len ~= 12 then
    return({err = 'Graph items was not removed'})
  end

  put({subject = 'Alice', predicate = 'friend', object = 'Bob'})

  local foaf = query({
    'Carol:friend:>A',
    '<A:friend:>B',
    '<B'
  })
  -- return cjson
  if #foaf ~= 2 then
    return({err = 'Wrong number of friends of a friend of Carol'})
  end
  if foaf[1] ~= 'Bob' or foaf[2] ~= 'Carol' then
    return({err = 'Wrong friends: ' .. cjson.encode(foaf)})
  end

  drop()
  return('OK')
end

-- parse arguments
if #KEYS ~= 2 then
  return(_USAGE)
end

local cmd = KEYS[2]:lower()

if cmd == 'test' then
  return test()
end

if cmd == 'get' then
  local result = get({
    subject = ARGV[1],
    predicate = ARGV[2],
    object = ARGV[3]
  })
  local response = {}
  for _, h in pairs(result) do
    table.insert(response, keys(h))
  end
  return response
end

if cmd == 'put' then
  return put({
    subject = ARGV[1],
    predicate = ARGV[2],
    object = ARGV[3]
  })
end

if cmd == 'delete' or cmd == 'del' then
  return delete({
    subject = ARGV[1],
    predicate = ARGV[2],
    object = ARGV[3]
  })
end

if cmd == 'query' then
  return query(ARGV)
end

if cmd == 'drop' then
  return drop()
end

return(_USAGE)
