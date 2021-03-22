local protocol = require "mongo.protocol"
-- 协议交互
local request_count = protocol.request_count
local request_query = protocol.request_query
local request_update = protocol.request_update
local request_insert = protocol.request_insert
local request_delete = protocol.request_delete
local request_aggregate = protocol.request_aggregate
-- 握手
local request_auth = protocol.request_auth
local request_handshake = protocol.request_handshake

local toint = math.tointeger
local tonumber = tonumber
local fmt = string.format

local tcp = require "internal.TCP"

local class = require "class"

---@class  MongoDB  @`MongoDB`对象
local mongo = class("MongoDB")

function mongo:ctor(opt)
  self.reqid = 1
  self.SSL  = opt.SSL
  self.db   = opt.db or "admin"
  self.host = opt.host or "localhost"
  self.port = opt.port or 27017
  self.username = opt.username
  self.password = opt.password
  self.auth_mode = opt.auth_mode or "SCRAM-SHA-1"
  self.sock = tcp:new()
  self.have_transaction = false
  self.connected = false
end

function mongo:set_timeout(timeout)
  if self.sock and tonumber(timeout) then
    self.sock._timeout = timeout
  end
end

---comment 连接服务器
function mongo:connect()
  if self.connected then
    return true, "already connected."
  end
  if not self.sock then
    self.sock = tcp:new()
  end
  local ok, err
  ok, err = self.sock:connect(self.host, self.port)
  if not ok then
    return false, err
  end
  if self.SSL then
    ok, err = self.sock.ssl_handshake()
    if not ok then
      return false, err or "Mongo SSL handshake failed."
    end
  end
  ok, err = request_handshake(self)
  if not ok then
    return false, err
  end
  ok, err = request_auth(self)
  if not ok then
    return false, err
  end
  self.reqid = self.reqid + 1
  self.connected = true
  return true
end

---comment 查询数据
---@param database string       @需要查询的数据库名称
---@param collect string        @需要查询的集合名称
---@param filter table          @需要执行查询的条件
---@param option table          @需要查询的可选参数(`cursor`/`limit`/`skip`/`sort`)
---@return table, integer | nil, string  @成功返回结果数据与游标`id`, 失败返回`false`与出错信息,
function mongo:find(database, collect, filter, option)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid find collect or database.")
  local tab, err = request_query(self, database, collect, filter, option)
  if not tab or tab.errmsg then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return tab.cursor.firstBatch or tab.cursor.nextBatch, tab.cursor.id
end

---comment 插入数据
---@param database string       @需要插入的数据库名称
---@param collect string        @需要插入的集合名称
---@param documents table       @需要插入的文档数组
---@param option table          @需要插入的文档可选参数(`ordered`)
function mongo:insert(database, collect, documents, option)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid insert collect or database.")
  assert(type(documents) == 'table' and #documents > 0 and type(documents[1]) == "table", "Invalid insert documents.")
  local tab, err = request_insert(self, database, collect, documents, option)
  if not tab or tab.errmsg then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return { acknowledged = (tab['ok'] == 1 or tab['ok'] == true) and true or false, insertedCount = toint(tab['n']) }
end

---comment 修改数据
---@param database string       @需要修改的数据库名称
---@param collect string        @需要修改的集合名称
---@param filter table          @需要更新的过滤条件
---@param update table          @需要更新的文档结果
---@param option table          @需要更新的可选参数(`upsert`/`multi`)
function mongo:update(database, collect, filter, update, option)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid update collect or database.")
  assert(type(filter) == 'table', "Invalid update filter.")
  local tab, err = request_update(self, database, collect, filter, update, option)
  if not tab or tab.errmsg then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return{ acknowledged = (tab['ok'] == 1 or tab['ok'] == true) and true or false, matchedCount = toint(tab['n']), modifiedCount = toint(tab['nModified']) }
end

---comment 删除数据
---@param database string       @需要删除的数据库名称
---@param collect string        @需要删除的集合名称
---@param filter table          @需要删除的过滤条件
---@param option table          @需要删除的可选参数(`one`)
function mongo:delete(database, collect, filter, option)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid delete collect or database.")
  assert(type(filter) == 'table', "Invalid delete filter.")
  local tab, err = request_delete(self, database, collect, filter, option)
  if not tab or tab.errmsg then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return { acknowledged = (tab['ok'] == 1 or tab['ok'] == true) and true or false, deletedCount = toint(tab['n']),  }
end

---comment COUNT - 聚合函数
---@param database string       @需要查询的数据库名称
---@param collect string        @需要查询的集合名称
---@param filter table          @需要执行查询的条件
---@param option table          @需要查询的可选参数
---@return table, integer | nil, string  @成功返回结果数据与游标`id`, 失败返回`false`与出错信息,
function mongo:count(database, collect, filter, option)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid find collect or database.")
  local tab, err = request_count(self, database, collect, filter, option)
  if not tab or tab.errmsg then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return { acknowledged = true , count = tab.cursor.firstBatch[1].n }, tab.cursor.id
end

---comment AGGREGATE - 聚合函数
---@param database string       @需要查询的数据库名称
---@param collect string        @需要查询的集合名称
---@param filter table          @需要执行查询的条件
---@param option table          @需要查询的可选参数
---@return table, integer | nil, string  @成功返回结果数据与游标`id`, 失败返回`false`与出错信息,
function mongo:aggregate(database, collect, filter, option)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid find collect or database.")
  local tab, err = request_aggregate(self, database, collect, filter, option)
  if not tab or tab.errmsg then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return tab.cursor.firstBatch or tab.cursor.nextBatch, tab.cursor.id
end

---comment 关闭连接
function mongo:close()
  self.connected = false
  if self.sock then
    self.sock:close()
    self.sock = nil
  end
end

return mongo
