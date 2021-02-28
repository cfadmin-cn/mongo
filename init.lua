local protocol = require "mongo.protocol"
-- 协议交互
local request_query = protocol.request_query
local request_update = protocol.request_update
local request_insert = protocol.request_insert
local request_delete = protocol.request_delete
-- 握手
local request_auth = protocol.request_auth
local request_handshake = protocol.request_handshake


local toint = math.tointeger

local tcp = require "internal.TCP"

local class = require "class"

---@class  MongoDB  @`MongoDB`对象
local mongo = class("MongoDB")

function mongo:ctor(opt)
  self.SSL  = opt.SSL
  self.db   = opt.db or "admin"
  self.host = opt.host or "localhost"
  self.port = opt.port or 27017
  self.username = opt.username
  self.password = opt.password
  self.auth_mode = opt.auth_mode or "SCRAM-SHA-1"
  self.reqid = 1
  self.sock = tcp:new()
  self.have_transaction = false
  self.connected = false
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
    return false, err or "连接失败."
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
  self.connected = true
  return true
end

---comment 查询
function mongo:find(database, collect, filter)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid find collect or database.")
  local tab, err = request_query(self, database, collect, filter)
  if not tab or tab.errmsg then
    return false, err or string.format('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return tab.cursor.firstBatch, tab.id
end

---comment 新增
function mongo:insert(database, collect, documents, option)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid insert collect or database.")
  assert(type(documents) == 'table' and #documents > 0 and type(documents[1]) == "table", "Invalid insert documents.")
  local tab, err = request_insert(self, database, collect, documents, option)
  if not tab or tab.errmsg then
    return false, err or string.format('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return { acknowledged = (tab['ok'] == 1 or tab['ok'] == true) and true or false, insertedCount = toint(tab['n']) }
end

---comment 修改
function mongo:update(database, collect, filter, update, option)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid update collect or database.")
  local tab, err = request_update(self, database, collect, filter, update, option)
  if not tab or tab.errmsg then
    return false, err or string.format('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return{ acknowledged = (tab['ok'] == 1 or tab['ok'] == true) and true or false, matchedCount = toint(tab['n']), modifiedCount = toint(tab['nModified']) }
end

---comment 删除
function mongo:delete(database, collect, array, option)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid delete collect or database.")
  assert(type(array) == 'table', "Invalid delete filter.")
  local tab, err = request_delete(self, database, collect, array, option)
  if not tab or tab.errmsg then
    return false, err or string.format('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return { acknowledged = (tab['ok'] == 1 or tab['ok'] == true) and true or false, deletedCount = toint(tab['n']),  }
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
