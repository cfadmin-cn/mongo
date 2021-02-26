local protocol = require "mongo.protocol"
local request_query = protocol.request_query
local send_handshake = protocol.send_handshake
local request_insert = protocol.request_insert

local toint = math.tointeger

local tcp = require "internal.TCP"

local class = require "class"

---@class  MongoDB  @`MongoDB`对象
local mongo = class("MongoDB")

function mongo:ctor(opt)
  self.SSL  = opt.SSL
  self.host = opt.host or "localhost"
  self.port = opt.port or 27017
  self.username = opt.username
  self.password = opt.password
  self.reqid = 1
  self.sock = tcp:new()
  self.have_transaction = false
  self.connected = false
end

---comment 授权认证
function mongo:auth()
  if not self.password or not self.username then
    return nil, "Invalid username or password."
  end
  self.authing = assert(not self.authing and true, "It is forbidden to call the `auth` method concurrently.")
  self.authing = nil
  return true
end

---comment 连接服务器
function mongo:connect()
  if self.connected then
    return true, "already connected."
  end
  self.connecting = assert(not self.connecting and true, "It is forbidden to call the `connect` method concurrently.")
  if not self.sock then
    self.sock = tcp:new()
  end
  local ok, err = self.sock:connect(self.host, self.port)
  if not ok then
    self.connecting = nil
    return false, err or "连接失败."
  end
  if self.SSL then
    ok, err = self.sock.ssl_handshake()
    if not ok then
      self.connecting = nil
      return false, err or "Mongo SSL handshake failed."
    end
  end
  send_handshake(self)
  self.connected = true
  self.connecting = nil
  return true
end

---comment 查询
function mongo:find(...)
  local tab, err = request_query(self, ...)
  if not tab or tab.errmsg then
    return false, err or string.format('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return tab
end

---comment 新增
function mongo:insert(...)
  local tab, err = request_insert(self, ...)
  if not tab or tab.errmsg then
    return false, err or string.format('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return { nInserted = toint(tab['n']), Inserted = tab['ok'] == 1 and true or false }
end

---comment 修改
function mongo:update()

end

---comment 删除
function mongo:delete()

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
