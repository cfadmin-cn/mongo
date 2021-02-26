--[[
  LICENSE: BSD
  Author: CandyMi[https://github.com/candymi]
]]

local bson = require "mongo.bson"
local bson_encode = bson.encode
local bson_decode = bson.decode
local bson_encode_order = bson.bson_encode_order

local sys = require "sys"
local new_tab = sys.new_tab

local assert = assert
local strpack = string.pack
local strunpack = string.unpack

local concat = table.concat

local toint = math.tointeger

local STR_TO_OPCODE = {
  OP_REPLY	      = 1,	   -- Reply to a client request. responseTo is set.
  OP_UPDATE       =	2001,	 -- receivedUpdate	checkAuthForUpdate	update.
  OP_INSERT       =	2002,	 -- receivedInsert	checkAuthForInsert	createIndex/insert.
  RESERVED	      = 2003,	 -- Formerly used for OP_GET_BY_OID.
  OP_QUERY        =	2004,  --	receivedQuery	checkAuthForQuery	find.
  OP_GET_MORE	    = 2005,  --	receivedGetMore	checkAuthForGetMore	listCollections/listIndexes/find.
  OP_DELETE       =	2006,	 -- receivedDelete	checkAuthForDelete	remove
  OP_KILL_CURSORS =	2007,	 -- receivedKillCursors	checkAuthForKillCursors	killCursors
  OP_MSG	        = 2013,	 -- Send a message using the format introduced in MongoDB 3.6.
}

local OPCODE_TO_STR = {
  [1]    = "OP_REPLY",
  [2001] = "OP_UPDATE",
  [2002] = "OP_INSERT",
  [2003] = "RESERVED",
  [2004] = "OP_QUERY",
  [2005] = "OP_GET_MORE",
  [2006] = "OP_DELETE",
  [2007] = "OP_KILL_CURSORS",
  [2013] = "OP_MSG",
}

local function sock_read(sock, bytes)
  local buffers = new_tab(32, 0)
  while 1 do
    local buf = sock:recv(bytes)
    if not buf then
      return false
    end
    buffers[#buffers+1] = buf
    bytes = bytes - #buf
    if bytes == 0 then
      break
    end
  end
  return concat(buffers)
end

---comment 读取公共头部
local function read_header(sock)
  -- print(1.1)
  local data = sock_read(sock, 16)
  if not data then
    return false, "[MONGO ERROR]: Server closed this session when client read header byte from socket."
  end
  -- print(1.2)
  local msg_len, req_id, resp_id, opcode = strunpack("<i4i4i4i4", data)
  return { msg_len = msg_len, req_id = req_id, resp_id = resp_id, opcode = opcode, opcode_str = OPCODE_TO_STR[opcode] }
end

---comment 读取MSG头部
local function read_msg_header(sock)
  local header, err = read_header(sock)
  if not header then
    return false, err
  end
  local data = sock_read(sock, 4)
  if not data then
    return false, "[MONGO ERROR]: Server closed this session when client read header byte from socket."
  end
  local flag = strunpack("<i4", data)
  header["flags"] = {
    checksum = flag & 0x01 == 0x01 and true,
    morecome = flag & 0x02 == 0x02 and true,
    allowed = flag & 0x010000 == 0x010000 and true,
  }
  return header
end

-- 读取OP_REPLY
local function read_reply(self)
  -- print(1)
  local sock = self.sock
  local header, err = read_header(sock)
  if not header then
    return false, err
  end
  -- print(2, header.req_id, header.resp_id ~= self.reqid)
  if header.req_id and header.resp_id ~= self.reqid then
    return false, "[MONGO ERROR]: Unexpected response request ID."
  end
  -- print(3)
  local buf = sock_read(sock, 20)
  if not buf then
    return false, "[MONGO ERROR]: Server closed this session when client read reply byte from socket."
  end
  -- print(4)
  header.flags, header.cuorsor, header.started, header.returned = strunpack("<i4i8i4i4", buf)
  local document = sock_read(sock, header.msg_len - 36)
  if not document then
    return false, "[MONGO ERROR]: Server closed this session when client read document byte from socket."
  end
  -- print(5)
  return header, bson_decode(document)
end

local function read_msg_reply(self)
  -- print("开始读取")
  local sock = self.sock
  local header, err = read_msg_header(sock)
  if not header then
    return false, err
  end
  -- print("读取完毕")
  -- var_dump(header)
  if header.msg_len - 20 > 1 then
    sock_read(sock, 1)
  end
  local document = sock_read(sock, header.msg_len - 21)
  if not document then
    return false, "[MONGO ERROR]: Server closed this session when client read reply."
  end
  self.reqid = self.reqid + 1
  return bson_decode(document)
end

-- --------- QUERY --------- --
local function send_query(self, db, table, option)
  local sections = bson_encode_order({"find", table}, {"filter", option}, {"$db", db}, {"$readPreference", { mode = "primaryPreferred" }})
  return self.sock:send(strpack("<i4i4i4i4i4B", #sections + 21, self.reqid, 0, STR_TO_OPCODE["OP_MSG"], 0, 0)) and self.sock:send(sections)
end

local function read_query(self)
  return read_msg_reply(self)
end
-- --------- QUERY --------- --


-- --------- INSERT --------- --
local function send_insert(self, db, table, array, option)
  local documents = new_tab(#array, 0)
  for index, tab in ipairs(array) do
    documents[index] = assert(bson_encode(tab))
  end
  local section1 = bson_encode_order({"insert", table}, {"$db", db}, {"ordered", type(option) == 'table' and option.ordered and true or false})
  local section2 = concat(documents)
  local sections = concat{strpack("<B", 0), section1, strpack("<Bi4z", 1, 10 + 4 + #section2, "documents"), section2 }
  return self.sock:send(strpack("<i4i4i4i4i4", 20 + #sections, self.reqid, 0, STR_TO_OPCODE["OP_MSG"], 0)) and self.sock:send(sections)
end

local function read_insert(self)
  return read_msg_reply(self)
end
-- --------- INSERT --------- --

-- --------- UPDATE --------- --
local function send_update(self, db, table, filter, update, option)
  local section1 = bson_encode_order({"update", table}, {"$db", db}, {"ordered", type(option) == 'table' and option.ordered and true or false})
  local section2 = bson_encode({q = filter, u = update, upsert = type(option) == 'table' and option.upsert and true or false, multi = type(option) == 'table' and option.multi and true or false} )
  local sections = concat{strpack("<B", 0), section1, strpack("<Bi4z", 1, 8 + 4 + #section2, "updates"), section2 }
  return self.sock:send(strpack("<i4i4i4i4i4", 20 + #sections, self.reqid, 0, STR_TO_OPCODE["OP_MSG"], 0)) and self.sock:send(sections)
end

local function read_update(self)
  return read_msg_reply(self)
end
-- --------- UPDATE --------- --

-- --------- DELETE --------- --
local function send_delete(self, db, table, array, option)
  local section1 = bson_encode_order({"delete", table}, {"$db", db}, {"ordered", type(option) == 'table' and option.ordered and true or false})
  local section2 = bson_encode({q = #array > 0 and { nav_result = array } or array, limit = type(option) == 'table' and toint(option.limit) or 0 })
  local sections = concat{strpack("<B", 0), section1, strpack("<Bi4z", 1, 8 + 4 + #section2, "deletes"), section2 }
  return self.sock:send(strpack("<i4i4i4i4i4", 20 + #sections, self.reqid, 0, STR_TO_OPCODE["OP_MSG"], 0)) and self.sock:send(sections)
end

local function read_delete(self)
  return read_msg_reply(self)
end
-- --------- DELETE --------- --



local protocol = { __Version__ = 0.1 }

---comment 心跳与握手消息
function protocol.send_handshake(self)
  local query = assert(bson_encode_order({"isMaster", 1}, {"compress", {}}))
  if not self.sock:send(strpack("<i4i4i4i4i4zi4i4", #query + 39, 1, 0, STR_TO_OPCODE["OP_QUERY"], 0x04, 'admin.$cmd', 0, -1)) or not self.sock:send(query) then
    return false, "[MONGO ERROR]: Server closed this session when client send handshake data."
  end
  local header, response, err = read_reply(self)
  if not header or not response then
    return false, response or err or "[MONGO ERROR]: Unknown Error."
  end
  -- var_dump(header);
  -- var_dump(response);
  -- 如果探测到MongoDB版本低于3.6则当做连接失败
  if response.maxWireVersion < 6 then
    return false, "[MONGO ERROR]: versions below 3.6 are not supported."
  end
  self.reqid = 2
  self.have_transaction = response.maxWireVersion >= 7 -- 是否支持事务
  return true
end

---comment 查询语句
function protocol.request_query(self, db, table, option)
  if not send_query(self, db, table, option) then
    return false, "[MONGO ERROR]: Server closed this session when client send query data."
  end
  return read_query(self)
end

---comment 插入语句
function protocol.request_insert(self, db, table, array, option)
  if not send_insert(self, db, table, array, option) then
    return false, "[MONGO ERROR]: Server closed this session when client send insert data."
  end
  return read_insert(self)
end

---comment 更新语句
function protocol.request_update(self, db, table, filter, update, option)
  if not send_update(self, db, table, filter, update, option) then
    return false, "[MONGO ERROR]: Server closed this session when client send delete data."
  end
  return read_update(self)
end

---comment 删除语句
function protocol.request_delete(self, db, table, array, option)
  if not send_delete(self, db, table, array, option) then
    return false, "[MONGO ERROR]: Server closed this session when client send delete data."
  end
  return read_delete(self)
end

return protocol
