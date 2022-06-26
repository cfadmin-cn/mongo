--[[
  LICENSE: BSD
  Author: CandyMi[https://github.com/candymi]
]]

local null = null
local type = type
local next = next
local pcall = pcall
local assert = assert
local rawlen = rawlen
local require = require
local tonumber = tonumber

local sub = string.sub
local gsub = string.gsub
local byte = string.byte
local fmt = string.format
local strpack = string.pack
local strunpack = string.unpack

local os_time = os.time

local concat = table.concat

local math_type = math.type
local math_random = math.random
local math_seed = math.randomseed
local math_toint = math.tointeger

if _VERSION then
  local num = tonumber(_VERSION:sub(5))
  if num and num < 5.4 then
    math_seed(os_time())
  end
end

local HOST  = math_random(1, 1 << 24)
local PID   = math_random(1, 1 << 16)
local INCRY

-- 这段代码让原生lua5.3也可以运行 --
local ok, crypt, hexencode, hexdecode, sys, new_tab, timestamp, md5, uuid, guid, lbson
ok, sys = pcall(require, "sys")
-- ok = false
if ok then
  local INCRY_ID = math_random(1 << 16, 1 << 18)
  local now = sys.now
  new_tab = sys.new_tab
  timestamp = function () return now() * 1e3 // 1 end
  INCRY = function()
    INCRY_ID = (INCRY_ID & 0xFFFFFF) + 1
    return INCRY_ID
  end
else
  local INCRY_ID = math_random(1 << 16, 1 << 18)
  new_tab = function (_, _) return {} end
  timestamp = function () return os_time() * 1e3 end
  INCRY = function()
    INCRY_ID = (INCRY_ID & 0xFFFFFF) + 1
    return INCRY_ID
  end
end

ok, crypt = pcall(require, "crypt")
-- ok = false
if ok and type(crypt) == 'table' then
  hexencode = crypt.hexencode
  hexdecode = crypt.hexdecode
  uuid = crypt.uuid
  guid = crypt.guid
  md5 = crypt.md5
else
  local function decode (ss)
    return tostring(tonumber(ss, 16)):char()
  end
  local function process(s)
    return fmt("%002x", byte(s))
  end
  hexencode = function (str)
   local s = gsub(str, ".", process)
   return s
  end
  hexdecode = function (str)
    local s = gsub(assert(#str & 0x01 == 0x00 and str, "Invalid hexencode."), "..", decode)
    return s
  end
end
-- 这段代码让原生lua5.3+也可以运行 --

local BSON_DOUBLE         = 0x01 -- (double)
local BSON_STRING         = 0x02 -- (utf8-string)
local BSON_TABLE          = 0x03 -- (table-字典)
local BSON_ARRAY          = 0x04 -- (table-数组)
local BSON_BINARY         = 0x05 -- (binary)
local BSON_UNDEFINE       = 0x06 -- (undefined)
local BSON_OBJECTID       = 0x07 -- (OBJECT ID)
local BSON_BOOLEAN        = 0x08 -- (0 = false, 1 = true)
local BSON_DATETIME       = 0x09 -- (int64)
local BSON_NULL           = 0x0A -- (Null value)
local BSON_CSTRING        = 0x0B -- (cstring)
local BSON_DBPOINT        = 0x0C -- (db point)
local BSON_JSCODE2        = 0x0D -- (normal JavaScript code)
local BSON_SYMBOL         = 0x0E -- (Symbol)
local BSON_JSCODE1        = 0x0F -- (JavaScript code w/ scope)
local BSON_INT32          = 0x10 -- (int32)
local BSON_TIMESTAMP      = 0x11 -- (Timestamp)
local BSON_INT64          = 0x12 -- (int64)
local BSON_DECIMAL        = 0x13 -- (DECIMAL)
local BSON_MINKEY         = 0xFF -- (MIN KEY)
local BSON_MAXKEY         = 0x7F -- (MAX KEY)

local MinKey = '\xFF'
local MaxKey = '\x7F'

local empty_table = "\x05\x00\x00\x00\x00"

local CMD = {}

CMD[BSON_DBPOINT] = function (_, _)
  return assert(nil, "Unsupported field type: [DB Point code].")
end

CMD[BSON_SYMBOL] = function (_, _)
  return assert(nil, "Unsupported field type: [Symbol code].")
end

CMD[BSON_JSCODE1] = function (_, _)
  return assert(nil, "Unsupported field type: [JavaScript code].")
end

CMD[BSON_JSCODE2] = function (str, pos)
  local _, code
  _, code, pos = strunpack("i4z", str, pos)
  return code, pos
end

CMD[BSON_UNDEFINE] = function (_, pos)
  return null, pos
end

CMD[BSON_NULL] = function (_, pos)
  return null, pos
end

CMD[BSON_MINKEY] = function (_, pos)
  return MinKey, pos
end

CMD[BSON_MAXKEY] = function (_, pos)
  return MaxKey, pos
end

CMD[BSON_BOOLEAN] = function (str, pos)
  local v
  v, pos = strunpack("<B", str, pos)
  return v == 1, pos
end

CMD[BSON_OBJECTID] = function (str, pos)
  return hexencode(sub(str, pos, pos + 11)), pos + 12
end

CMD[BSON_BINARY] = function (str, pos)
  local len, typ
  len, typ, pos = strunpack("<i4B", str, pos)
  local data = sub(str, pos, pos + len - 1)
  if typ == 0x03 or typ == 0x04 then -- UUID
    data = fmt("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
      byte(data, 1),  byte(data, 2),  byte(data, 3),  byte(data, 4),
      byte(data, 5),  byte(data, 6),
      byte(data, 7),  byte(data, 8),
      byte(data, 9),  byte(data, 10),
      byte(data, 11), byte(data, 12), byte(data, 13), byte(data, 14), byte(data, 15), byte(data, 16)
    )
  elseif typ == 0x05 then            -- MD5
    data = hexencode(data)
  end
  return data, pos + len
end

CMD[BSON_DECIMAL] = function (str, pos)
  return strunpack("<n", str, pos)
end

CMD[BSON_DOUBLE] = function (str, pos)
  return strunpack("<n", str, pos)
end

CMD[BSON_INT32] = function (str, pos)
  return strunpack("<i4", str, pos)
end

CMD[BSON_INT64] = function (str, pos)
  return strunpack("<i8", str, pos)
end

CMD[BSON_TIMESTAMP] = function (str, pos)
  return strunpack("<i8", str, pos)
end

CMD[BSON_DATETIME] = function (str, pos)
  return strunpack("<i8", str, pos)
end

CMD[BSON_TABLE] = function (_, pos)
  return new_tab(0, 16), pos
  -- return {}, pos
end

CMD[BSON_ARRAY] = function (_, pos)
  return new_tab(16, 0), pos
  -- return {}, pos
end

CMD[BSON_STRING] = function (str, pos)
  local _, data
  _, data, pos = strunpack("<i4z", str, pos)
  return data, pos
end

CMD[BSON_CSTRING] = function (str, pos)
  local v1, v2
  v1, v2, pos = strunpack("<zz", str, pos)
  return '/'..v1..'/' .. (v2 == 'i' and "i" or ""), pos
end

-- `table`与`array`的解码方法 --
local array_decode, table_decode

local function unpack_key(str, pos)
  return strunpack("<Bz", str, pos)
end

local function byte_dump(str)
  local bytes = str:gsub(".", function (s) return fmt("\\x%002x", s:byte()) end)
  return bytes
end

array_decode = function (str, pos, tab)
  local len
  len, pos = strunpack("<i4", str, pos)
  if not tab then
    tab = new_tab(16, 0)
  end
  if len == 5 and (not byte(str, pos) or byte(str, pos) == 0x00) then
    return tab, pos + 1
  end
  local _, value, typ
  while 1 do
    typ, _, pos = unpack_key(str, pos)
    value, pos = assert(CMD[typ], fmt("CMD ERROR: { type: %d(0x%002x), pos : %d, '%s' }, ", typ, typ, pos, byte_dump(str)))(str, pos)
    if typ == BSON_TABLE then
      tab[rawlen(tab) + 1], pos = table_decode(str, pos, value)
    elseif typ == BSON_ARRAY then
      tab[rawlen(tab) + 1], pos = array_decode(str, pos, value)
    else
      tab[rawlen(tab) + 1] = value
    end
    if not byte(str, pos) or byte(str, pos) == 0x00 then
      break
    end
  end
  return tab, pos + 1
end

table_decode =  function (str, pos, tab)
  local len
  len, pos = strunpack("<i4", str, pos)
  if not tab then
    tab = new_tab(0, 8)
  end
  if len == 5 and (not byte(str, pos) or byte(str, pos) == 0x00) then
    return tab, pos + 1
  end
  local key, value, typ
  while 1 do
    typ, key, pos = unpack_key(str, pos)
    value, pos = assert(CMD[typ], fmt("CMD ERROR: { type: %d(0x%002x), pos : %d, '%s' }, ", typ, typ, pos, byte_dump(str)))(str, pos)
    if typ == BSON_TABLE then
      tab[key], pos = table_decode(str, pos, value)
    elseif typ == BSON_ARRAY then
      tab[key], pos = array_decode(str, pos, value)
    else
      tab[key] = value
    end
    if not byte(str, pos) or byte(str, pos) == 0x00 then
      break
    end
  end
  return tab, pos + 1
end
-- `table`与`array`的解码方法 --


-- `table`与`array`的编码方法 --
local array_encode, table_encode, normal_encode

local function number_encode(buffers, index, value, mode)
  if mode == BSON_ARRAY then
    index = index - 1
  end
  -- int32或者int64
  if math_type(value) == "integer" then
    if value > 2147483647 or value < -2147483648 then
      buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_INT64, index)
      buffers[rawlen(buffers) + 1] = strpack("<i8", value)
    else
      buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_INT32, index)
      buffers[rawlen(buffers) + 1] = strpack("<i4", value)
    end
  else -- double
    buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_DOUBLE, index)
    buffers[rawlen(buffers) + 1] = strpack("<n", value)
  end
  return
end

local function advance_encode(buffers, index, func, mode)
  local v, typ = func()
  if mode == BSON_ARRAY then
    index = index - 1
  end
  if typ == BSON_OBJECTID or typ == BSON_CSTRING then
    buffers[rawlen(buffers) + 1] = strpack("<Bz", typ, index)
    buffers[rawlen(buffers) + 1] = v
  elseif typ == BSON_DATETIME or typ == BSON_TIMESTAMP then
    buffers[rawlen(buffers) + 1] = strpack("<Bz", typ, index)
    buffers[rawlen(buffers) + 1] = strpack("i8", v)
  elseif typ == BSON_MINKEY then
    buffers[rawlen(buffers) + 1] = strpack("<Bz", typ, index)
  elseif typ == BSON_MAXKEY then
    buffers[rawlen(buffers) + 1] = strpack("<Bz", typ, index)
  elseif typ == BSON_BINARY then
    buffers[rawlen(buffers) + 1] = strpack("<Bz", typ, index)
    buffers[rawlen(buffers) + 1] = strpack("<i4", #v - 1)
    buffers[rawlen(buffers) + 1] = v
  elseif typ == BSON_ARRAY or typ == BSON_TABLE then
    buffers[rawlen(buffers) + 1] = strpack("<Bz", typ, index)
    buffers[rawlen(buffers) + 1] = empty_table
  elseif typ == BSON_UNDEFINE then
    buffers[rawlen(buffers) + 1] = strpack("<Bz", typ, index)
  elseif typ == BSON_JSCODE2 then
    buffers[rawlen(buffers) + 1] = strpack("<Bz", typ, index)
    buffers[rawlen(buffers) + 1] = strpack("<i4z", #v + 1, v)
  end
  return
end

local tab_all = new_tab(3, 0)

local function concat_all(buffers)
  local data = concat(buffers)
  tab_all[1] = strpack("<I4", rawlen(data) + 5)
  tab_all[2] = data
  tab_all[3] = "\x00"
  return concat(tab_all)
end

array_encode = function (tab)
  local index, value = next(tab)
  if not index then
    return empty_table
  end
  if math_type(index) == 'nil' then
    return table_encode(tab)
  end
  local buffers = new_tab(rawlen(tab), 0)
  local mode = BSON_ARRAY
  while 1 do
    if value == null then -- null
      buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_NULL, index - 1)
    elseif type(value) == "function" then
      -- 闭包返回的类型可以让编码器知道应该如何编码
      advance_encode(buffers, index - 1, value, mode)
    elseif type(value) == 'number' then
      number_encode(buffers, index - 1, value, mode)
    elseif type(value) == 'string' then
      buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_STRING, index - 1)
      buffers[rawlen(buffers) + 1] = strpack("<I4z", rawlen(value) + 1, value)
    elseif type(value) == 'boolean' then
      buffers[rawlen(buffers) + 1] = strpack("<BzB", BSON_BOOLEAN, index - 1, value and 1 or 0)
    elseif type(value) == 'table' then
      local k, _ = next(value)
      if type(k) == "number" then
        buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_ARRAY, index - 1)
        buffers[rawlen(buffers) + 1] = array_encode(value)
      else
        buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_TABLE, index - 1)
        buffers[rawlen(buffers) + 1] = table_encode(value)
      end
    end
    index, value = next(tab, index)
    if not index then
      break
    end
    if mode == BSON_ARRAY then
      index = assert(type(index) == 'number' and index, "invalid key type: " .. type(index))
    else
      index = assert(type(index) == 'string' and index, "invalid key type: " .. type(index))
    end
  end
  return concat_all(buffers)
end

table_encode = function (tab)
  local key, value = next(tab)
  -- print(key, value)
  if not key then
    return empty_table
  end
  if math_type(key) == 'integer' then
    return array_encode(tab)
  end
  local buffers = new_tab(0, 32)
  local mode = BSON_TABLE
  while 1 do
    if value == null then -- null
      buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_NULL, key)
    elseif type(value) == "function" then
      -- 闭包返回的类型可以让编码器知道应该如何编码
      advance_encode(buffers, key, value, mode)
    elseif type(value) == 'number' then
      number_encode(buffers, key, value, mode)
    elseif type(value) == 'string' then
      buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_STRING, key)
      buffers[rawlen(buffers) + 1] = strpack("<I4z", rawlen(value) + 1, value)
    elseif type(value) == 'boolean' then
      buffers[rawlen(buffers) + 1] = strpack("<BzB", BSON_BOOLEAN, key, value and 1 or 0)
    elseif type(value) == 'table' then
      local k, _ = next(value)
      if type(k) == "number" then
        buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_ARRAY, key)
        buffers[rawlen(buffers) + 1] = array_encode(value)
      else
        buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_TABLE, key)
        buffers[rawlen(buffers) + 1] = table_encode(value)
      end
    end
    key, value = next(tab, key)
    if not key then
      break
    end
    key = assert(type(key) == 'string' and key, "invalid key type: " .. type(key))
  end
  return concat_all(buffers)
end

local function bson_encode_order(...)
  local array = {...}
  if #array < 1 then
    return empty_table
  end
  local buffers = new_tab(0, #array)
  for _, item in ipairs(array) do
    local key, value = item[1], item[2]
    if value == null then -- null
      buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_NULL, key)
    elseif type(value) == "function" then
      -- 闭包返回的类型可以让编码器知道应该如何编码
      advance_encode(buffers, key, value, BSON_TABLE)
    elseif type(value) == 'number' then
      number_encode(buffers, key, value, BSON_TABLE)
    elseif type(value) == 'string' then
      buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_STRING, key)
      buffers[rawlen(buffers) + 1] = strpack("<I4z", rawlen(value) + 1, value)
    elseif type(value) == 'boolean' then
      buffers[rawlen(buffers) + 1] = strpack("<BzB", BSON_BOOLEAN, key, value and 1 or 0)
    elseif type(value) == 'table' then
      local k, _ = next(value)
      if type(k) == "number" then
        buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_ARRAY, key)
        buffers[rawlen(buffers) + 1] = array_encode(value)
      else
        buffers[rawlen(buffers) + 1] = strpack("<Bz", BSON_TABLE, key)
        buffers[rawlen(buffers) + 1] = table_encode(value)
      end
    end
  end
  return concat_all(buffers)
end
-- `table`与`array`的编码方法 --

--- 使用Lua编写的bson序列化与反序列化库.
local bson = { __VERSION__ = 0.1 }

---comment 构造C的NULL指针(如果有)
function bson.null()
  return null
end

---comment 存储undefined数据类型
---@return function    @该类型的构造方法
function bson.undefined()
  return function ()
    return null, BSON_UNDEFINE
  end
end

---comment 存储合法的java script code
---@param jscode any
function bson.dump(jscode)
  return function ()
    return jscode, BSON_JSCODE2
  end
end

---comment 正则表达式
---@param re string    @合法的java script正则表达式
---@return function    @该类型的构造方法
function bson.regex(re)
  assert(type(re) == 'string' and re ~= '', "Invalid bson regex.")
  return function ()
    local v1, v2 = re:match("^/([^/]+)/(.?)")
    assert(v1 and v2, "invalid regex expressions.")
    return strpack("<zz", v1, v2 == 'i' and 'i' or ''), BSON_CSTRING
  end
end

---comment BINARY类型的构造方法
---@param  bin string  @二进制字符串
---@return function    @该类型的构造方法
function bson.bin(bin)
  assert(type(bin) == 'string', "Invalid binary buffers.")
  return function()
    return "\x00" .. bin, BSON_BINARY
  end
end

---comment BINARY类型的构造方法
---@param  bin string  @二进制字符串
---@return function    @该类型的构造方法
function bson.binary(bin)
  assert(type(bin) == 'string', "Invalid binary buffers.")
  return function()
    return "\x02" .. bin, BSON_BINARY
  end
end

if md5 then
  ---comment `MD5`类型的构造方法(只有`cfadmin`框架才可以使用)
  ---@param  buffer string   @二进制字符串
  ---@return function        @该类型的构造方法
  function bson.md5(buffer)
    assert(type(buffer) ~= 'string' or #buffer ~= 32, "Invalid MD5 string.")
    return function()
      return "\x05" .. hexdecode(buffer), BSON_BINARY
    end
  end
end

if uuid then
  ---comment `UUID`类型的构造方法
  ---comment `UUID`的`version 4`版本实现
  ---@param  id string   @二进制字符串
  ---@return function    @该类型的构造方法
  function bson.uuid(id)
    return function()
      if type(id) ~= 'string' or #id ~= 32 then
        id = uuid():gsub('-', '')
        -- print(hexdecode(id))
      end
      return "\x04" .. hexdecode(id), BSON_BINARY
    end
  end
end

if guid then
  ---comment `GUID`类型的构造方法(只有`cfadmin`框架才可以使用)
  ---comment 框架实现的算法, 具有`sequence`属性.
  ---@param  id string   @二进制字符串
  ---@return function    @该类型的构造方法
  function bson.guid(id)
    return function()
      if type(id) ~= 'string' or #id ~= 32 then
        id = guid():gsub('-', '')
        -- print(hexdecode(id))
      end
      return "\x03" .. hexdecode(id), BSON_BINARY
    end
  end
end

function bson.empty_array()
  return function ()
    return empty_table, BSON_ARRAY
  end
end

function bson.empty_table()
  return function ()
    return empty_table, BSON_TABLE
  end
end

---comment `objectid`的构造方法
---@param  oid  string | nil @如果指定必须为24个字节的字符串
---@return function          @该类型的构造方法
function bson.objectid(oid)
  if oid then
    return function ()
      return hexdecode(assert(type(oid) == 'string' and #oid == 24 and oid, "oid string must equal 24 bytes.")), BSON_OBJECTID
    end
  end
  return function ()
    return strpack("<I4I3I2>I3", os_time(), HOST, PID, INCRY()), BSON_OBJECTID
  end
end

---comment `timestamp`的构造方法
---@return function  @该类型的构造方法
function bson.timestamp(ts)
  return function ()
    if not ts then
      ts = timestamp()
    end
    return math_toint(ts), BSON_TIMESTAMP
  end
end

---comment `datetime`的构造方法
---@return function  @该类型的构造方法
function bson.datetime(dt)
  return function ()
    if not dt then
      dt = timestamp()
    end
    return math_toint(dt), BSON_DATETIME
  end
end

---comment `minkey`的构造方法
---@return function  @该类型的构造方法
function bson.minkey()
  return function ()
    return MinKey, BSON_MINKEY
  end
end

---comment `maxkey`的构造方法
---@return function  @该类型的构造方法
function bson.maxkey()
  return function ()
    return MaxKey, BSON_MAXKEY
  end
end

---comment `bson`序列化方法
---@param tab table                @合法有效的`table`
---@return string | nil, string    @合法的`bson`字符串
function bson.encode(tab)
  assert(type(tab) == 'table', "Invalid encode table.")
  if not next(tab) then
    return empty_table
  end
  return table_encode(tab)
end

---comment `bson`反序列化方法
---@param str string            @合法的`bson`字符串
---@return table | nil, string  @合法的`lua` `table`
function bson.decode(str)
  if type(str) ~= 'string' or rawlen(str) < 5 or byte(str, rawlen(str)) ~= 0x00 then
    return nil, "Invalid bson string."
  end
  local success, info, err = pcall(table_decode, str)
  if success then
    if not info then
      return false, err
    end
    return info
  end
  return false, info
end

---comment `bson`序列化方法(自主排序)
---@return string | nil, string    @合法的`bson`字符串
function bson.bson_encode_order(...)
  return bson_encode_order(...)
end

-- 如果有可能, 使用C版本的BSON解析器
ok, lbson = pcall(require, "lbson")
if ok and type(lbson) == 'table' then
  bson.decode = lbson.decode or bson.decode
end

return bson
