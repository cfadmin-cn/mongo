local bson = require "mongo.bson"

local crypt = require "crypt"
local md5 = crypt.md5
local hexencode = crypt.hexencode
local randomkey = crypt.randomkey_ex

local protocol = require "mongo.protocol"
local request_gridfs_files_list = protocol.request_gridfs_files_list
local request_gridfs_chunks_list = protocol.request_gridfs_chunks_list
local request_gridfs_files_delete = protocol.request_gridfs_files_delete
local request_gridfs_chunks_delete = protocol.request_gridfs_chunks_delete
local request_gridfs_files_upload = protocol.request_gridfs_files_upload
local request_gridfs_chunks_upload = protocol.request_gridfs_chunks_upload

local type = type
local ipairs = ipairs
local assert = assert

local io_type = io.type
local fmt = string.format
local tinsert = table.insert

local sys = require "sys"
local new_tab = sys.new_tab

local class = require "class"

local GRIDFS = class("GRIDFS")

function GRIDFS:ctor(opt)
  self.ctx = opt.ctx
end

---comment 上传
---@param database string      @需要上传的数据库名称
---@param collect string       @需要上传的集合名称
---@param filename string      @文件名称
---@param file string | file*  @文件内容或者文件对象(文件对象需要自行关闭)
---@param meta table           @文件元数据信息, 一般情况下课忽略
function GRIDFS:gridfs_upload(database, collect, filename, file, meta)
  -- 检查文件名
  if type(filename) ~= 'string' or filename == '' then
    filename = randomkey(16, true)
  end
  -- 检查文件对象
  if io_type(file) == "file" then
    file = file:read "*a"
  end
  local filelen = #file
  assert(type(file) == 'string', "Invalid file content.")
  -- 生成OID
  local oid = hexencode(bson.objectid()())
  local tab1, tab2, err
  -- 开始写入数据
  if filelen < 16777216 then
    tab2, err = request_gridfs_chunks_upload(self.ctx, database, collect, oid, file)
    if not tab2 then
      return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab2.code, tab2.errmsg)
    end
  else
    -- 大文件的分片传输实现.
    local s, e = 1, 16777216
    local list = new_tab(128, 0)
    while true do
      list[#list+1] = file:sub(s, e)
      if e >= filelen then
        break
      end
      s = e + 1
      e = e + 16777216
    end
    local n = 0
    for _, content in ipairs(list) do
      tab2, err = request_gridfs_chunks_upload(self.ctx, database, collect, oid, content)
      if not tab2 then
        return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab2.code, tab2.errmsg)
      end
      n = n + tab2['n']
    end
    tab2["n"] = n
  end
  -- 数据插入完毕后再插入文件信息.
  tab1, err = request_gridfs_files_upload(self.ctx, database, collect, oid, filename, filelen, md5(file, true), meta)
  if not tab1 then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab1.code, tab1.errmsg)
  end
  return { acknowledged = true, _id = oid, insertedCount = tab1["n"], sharedCount = tab2["n"] }
end

---comment 下载
---@param database string   @需要下载的数据库名称
---@param collect string    @需要下载的集合名称
---@param filter table      @过滤条件
function GRIDFS:gridfs_download(database, collect, filter)
  local list = new_tab(128, 0)
  local id = 0
  while true do
    local tab, err = request_gridfs_chunks_list(self.ctx, database, collect, type(filter) == 'table' and filter or {}, id)
    if not tab then
      return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
    end
    local cursor = tab.cursor
    for _, item in ipairs(cursor.firstBatch or cursor.nextBatch) do
      -- item.data = nil
      tinsert(list, item)
    end
    id = cursor.id
    if id == 0 then
      break
    end
  end
  return list
end

---comment 删除
---@param database string   @需要下载的数据库名称
---@param collect string    @需要下载的集合名称
---@param filter table      @过滤条件
function GRIDFS:gridfs_delete(database, collect, filter, limit)
  local tab1, tab2, err
  tab1, err = request_gridfs_files_delete(self.ctx, database, collect, filter, limit)
  if not tab1 then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab1.code, tab1.errmsg)
  end
  -- var_dump(tab)
  tab2, err = request_gridfs_chunks_delete(self.ctx, database, collect, filter, limit)
  if not tab2 then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab2.code, tab2.errmsg)
  end
  -- var_dump(tab)
  -- return true
  return { acknowledged = true, deletedCount = tab1.n == tab2.n and tab1.n or tab2.n }
end

---comment 查询所有
---@param database string   @需要查询的数据库名称
---@param collect string    @需要查询的集合名称
---@param id integer        @游标ID
function GRIDFS:gridfs_findall(database, collect, id)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid gridfs collect or database.")
  local tab, err = request_gridfs_files_list(self.ctx, database, collect, {}, id)
  if not tab then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return tab.cursor.firstBatch or tab.cursor.nextBatch, tab.cursor.id
end

---comment 查询指定
---@param database string   @需要查询的数据库名称
---@param collect string    @需要查询的集合名称
---@param filter table      @过滤条件
---@param id integer        @游标ID
function GRIDFS:gridfs_find(database, collect, filter, id)
  assert(type(database) == 'string' and database ~= '' and type(collect) == 'string' and collect ~= '', "Invalid gridfs collect or database.")
  local tab, err = request_gridfs_files_list(self.ctx, database, collect, filter, id)
  if not tab then
    return false, err or fmt('{"errcode":%d,"errmsg":"%s"}', tab.code, tab.errmsg)
  end
  return tab.cursor.firstBatch or tab.cursor.nextBatch, tab.cursor.id
end

-- 释放资源
function GRIDFS:close()
  self.ctx = nil
end

return GRIDFS