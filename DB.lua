local class = require "class"

local timer = require "internal.Timer"
local mongo = require "mongo"

local log = require "logging"
local Log = log:new({ dump = true, path = 'DB'})

local co = require "internal.Co"
local co_self = co.self
local co_wait = co.wait
local co_spawn = co.spawn
local co_wakeup = co.wakeup

local type = type
local error = error
local xpcall = xpcall
local assert = assert

local fmt = string.format

local insert = table.insert
local remove = table.remove

-- 数据库连接创建函数
local function DB_CREATE (opt)
  local db
  while 1 do
    db = mongo:new(opt)
    db:set_timeout(3)
    local connect, err = db:connect()
    if connect then
      break
    end
    Log:WARN("The connection failed. The reasons are: [" .. err .. "], Try to reconnect after 3 seconds")
    timer.sleep(3)
    db:close()
  end
  return db
end

local function add_wait(self, co)
  insert(self.co_pool, co)
end

local function pop_wait(self)
  return remove(self.co_pool)
end

local function add_db(self, db)
  insert(self.db_pool, db)
end

-- 负责创建连接/加入等待队列
local function pop_db(self)
  local session = remove(self.db_pool)
  if session then
    return session
  end
  if self.current < self.max then
    self.current = self.current + 1
    return DB_CREATE(self)
  end
  add_wait(self, co_self())
  return co_wait()
end

local function traceback(msg)
  return fmt("[%s] %s", os.date("%Y/%m/%d %H:%M:%S"), debug.traceback(co_self(), msg, 3))
end

local function run_query(self, name, ...)
  local db, ok, ret, err
  while 1 do
    db = pop_db(self)
    if db then
      local func, cls = db[name], db
      if not func then
        func, cls = db.gridfs[name], db.gridfs
      end
      ok, ret, err = xpcall(func, traceback, cls, ...)
      if db.connected then
        break
      end
      db:close()
      self.current = self.current - 1
      db, ret, err = nil, nil, nil
    end
  end
  local co = pop_wait(self)
  if co then
    co_wakeup(co, db)
  else
    add_db(self, db)
  end
  if not ok then
    return false, ret
  end
  return ret, err
end

local DB = class("DB")

function DB:ctor(opt)
  self.db   = opt.db or "admin"
  self.host = opt.host or "localhost"
  self.port = opt.port or 27017
  self.username = opt.username
  self.password = opt.password
  self.auth_mode = opt.auth_mode or "SCRAM-SHA-1"
  self.max = opt.max or 50
  self.current = 0
  -- 协程池
  self.co_pool = {}
  -- 连接池
  self.db_pool = {}
end

function DB:connect ()
  if not self.INITIALIZATION then
    add_db(self, pop_db(self))
    self.INITIALIZATION = true
    return self.INITIALIZATION
  end
  return self.INITIALIZATION
end

-- 查询语句
function DB:find(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "find", database, collect, ...)
end

-- 插入语句
function DB:insert(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "insert", database, collect, ...)
end

-- 修改语句
function DB:update(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "update", database, collect, ...)
end

-- 删除语句
function DB:delete(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "delete", database, collect, ...)
end

-- 统计
function DB:count(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "count", database, collect, ...)
end

-- 聚合
function DB:aggregate(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "aggregate", database, collect, ...)
end

-- 计算
function DB:mapreduce(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "mapreduce", database, collect, ...)
end

-- 创建索引
function DB:create_indexes(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "create_indexes", database, collect, ...)
end

-- 删除指定索引
function DB:drop_indexes(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "drop_indexes", database, collect, ...)
end

-- 获取全部索引
function DB:get_indexes(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "get_indexes", database, collect, ...)
end

-- 查找文件
function DB:gridfs_find(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "gridfs_find", database, collect, ...)
end

-- 查找文件
function DB:gridfs_findall(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "gridfs_findall", database, collect, ...)
end

-- 删除文件
function DB:gridfs_delete(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "gridfs_delete", database, collect, ...)
end

-- 下载文件
function DB:gridfs_download(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "gridfs_download", database, collect, ...)
end

-- 上传文件
function DB:gridfs_upload(database, collect, ...)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "gridfs_upload", database, collect, ...)
end

return DB
