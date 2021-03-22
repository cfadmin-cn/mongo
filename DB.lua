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
      ok, ret, err = xpcall(db[name], traceback, db, ...)
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
function DB:find(database, collect, filter, option)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "find", database, collect, filter, option)
end

-- 插入语句
function DB:insert(database, collect, documents, option)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "insert", database, collect, documents, option)
end

-- 修改语句
function DB:update(database, collect, filter, update, option)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "update", database, collect, filter, update, option)
end

-- 删除语句
function DB:delete(database, collect, filter, option)
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return run_query(self, "delete", database, collect, filter, option)
end

function DB:count()
  assert(self and self.INITIALIZATION, "DB needs to be initialized first.")
  return self.current, self.max, #self.co_pool, #self.db_pool
end

return DB
