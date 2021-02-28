# Lua MongoDB Driver

  基于`cfadmin`框架实现的`MongoDB Driver`.

## 特性

  * 支持基础的`CURD API`;

  * 内置高效、类型丰富的`BSON`解析器;
  
  * 使用最新版的交互协议(`OP_MSG`);

  * 类似`mongo shell`的语法减少了学习成本, 简单易用的`API`可以让大家更容易上手使用;

  * 类型支持(`string`/`double`/`table`/`array`/`null`/`datetime`/`timestamp`/`objectid`/`int32`/`int64`/`minkey`/`maxkey`/`uuid`/`md5`/`binary`/`regex`)(参考下面的示例代码);

## 效率

  本库使用了纯`Lua`实现了`bson`的序列化与反序列化, 但是经过一段时间的使用与测试发现体验并不乐观; 因为特性原因必须增加复杂的反序列化流程;
  
  而我们主要在与`mongodb`服务器进行交互的时候回才会使用`bson`, 并且交互请求的编码不会有太多内容所以主要性能问题定位在`bson`的反序列化上.

  在经过详细的测试后发现纯`Lua`实现的`BSON`反序列化性能十分糟糕，所以最后经过使用使用`C`语言重写的反序列化方法类解决这方面带来的一些负面影响.
  
  值得一提的是`C`语言版的实现效率是`Lua`的`100`倍, 所以不用再担心性能问题了; 并且内部能自动检查用户是否有编译出`C`版本的`bson`实现, 用户`有需要`编译即可.

## 安装

  * `clone`项目到`3rd`目录下, 这样就完成了基础安装;

  * (可选) 如果您安装有`GCC`或者`clang`编译器; 那么可以进入`mongo`的目录运行`make build`编译`lbson.so`;

## 使用介绍

  `local mongo = require "mongo"`

  `local bson = require "mongo.bson"`

### 1. 创建对象

  `function mongo:new(opt) return mongo end`

  * opt.host - `string`类型, 服务器域名(默认是:"localhost");

  * opt.port - `integer`类型, 服务器端口(默认是:27017");

  * opt.SSL - `boolean`类型, 是否需要使用`SSL`协议握手;

  * opt.auth_mode - `string`类型, 授权验证模式(仅支持:`SCRAM-SHA-1`);

  * opt.db - `string`类型, 授权数据库名称(默认是:"admin");

  * opt.username - `string`类型, 授权用户账号;

  * opt.password - `string`类型, 授权用户密码;

  在您填写了用户名称与密码之后, 将会在连接的时候自动检查是否需要授权认证


### 2. 连接服务器

  `function mongo:connect() return true | nil, string end`

  成功返回`true`, 失败返回`false`与失败信息`string`,

### 3. 查询语句

  `function mongo:find(database, collect, filter, option) return info | nil, string end`

  * `database` - `string`类型, MongoDB的数据库名称;

  * `collect`  - `string`类型, MongoDB的集合名称;

  * `filter`   - `table`类型, 一个符合语法规范的查询条件;

  * `option`   - `table`类型, 可选参数(`limit`/`skip`);

  成功返回`table`类型的info, 失败返回`false`与失败信息`string`.

### 3. 插入语句

  `function mongo:insert(database, collect, documents, option) return info | nil, string end`

  * `database`  - `string`类型, MongoDB的数据库名称;

  * `collect`   - `string`类型, MongoDB的集合名称;

  * `documents` - `table数组`类型, 包含(至少)有一个或者多个(可选)文档的数组;

  * `option`   - `table`类型, 可选参数(`option.ordered`);

  成功返回`table`类型的info, 失败返回`false`与失败信息`string`.

### 4. 更新语句

  `function mongo:update(database, collect, filter, set, option) return info | nil, string end`

  * `database`  - `string`类型, MongoDB的数据库名称;

  * `collect`   - `string`类型, MongoDB的集合名称;

  * `filter`  - `table`类型, 查询过滤的条件;

  * `set`   - `table`类型, 查询修改的内容;

  * `option`   - `table`类型, 可选参数(`option.upsert`/`option.multi`);

  成功返回`table`类型的info, 失败返回`false`与失败信息`string`.

### 5. 删除语句

  `function mongo:delete(database, collect, option) return info | nil, string end`

  * `database`  - `string`类型, MongoDB的数据库名称;

  * `collect`   - `string`类型, MongoDB的集合名称;

  * `filter`  - `table`类型, 查询过滤的条件;

  * `option`   - `table`类型, 可选参数(`option.limit`);

  成功返回`table`类型的info, 失败返回`false`与失败信息`string`.

### 6. 断开连接

  `function mongo:close() return nil end`

  此方法无返回值.

## 使用示例:

```lua
require"utils"

local mongo = require "mongo"
local bson = require "mongo.bson"

local m = mongo:new {
  db = "mydb",        -- 授权DB名称
  username = "admin", -- 授权用户名称
  password = "admin", -- 授权用户密码
}

require "logging":DEBUG("开始")

local ok, err = m:connect()
if not ok then
  return print(false, err)
end

local database, collect = "mydb", "table"

local tab

tab, err = m:insert(database, collect, {
  { nickname = "車先生", age = 30, ts = bson.timestamp(), nullptr = bson.null(), regex = bson.regex("/先生/i"), uuid = bson.uuid() },
  { nickname = "車太太", age = 26, ts = bson.timestamp(), nullptr = bson.null(), regex = bson.regex("/太太/i"), guid = bson.guid() },
})
if not tab then
  return print(false, err)
end
var_dump(tab)

tab, err = m:find(database, collect)
if not tab then
  return print(false, err)
end
var_dump(tab)

tab, err = m:update(database, collect, { nickname = "車太太" }, { ["$set"] = { nickname = "車先生" }})
if not tab then
  return print(false, err)
end
var_dump(tab)

tab, err = m:find(database, collect)
if not tab then
  return print(false, err)
end
var_dump(tab)

tab, err = m:delete(database, collect, { nickname = "車先生" } )
if not tab then
  return print(false, err)
end
var_dump(tab)

m:close()

require "logging":DEBUG("结束")
```
```bash
[candy@MacBookPro:~/Documents/cfadmin] $ ./cfadmin
[2021-02-28 13:26:35,947] [@script/main.lua:94] [DEBUG] : "开始"
{
      ["acknowledged"] = true,
      ["insertedCount"] = 2,
}
{
      [1] = {
            ["_id"] = "603b298bb7bacd21ef3c59d9",
            ["regex"] = "/先生/i",
            ["nickname"] = "車先生",
            ["age"] = 30,
            ["nullptr"] = userdata: 0x0,
            ["ts"] = 1614489995947,
            ["uuid"] = "ba1c1b0c-191b-4480-9d68-cb4c5755cd5d",
      },
      [2] = {
            ["_id"] = "603b298bb7bacd21ef3c59da",
            ["regex"] = "/太太/i",
            ["nickname"] = "車太太",
            ["guid"] = "f47bf20c-7a7f-4c61-603b-298b2507b289",
            ["age"] = 26,
            ["nullptr"] = userdata: 0x0,
            ["ts"] = 1614489995947,
      },
}
{
      ["matchedCount"] = 1,
      ["modifiedCount"] = 1,
      ["acknowledged"] = true,
}
{
      [1] = {
            ["_id"] = "603b298bb7bacd21ef3c59d9",
            ["regex"] = "/先生/i",
            ["nickname"] = "車先生",
            ["age"] = 30,
            ["nullptr"] = userdata: 0x0,
            ["ts"] = 1614489995947,
            ["uuid"] = "ba1c1b0c-191b-4480-9d68-cb4c5755cd5d",
      },
      [2] = {
            ["_id"] = "603b298bb7bacd21ef3c59da",
            ["regex"] = "/太太/i",
            ["nickname"] = "車先生",
            ["guid"] = "f47bf20c-7a7f-4c61-603b-298b2507b289",
            ["age"] = 26,
            ["nullptr"] = userdata: 0x0,
            ["ts"] = 1614489995947,
      },
}
{
      ["acknowledged"] = true,
      ["deletedCount"] = 2,
}
[2021-02-28 13:26:35,961] [@script/main.lua:135] [DEBUG] : "结束"
```

## 注意

  * 授权仅支持用户名/密码授权(SCRAM-SHA-1);

  * 本驱动仅支持MongoDB 3.6及以上版本;

## 建议

  如果您有更好的需求与建议请留言到ISSUE, 作者在收到后会尽快回复并与您进行沟通.

## LICENSE

  [BSD LICENSE](https://github.com/CandyMi/mongo/blob/master/LICENSE)