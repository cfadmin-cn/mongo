# Lua MongoDB Driver

  基于`cfadmin`框架实现的`MongoDB Driver`.

## 特性

  * 完善的`CURD API`支持;
  
  * 使用最新版的协议(`OP_MSG`)交互更加高效;

  * 内置社区内最好的`BSON`解析器, 使用方便、简单、直观;

  * 更简洁的语法降低学习成本, 核心协议实现仅用`1500`多行代码完成;

## 类型

  * 字符串(`String`)

  * 二进制类型(`Binary`/`MD5`/`uuid`)

  * 正则表达式(`Regex`)

  * 表(`table`/`array`)

  * 空(`null`)

  * 未定义(`undefined`)

  * 时间(`datetime`/`timestamp`)

  * 对象ID(`objectid`)

  * 整型(`int32`/`int64`)

  * 浮点型(`Double`)

  * 布尔(`true`/`false`)

  * 大小值(`MaxKey`/`MinKey`)

  * 代码(java script code)

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

  * `opt.host` - `string`类型, 服务器域名(默认是:"localhost");

  * `opt.port` - `integer`类型, 服务器端口(默认是:27017");

  * `opt.SSL` - `boolean`类型, 是否需要使用`SSL`协议握手;

  * `opt.auth_mode` - `string`类型, 授权验证模式(仅支持:`SCRAM-SHA-1`);

  * `opt.db` - `string`类型, 授权数据库名称(默认是:"admin");

  * `opt.username` - `string`类型, 授权用户账号;

  * `opt.password` - `string`类型, 授权用户密码;

  调用此构造方法将会创建`MongoDB`对象.

### 2. 连接服务器

  `function mongo:connect() return true | nil, string end`

  开发者在创建`MongoDB`对象的时候如果填写了`username`与`password`, 调用此方法的时候会自动完成授权认证.

  但可能存在授权操作不完善(例如不可读、写)的情况, 这就可能出现鉴权成功但后续执行`CRUD`时提示鉴权的可能;

  如遇到以上问题, 请开发者自行使用相关管理工具解决.

  成功返回`true`, 失败返回`false`与失败信息`string`,

### 3. 查询语句

  `function mongo:find(database, collect, filter, option) return info, id | nil, string end`

  * `database` - `string`类型, MongoDB的数据库名称;

  * `collect`  - `string`类型, MongoDB的集合名称;

  * `filter`   - `table`类型, 一个符合语法规范的查询条件;

  * `option`   - `table`类型, 可选参数(`option.sort`/`option.limit`/`option.skip`/`option.cursor`/`option.size`);

  `filter`可以用作查询的过滤条件, 例如: `{ nickname = "李小龙" }`或一个空表; (但是不能为空数组);

  `option`参数有2组4个参数, 其组合作用为`游标分页`与`跳跃分页`:

    * 跳跃分页(`limit`与`skip`): 操作方式类似结构化数据库`MySQL`、`Oracle`等的`LIMIT`与`OFFSET`;

    * 游标分页(`cursor`与`size`): 每次迭代(包括第一次)都会返回`size`条数据与下次迭代的游标`ID`(游标ID是一次性的);

  `sort`指定了排序的方式, 表达式为: `{sort = {age =  1}}` 或者 `{sort = {age =  -1}}`, (1)升序、(-1)降序;

  成功返回`table`类型的`info`与`integer`类型的`cursor id`, 失败返回`false`与失败信息`string`.

### 3. 插入语句

  `function mongo:insert(database, collect, documents, option) return info | nil, string end`

  * `database`  - `string`类型, MongoDB的数据库名称;

  * `collect`   - `string`类型, MongoDB的集合名称;

  * `documents` - `table数组`类型, 包含(至少)有一个或者多个(可选)文档的数组;

  * `option`   - `table`类型, 可选参数(`option.ordered`);

  `documents`为文档数组, 即使只插入一条数据也应该使用这样的数组表达式: `{ {nickname = "名称", age = 18 }}`;

  `option.ordered`默认为`false`(如果插入多条的时候出错, 则忽略并继续处理后续数据), 设置为`true`则会不再处理后续数据;

  成功返回`table`类型的`info`, 失败返回`false`与失败信息`string`.

### 4. 更新语句

  `function mongo:update(database, collect, filter, set, option) return info | nil, string end`

  * `database`  - `string`类型, MongoDB的数据库名称;

  * `collect`   - `string`类型, MongoDB的集合名称;

  * `filter`  - `table`类型, 查询过滤的条件;

  * `set`   - `table`类型, 查询修改的内容;

  * `option`   - `table`类型, 可选参数(`option.upsert`/`option.multi`);

  `filter`可以用作查询的过滤条件, 例如: `{ nickname = "李小龙" }`或一个空表; (但是不能为空数组);

  `set`参数为一个`文档`或者`文档更新语法`, 具体使用方式类似`mongo shell语法`:

    * `mongo:update('db', 'table', { name = "123" }, { ['$set'] = { name = "234" } } )`

    * `mongo:update('db', 'table', { name = "123" }, { ['$inc'] = { age = 1 } } )`

    * `mongo:update('db', 'table', { name = "123" }, { ['$unset'] = { age = 1} } )`

  `option.upsert`默认为`false`; 如果设置为`true`, 则表示不存在`set`指定的记录则插入;

  `option.multi`默认为`false`(只更新找到的第一条记录), 如果设置为`true`, 则表示更新所有记录;

  成功返回`table`类型的`info`, 失败返回`false`与失败信息`string`.

### 5. 删除语句

  `function mongo:delete(database, collect, option) return info | nil, string end`

  * `database`  - `string`类型, MongoDB的数据库名称;

  * `collect`   - `string`类型, MongoDB的集合名称;

  * `filter`  - `table`类型, 查询过滤的条件;

  * `option`   - `table`类型, 可选参数(`option.one`);

  `filter`可以用作查询的过滤条件, 例如: `{ nickname = "李小龙" }`或一个空表; (但是不能为空数组);

  `option.one`属性指定为`1`表示只删除1条数据, 其它值与默认情况下都表示删除所有匹配项;

  成功返回`table`类型的`info`, 失败返回`false`与失败信息`string`.

### 6. 统计查询

  `function mongo:count(database, collect, filter) return info | nil, string end`

  * `database`  - `string`类型, MongoDB的数据库名称;

  * `collect`   - `string`类型, MongoDB的集合名称;

  * `filter`  - `table`类型, 查询过滤的条件;

  根据`filter`参数的过滤条件(可以为空), 统计集合内符合过滤条件的数量;

  成功返回`table`类型的`info`, 失败返回`false`与失败信息`string`.

### 7. 聚合查询

  `function mongo:aggregate(database, collect, filters, option) return info | nil, string end`

  * `database`  - `string`类型, MongoDB的数据库名称;

  * `collect`   - `string`类型, MongoDB的集合名称;

  * `filters`  - `table`类型, 查询过滤的条件的数组;

### 8. 断开连接

  `function mongo:close() return nil end`

  此方法无返回值.

## 使用示例:

  以下示例展示了基础API的使用方法.

### 1. CRUD操作
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

  输出如下:

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

### 2. 聚合操作

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

local tab, id

tab, err = m:count(database, collect, {})
if not tab then
  return print(false, err)
end
var_dump(tab)

tab, id = m:aggregate(database, collect
  ,{
    { ["$match"] = {age = 26} },
    { ["$sort"] =  {_id = -1} },
  }
  ,{
    -- cursor = 8896207551195673826,  -- 游标的写法.
    -- size = 3, -- 聚合函数内单独指定size无意义, 如有必要请在fileters里使用$limit
  }
)
if not tab then
  return print(false, err)
end
print(tab, #tab, id)
-- var_dump(tab)

require "logging":DEBUG("结束")
```

  输出如下:

```bash
Candy@CandyMi MSYS ~/stt_trade
$ ./cfadmin.exe
[2021-03-02 14:15:47,675] [@script/main.lua:92] [DEBUG] : "开始"
{
      ["acknowledged"] = true,
      ["count"] = 122,
}
table: 0x8000f9f30      101     4732584172034615803
[2021-03-02 14:15:47,685] [@script/main.lua:130] [DEBUG] : "结束"
```

## 提示

  * 如果对`bson`库的性能有要求, 请务必编译`lbson.so`库文件出来.

  * 当某字段需要插入`空数组`的时候, 可以使用内置的`bson.empty_array()`方法进行构造.

  * 查询未指定`cursor`但是指定了`size`, 数据如果超出`size`就会返回游标`ID`.

  * 一些特殊数据类型(`bson.objectid`)需要`bson`的构造方法来编码传递, 如果有疑问可以咨询作者.

  * 授权仅支持用户名/密码授权(SCRAM-SHA-1);

  * 本驱动仅支持MongoDB 3.6及以上版本;

## 建议

  * 使用`VS CODE`安装`lua-language-server`插件后使用会自动得到上述代码不全与编码提示;

  * 如果您有更好的需求与建议请留言到ISSUE, 作者在收到后会尽快回复并与您进行沟通;

## LICENSE

  [BSD LICENSE](https://github.com/CandyMi/mongo/blob/master/LICENSE)