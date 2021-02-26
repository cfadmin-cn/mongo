# Lua MongoDB Driver

  基于`cfadmin`框架实现的`MongoDB Driver`.

## 特性

  * 类似`mongo shell`的语法减少了学习成本, 简单易用的`API`可以让大家更容易上手使用;

  * 丰富的类型支持(`string`/`number`/`table`/`array`/`null`/`datetime`/`timestamp`/`objectid`/`int32`/`int64`/`minkey`/`maxkey`/`uuid`/`md5`/`binary`/`regex`);

## 效率

  在测试期间使用了纯`Lua`实现了`bson`的序列化与反序列化, 但是经过一段时间的使用与测试在数据上并不乐观;
  
  我们主要是在与`mongodb`服务器进行交互的时候回才会使用`bson`, 而交互的编码不会有太多内容所以主要性能问题出现在`bson`的反序列化上.

  经过测试发现纯`Lua`实现的`BSON`反序列化性能十分糟糕，并且在内部会极度影响到用户体验与交互的敏感度; 所以我们内部使用`C`语言重写的反序列化方法.
  
  值得一提的时候`C`语言版的实现效率是`Lua`的100倍, 所以不用再担心性能问题了; 并且我们的内部能自动识别用户是否有编译出`C`版本的`bson`实现, 用户仅需运行`编译命令`即可.

## 安装

  * `clone`项目到`3rd`目录下, 这样就完成了基础安装;

  * (可选) 如果您安装有`GCC`或者`clang`编译器; 那么可以进入`mongo`的目录运行`make build`编译`lbson.so`;

## 使用介绍

  `local mongo = require "mongo"`

  `local bson = require "mongo.bson"`

### 1. 创建对象

  `function mongo:new(opt) return mongo  end`

### 2. 连接服务器

  `function mongo:connect(opt) return true | nil, string  end`

### 3. 查询语句

  `function mongo:find(db, table, option) return info, | nil, string  end`

### 3. 插入语句

  `function mongo:insert(db, table, option) return info, | nil, string  end`

### 4. 更新语句

  `function mongo:update(db, table, option) return info, | nil, string  end`

### 5. 删除语句

  `function mongo:delete(db, table, option) return info, | nil, string  end`

### 6. 断开连接

  `function mongo:close() return nil end`
