/*
**  LICENSE: BSD
**  Author: CandyMi[https://github.com/candymi]
*/

#define LUA_LIB

#include <core.h>

enum BSON_TYPE_T {
  BSON_DOUBLE         = 0x01, // (double)
  BSON_STRING         = 0x02, // (utf8-string)
  BSON_TABLE          = 0x03, // (table-字典)
  BSON_ARRAY          = 0x04, // (table-数组)
  BSON_BINARY         = 0x05, // (binary)
  BSON_UNDEFINED      = 0x06, // (undefined)
  BSON_OBJECTID       = 0x07, // (OBJECT ID)
  BSON_BOOLEAN        = 0x08, // (0 = false, 1 = true)
  BSON_DATETIME       = 0x09, // (int64)
  BSON_NULL           = 0x0A, // (Null value)
  BSON_REGEX          = 0x0B, // (REGEX)
  BSON_DBPOINT        = 0x0C, // (db point)
  BSON_JSCODE2        = 0x0D, // (normal JavaScript code)
  BSON_SYMBOL         = 0x0E, // (Symbol)
  BSON_JSCODE1        = 0x0F, // (JavaScript code w/ scope)
  BSON_INT32          = 0x10, // (int32)
  BSON_TIMESTAMP      = 0x11, // (Timestamp)
  BSON_INT64          = 0x12, // (int64)
  BSON_DECIMAL        = 0x13, // (DECIMAL)
  BSON_MINKEY         = 0xFF, // (MIN KEY)
  BSON_MAXKEY         = 0x7F, // (MAX KEY)
};

struct BSON_T {
  int len;
  const uint8_t *ptr;
};

// 解码
static int bson_decode_array(lua_State *, struct BSON_T *);
static int bson_decode_table(lua_State *, struct BSON_T *);


//检查`bson buffer`是否超出.
static void bson_assert(lua_State *L, struct BSON_T *b, int bytes) {
  if (b->len - bytes < 0)
    luaL_error(L, "BSON buffer was not enough.");
}

// 读取int8_t
static inline int8_t read_int8(lua_State *L, struct BSON_T *b) {
  bson_assert(L, b, 1);
  int v = b->ptr[0];
	b->ptr += 1;
	b->len -= 1;
  return v ;
}

// 读取uint8_t
static inline uint8_t read_uint8(lua_State *L, struct BSON_T *b) {
  return (uint8_t)read_int8(L, b);
}

// 读取int32_t
static inline int32_t read_int32(lua_State *L, struct BSON_T *b) {
  bson_assert(L, b, 4);
  uint32_t v = b->ptr[0] | b->ptr[1] << 8 | b->ptr[2]<<16 | b->ptr[3] << 24;
	b->ptr += 4;
	b->len -= 4;
  return v;
}

// 读取int64_t
static inline int64_t read_int64(lua_State *L, struct BSON_T *b) {
  bson_assert(L, b, 8);
	uint32_t lo = b->ptr[0] | b->ptr[1]<<8 | b->ptr[2]<<16 | b->ptr[3]<<24;
	uint32_t hi = b->ptr[4] | b->ptr[5]<<8 | b->ptr[6]<<16 | b->ptr[7]<<24;
	uint64_t v = (uint64_t)lo | (uint64_t)hi<<32;
	b->ptr += 8;
	b->len -= 8;
  return v ;
}

// 读取double_t
static inline lua_Number read_double(lua_State *L, struct BSON_T *b) {
	bson_assert(L, b, 8);
	union { uint64_t i; double d; } val;
	uint32_t lo = b->ptr[0] | b->ptr[1]<<8 | b->ptr[2]<<16 | b->ptr[3]<<24;
	uint32_t hi = b->ptr[4] | b->ptr[5]<<8 | b->ptr[6]<<16 | b->ptr[7]<<24;
	val.i = (uint64_t)lo | (uint64_t)hi << 32;
	b->ptr += 8;
	b->len -= 8;
	return val.d;
}

// 读取cstring
static inline const uint8_t* read_cstring(lua_State *L, struct BSON_T *b, size_t *len){
  int index = 0;
  while (1) {
    if (index == b->len)
      luaL_error(L, "[BSON ERROR] : Invalid cstring buffer.");
    if (b->ptr[index] == '\x00')
      break;
    index++;
  }
  *len = index;
  const uint8_t *p = b->ptr;
	b->ptr += index + 1;
	b->len -= index + 1;
  return p;
}

// 读取objectid
static inline const char * read_objectid(lua_State *L, struct BSON_T *b, size_t *len) {
  (void)L;
  const uint8_t *p = b->ptr;
  *len = 12;
  b->len -= 12;
  b->ptr += 12;
  return (const char *)p;
}

static inline const char * hex_objectid(char *src, const char *oid) {
  snprintf(src, 25, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
    (uint8_t)oid[0], (uint8_t) oid[1], (uint8_t) oid[2], (uint8_t) oid[3],
    (uint8_t)oid[4], (uint8_t) oid[5], (uint8_t) oid[6], (uint8_t) oid[7],
    (uint8_t)oid[8], (uint8_t) oid[9], (uint8_t) oid[10], (uint8_t) oid[11]
  );
  return src;
}

static inline const char * hex_spec_uuid(char *src, const char *bin) {
  snprintf(src, 37, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
    (uint8_t)bin[0], (uint8_t) bin[1], (uint8_t) bin[2], (uint8_t) bin[3],
    (uint8_t)bin[4], (uint8_t) bin[5], (uint8_t) bin[6], (uint8_t) bin[7],
    (uint8_t)bin[8], (uint8_t) bin[9], (uint8_t) bin[10], (uint8_t) bin[11],
    (uint8_t)bin[12], (uint8_t) bin[13], (uint8_t) bin[14], (uint8_t) bin[15]
  );
  return src;
}

static inline const char * hex_spec_md5(char *src, const char *bin) {
  snprintf(src, 33, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
    (uint8_t)bin[0], (uint8_t) bin[1], (uint8_t) bin[2], (uint8_t) bin[3],
    (uint8_t)bin[4], (uint8_t) bin[5], (uint8_t) bin[6], (uint8_t) bin[7],
    (uint8_t)bin[8], (uint8_t) bin[9], (uint8_t) bin[10], (uint8_t) bin[11],
    (uint8_t)bin[12], (uint8_t) bin[13], (uint8_t) bin[14], (uint8_t) bin[15]
  );
  return src;
}

// 解析为数组
static int bson_decode_array(lua_State *L, struct BSON_T *b) {
  bson_assert(L, b, 5);
  lua_createtable(L, 16, 0);
  // 如果是个空表
  if (b->len == 5 && b->ptr[0] == 0x05 && b->ptr[4] == 0x00)
    return 1;
  b->len -= 4; b->ptr += 4;
  const char *value;
  size_t key_len, value_len;
  int index = 1;
  while (1) {
    uint8_t type = read_uint8(L, b);
    switch (type) {
      case BSON_OBJECTID:
        read_cstring(L, b, &key_len);
        value = read_objectid(L, b, &value_len);
        char oid[25]; 
        lua_pushlstring(L, hex_objectid(oid, value), 24);
        break;
      case BSON_NULL:
      case BSON_UNDEFINED:
        read_cstring(L, b, &key_len);
        lua_pushlightuserdata(L, NULL);
        break;
      case BSON_BOOLEAN:
        read_cstring(L, b, &key_len);
        read_uint8(L, b) == 0x01 ? lua_pushboolean(L, 1) : lua_pushboolean(L, 0);
        break;
      case BSON_MINKEY:
      case BSON_MAXKEY:
        read_cstring(L, b, &key_len);
        lua_pushlstring(L, (const char *)(type == BSON_MINKEY ? "\xff" : "\x7f"), 1);
        break;
      case BSON_INT32:
        read_cstring(L, b, &key_len);
        lua_pushinteger(L, read_int32(L, b));
        break;
      case BSON_INT64:
      case BSON_DATETIME:
      case BSON_TIMESTAMP:
        read_cstring(L, b, &key_len);
        lua_pushinteger(L, read_int64(L, b));
        break;
      case BSON_DOUBLE :
      case BSON_DECIMAL :
        read_cstring(L, b, &key_len);
        lua_pushnumber(L, read_double(L, b));
        break;
      case BSON_REGEX:
        read_cstring(L, b, &key_len);
        size_t vlen = 0 ;
        value = (const char *)read_cstring(L, b, &value_len);
        vlen += value_len;
        read_cstring(L, b, &value_len);
        if (value_len > 0)
          vlen += value_len;
        lua_pushfstring(L, "/%s/%s", value, value_len == 0 ? "" : "i");
        (void)vlen;
        break;
      case BSON_BINARY :
        read_cstring(L, b, &key_len);
        value_len = read_int32(L, b);
        type = read_uint8(L, b);
        if (type == 0x03 || type == 0x04) {
          char uuid[37];
          lua_pushlstring(L, hex_spec_uuid(uuid, (const char *)b->ptr), 36);
        } else if (type == 0x05) {
          char md5[33];
          lua_pushlstring(L, hex_spec_md5(md5, (const char *)b->ptr), 32);
        } else {
          lua_pushlstring(L, (const char *)b->ptr, value_len);
        }
        b->len -= value_len;
        b->ptr += value_len;
        break;
      case BSON_STRING :
        read_cstring(L, b, &key_len);
        value_len = read_int32(L, b);
        value = (const char *)read_cstring(L, b, &value_len);
        lua_pushlstring(L, value, value_len);
        break;
      case BSON_ARRAY :
        read_cstring(L, b, &key_len);
        bson_decode_array(L, b);
        break;
      case BSON_TABLE :
        read_cstring(L, b, &key_len);
        bson_decode_table(L, b);
        break;
      case BSON_JSCODE2:
        read_cstring(L, b, &key_len);
        value_len = read_int32(L, b);
        value = (const char *)read_cstring(L, b, &value_len);
        lua_pushlstring(L, value, value_len);
        break;
      case '\x00':
        return 1;
      default:
        luaL_error(L, "[BSON ERROR] : Unsupported field type:[%U], pos:[%d].", b->ptr[0], b->len);
    }
    lua_rawseti(L, -2, index++);
  }
  return 1;
}

// 解析为表
static int bson_decode_table(lua_State *L, struct BSON_T *b) {
  bson_assert(L, b, 5);
  lua_createtable(L, 0, 16);
  // 如果是个空表
  if (b->len == 5 && b->ptr[0] == 0x05 && b->ptr[4] == 0x00)
    return 1;
  b->len -= 4; b->ptr += 4;
  const char *key, *value;
  size_t key_len, value_len;
  while (1) {
    uint8_t type = read_uint8(L, b);
    switch (type) {
      case BSON_OBJECTID:
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        value = read_objectid(L, b, &value_len);
        char oid[25];
        lua_pushlstring(L, hex_objectid(oid, value), 24);
        break;
      case BSON_NULL:
      case BSON_UNDEFINED:
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        lua_pushlightuserdata(L, NULL);
        break;
      case BSON_BOOLEAN:
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        read_uint8(L, b) == 0x01 ? lua_pushboolean(L, 1) : lua_pushboolean(L, 0);
        break;
      case BSON_MINKEY:
      case BSON_MAXKEY:
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        lua_pushlstring(L, (const char *)(type == BSON_MINKEY ? "\xff" : "\x7f"), 1);
        break;
      case BSON_INT32:
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        lua_pushinteger(L, read_int32(L, b));
        break;
      case BSON_INT64:
      case BSON_DATETIME:
      case BSON_TIMESTAMP:
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        lua_pushinteger(L, read_int64(L, b));
        break;
      case BSON_DOUBLE :
      case BSON_DECIMAL :
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        lua_pushnumber(L, read_double(L, b));
        break;
      case BSON_REGEX:
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        size_t vlen = 0 ;
        value = (const char *)read_cstring(L, b, &value_len);
        vlen += value_len;
        read_cstring(L, b, &value_len);
        if (value_len > 0)
          vlen += value_len;
        lua_pushfstring(L, "/%s/%s", value , value_len == 0 ? "" : "i");
        (void)vlen;
        break;
      case BSON_BINARY :
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        value_len = read_int32(L, b);
        type = read_uint8(L, b);
        if (type == 0x03 || type == 0x04) {
          char uuid[37];
          lua_pushlstring(L, hex_spec_uuid(uuid, (const char *)b->ptr), 36);
        } else if (type == 0x05) {
          char md5[33];
          lua_pushlstring(L, hex_spec_md5(md5, (const char *)b->ptr), 32);
        } else {
          lua_pushlstring(L, (const char *)b->ptr, value_len);
        }
        b->len -= value_len;
        b->ptr += value_len;
        break;
      case BSON_STRING :
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        value_len = read_int32(L, b);
        value = (const char *)read_cstring(L, b, &value_len);
        lua_pushlstring(L, value, value_len);
        break;
      case BSON_ARRAY :
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        bson_decode_array(L, b);
        break;
      case BSON_TABLE :
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        bson_decode_table(L, b);
        break;
      case BSON_JSCODE2:
        key = (const char *)read_cstring(L, b, &key_len);
        lua_pushlstring(L, key, key_len);
        value_len = read_int32(L, b);
        value = (const char *)read_cstring(L, b, &value_len);
        lua_pushlstring(L, value, value_len);
        break;
      case '\x00':
        return 1;
      default:
        luaL_error(L, "[BSON ERROR] : Unsupported field type:[%p], pos:[%d].", b->ptr[0], b->len);
    }
    lua_rawset(L, -3);
  }
  return 1;
}

static int ldecode(lua_State *L){
  struct BSON_T B;
  B.ptr = (const uint8_t *)luaL_checklstring(L, 1, (size_t*)&B.len);
  if (!B.ptr || B.len < 5)
    return luaL_error(L, "[BSON ERROR] : Invalid BSON string buffer.");
  lua_settop(L, 1);
  return bson_decode_table(L, &B);
}

LUAMOD_API int luaopen_lbson(lua_State *L){
  luaL_checkversion(L);
  luaL_Reg bson_libs[] = {
    {"decode", ldecode},
    {NULL, NULL}
  };
  luaL_newlib(L, bson_libs);
  lua_pushliteral(L, "__VERSION__");
  lua_pushnumber(L, 0.1);
  lua_rawset(L, -3);
  return 1;
}
