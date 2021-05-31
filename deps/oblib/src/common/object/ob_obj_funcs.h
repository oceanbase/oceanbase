/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_COMMON_OB_OBJ_FUNCS_
#define OCEANBASE_COMMON_OB_OBJ_FUNCS_

#include "lib/timezone/ob_timezone_info.h"
#include "lib/timezone/ob_time_convert.h"
#include "common/object/ob_object.h"

namespace oceanbase {
namespace common {

#define PRINT_META()
//#define PRINT_META() BUF_PRINTO(obj.get_meta()); J_COLON();
//
////////////////////////////////////////////////////////////////
// define the following functions for each ObObjType
struct ObObjTypeFuncs {
  /// print as SQL literal style
  ob_obj_print print_sql;
  /// print as SQL varchar literal, used to compose SQL statement
  // used for print default value in show create table, always with ''
  // for type binary, will make up \0 at the end
  // eg. binary(6) default '123' -> show create table will print '123\0\0\0'
  ob_obj_print print_str;
  /// print as plain string, used to transport session vairable
  /// used for compose sql in inner sql, eg. insert into xxx values (0, 'abc');
  ob_obj_print print_plain_str;
  /// print as JSON style
  ob_obj_print print_json;
  ob_obj_crc64 crc64;
  ob_obj_crc64 crc64_v2;
  ob_obj_batch_checksum batch_checksum;
  ob_obj_hash murmurhash;
  ob_obj_hash murmurhash_v2;
  ob_obj_value_serialize serialize;
  ob_obj_value_deserialize deserialize;
  ob_obj_value_get_serialize_size get_serialize_size;
  ob_obj_hash wyhash;
  ob_obj_crc64_v3 crc64_v3;
  ob_obj_hash xxhash64;
  ob_obj_hash murmurhash_v3;
};

// function templates for the above functions
template <ObObjType type>
inline int obj_print_sql(const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params);
template <ObObjType type>
inline int obj_print_str(const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params);
template <ObObjType type>
inline int obj_print_plain_str(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params);
template <ObObjType type>
inline int obj_print_json(const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params);
template <ObObjType type>
inline int64_t obj_crc64(const ObObj& obj, const int64_t current);
template <ObObjType type>
inline int64_t obj_crc64_v2(const ObObj& obj, const int64_t current);
template <ObObjType type>
inline void obj_batch_checksum(const ObObj& obj, ObBatchChecksum& bc);
template <ObObjType type>
inline uint64_t obj_murmurhash(const ObObj& obj, const uint64_t hash);
template <ObObjType type>
inline int obj_val_serialize(const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos);
template <ObObjType type>
inline int obj_val_deserialize(ObObj& obj, const char* buf, const int64_t data_len, int64_t& pos);
template <ObObjType type>
inline int64_t obj_val_get_serialize_size(const ObObj& obj);
template <ObObjType type>
inline uint64_t obj_crc64_v3(const ObObj& obj, const uint64_t hash);
////////////////////////////////////////////////////////////////
// ObNullType = 0,
template <>
inline int obj_print_sql<ObNullType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(obj);
  UNUSED(params);
  int ret = OB_SUCCESS;
  ret = databuff_printf(buffer, length, pos, "NULL");
  return ret;
}
template <>
inline int obj_print_str<ObNullType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(obj);
  UNUSED(params);
  int ret = OB_SUCCESS;
  ret = databuff_printf(buffer, length, pos, "NULL");
  return ret;
}
template <>
inline int obj_print_plain_str<ObNullType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(obj);
  UNUSED(params);
  int ret = OB_SUCCESS;
  ret = databuff_printf(buffer, length, pos, "NULL");
  return ret;
}

template <>
inline int obj_print_json<ObNullType>(
    const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(params);
  J_OBJ_START();
  BUF_PRINTO(ob_obj_type_str(obj.get_type()));
  J_COLON();
  BUF_PRINTF("\"NULL\"");
  J_OBJ_END();
  return OB_SUCCESS;
}
template <>
inline int64_t obj_crc64<ObNullType>(const ObObj& obj, const int64_t current)
{
  int type = obj.get_type();
  return ob_crc64_sse42(current, &type, sizeof(type));
}
template <>
inline int64_t obj_crc64_v2<ObNullType>(const ObObj& obj, const int64_t current)
{
  UNUSED(obj);
  return current;
}
template <>
inline uint64_t obj_crc64_v3<ObNullType>(const ObObj& obj, const uint64_t current)
{
  UNUSED(obj);
  return current;
}
template <>
inline void obj_batch_checksum<ObNullType>(const ObObj& obj, ObBatchChecksum& bc)
{
  int type = obj.get_type();
  bc.fill(&type, sizeof(type));
}
template <>
inline uint64_t obj_murmurhash<ObNullType>(const ObObj& obj, const uint64_t hash)
{
  int type = obj.get_type();
  return murmurhash(&type, sizeof(type), hash);
}
template <typename T, typename P>
struct ObjHashCalculator<ObNullType, T, P> {
  static uint64_t calc_hash_value(const P& param, const uint64_t hash)
  {
    UNUSED(param);
    int type = ObNullType;
    uint64_t ret = T::hash(&type, sizeof(type), hash);
    return ret;
  }
};
template <>
inline int obj_val_serialize<ObNullType>(const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos)
{
  UNUSED(obj);
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  return OB_SUCCESS;
}
template <>
inline int obj_val_deserialize<ObNullType>(ObObj& obj, const char* buf, const int64_t data_len, int64_t& pos)
{
  UNUSED(obj);
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  return OB_SUCCESS;
}

template <>
inline int64_t obj_val_get_serialize_size<ObNullType>(const ObObj& obj)
{
  UNUSED(obj);
  return 0;
}
////////////////
// general checksum functions generator
#define DEF_CS_FUNCS(OBJTYPE, TYPE, VTYPE, HTYPE)                                 \
  template <>                                                                     \
  inline int64_t obj_crc64<OBJTYPE>(const ObObj& obj, const int64_t current)      \
  {                                                                               \
    int type = obj.get_type();                                                    \
    int64_t ret = ob_crc64_sse42(current, &type, sizeof(type));                   \
    VTYPE v = obj.get_##TYPE();                                                   \
    return ob_crc64_sse42(ret, &v, sizeof(obj.get_##TYPE()));                     \
  }                                                                               \
                                                                                  \
  template <>                                                                     \
  inline int64_t obj_crc64_v2<OBJTYPE>(const ObObj& obj, const int64_t current)   \
  {                                                                               \
    VTYPE v = obj.get_##TYPE();                                                   \
    return ob_crc64_sse42(current, &v, sizeof(obj.get_##TYPE()));                 \
  }                                                                               \
                                                                                  \
  template <>                                                                     \
  inline void obj_batch_checksum<OBJTYPE>(const ObObj& obj, ObBatchChecksum& bc)  \
  {                                                                               \
    int type = obj.get_type();                                                    \
    bc.fill(&type, sizeof(type));                                                 \
    VTYPE v = obj.get_##TYPE();                                                   \
    bc.fill(&v, sizeof(obj.get_##TYPE()));                                        \
  }                                                                               \
                                                                                  \
  template <>                                                                     \
  inline uint64_t obj_murmurhash<OBJTYPE>(const ObObj& obj, const uint64_t hash)  \
  {                                                                               \
    int type = obj.get_type();                                                    \
    uint64_t ret = murmurhash(&type, sizeof(type), hash);                         \
    VTYPE v = obj.get_##TYPE();                                                   \
    if (ob_is_real_type(static_cast<ObObjType>(type)) && 0.0 == v) {              \
      v = 0.0;                                                                    \
    }                                                                             \
    return murmurhash(&v, sizeof(obj.get_##TYPE()), ret);                         \
  }                                                                               \
  template <typename T, typename P>                                               \
  struct ObjHashCalculator<OBJTYPE, T, P> {                                       \
    static uint64_t calc_hash_value(const P& param, const uint64_t hash)          \
    {                                                                             \
      VTYPE v = param.get_##TYPE();                                               \
      HTYPE v2 = v;                                                               \
      if (ob_is_real_type(OBJTYPE) && 0.0 == v2) {                                \
        v2 = 0.0;                                                                 \
      }                                                                           \
      return T::hash(&v2, sizeof(v2), hash);                                      \
    }                                                                             \
  };                                                                              \
  template <>                                                                     \
  inline uint64_t obj_crc64_v3<OBJTYPE>(const ObObj& obj, const uint64_t current) \
  {                                                                               \
    VTYPE v = obj.get_##TYPE();                                                   \
    return ob_crc64_sse42(current, &v, sizeof(obj.get_##TYPE()));                 \
  }                                                                               \
// general print functions generator
#define DEF_PRINT_FUNCS(OBJTYPE, TYPE, SQL_FORMAT, STR_FORMAT)                                          \
  template <>                                                                                           \
  inline int obj_print_sql<OBJTYPE>(                                                                    \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    UNUSED(params);                                                                                     \
    return databuff_printf(buffer, length, pos, SQL_FORMAT, obj.get_##TYPE());                          \
  }                                                                                                     \
                                                                                                        \
  template <>                                                                                           \
  inline int obj_print_str<OBJTYPE>(                                                                    \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    UNUSED(params);                                                                                     \
    return databuff_printf(buffer, length, pos, STR_FORMAT, obj.get_##TYPE());                          \
  }                                                                                                     \
                                                                                                        \
  template <>                                                                                           \
  inline int obj_print_plain_str<OBJTYPE>(                                                              \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    UNUSED(params);                                                                                     \
    return databuff_printf(buffer, length, pos, SQL_FORMAT, obj.get_##TYPE());                          \
  }                                                                                                     \
                                                                                                        \
  template <>                                                                                           \
  inline int obj_print_json<OBJTYPE>(                                                                   \
      const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                     \
    UNUSED(params);                                                                                     \
    J_OBJ_START();                                                                                      \
    PRINT_META();                                                                                       \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                        \
    J_COLON();                                                                                          \
    BUF_PRINTO(obj.get_##TYPE());                                                                       \
    J_OBJ_END();                                                                                        \
    return OB_SUCCESS;                                                                                  \
  }

// general print functions generator
#define DEF_DOUBLE_FLOAT_PRINT_FUNCS(OBJTYPE, TYPE, IS_DOUBLE)                                                        \
  template <>                                                                                                         \
  inline int obj_print_sql<OBJTYPE>(                                                                                  \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)                   \
  {                                                                                                                   \
    int ret = OB_SUCCESS;                                                                                             \
    UNUSED(params);                                                                                                   \
    int64_t print_len = 0;                                                                                            \
    if (IS_DOUBLE) {                                                                                                  \
      print_len =                                                                                                     \
          ob_gcvt_opt(obj.get_double(), OB_GCVT_ARG_DOUBLE, length - pos, buffer + pos, NULL, lib::is_oracle_mode()); \
    } else {                                                                                                          \
      print_len =                                                                                                     \
          ob_gcvt_opt(obj.get_float(), OB_GCVT_ARG_FLOAT, length - pos, buffer + pos, NULL, lib::is_oracle_mode());   \
    }                                                                                                                 \
    if (OB_UNLIKELY(print_len <= 0 || print_len + pos > length)) {                                                    \
      ret = OB_SIZE_OVERFLOW;                                                                                         \
    } else {                                                                                                          \
      pos += print_len;                                                                                               \
      if (lib::is_oracle_mode()) {                                                                                    \
        if (OB_UNLIKELY(pos + 1 > length)) {                                                                          \
          ret = OB_SIZE_OVERFLOW;                                                                                     \
        } else {                                                                                                      \
          buffer[pos++] = (IS_DOUBLE ? 'D' : 'F');                                                                    \
          buffer[pos] = '\0';                                                                                         \
        }                                                                                                             \
      }                                                                                                               \
    }                                                                                                                 \
    return ret;                                                                                                       \
  }                                                                                                                   \
                                                                                                                      \
  template <>                                                                                                         \
  inline int obj_print_str<OBJTYPE>(                                                                                  \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)                   \
  {                                                                                                                   \
    int ret = OB_SUCCESS;                                                                                             \
    UNUSED(params);                                                                                                   \
    int64_t print_len = 0;                                                                                            \
    if (OB_UNLIKELY(length - pos < 3)) {                                                                              \
      ret = OB_SIZE_OVERFLOW;                                                                                         \
    } else {                                                                                                          \
      buffer[pos++] = '\'';                                                                                           \
      if (IS_DOUBLE) {                                                                                                \
        print_len = ob_gcvt_opt(                                                                                      \
            obj.get_double(), OB_GCVT_ARG_DOUBLE, length - pos, buffer + pos, NULL, lib::is_oracle_mode());           \
      } else {                                                                                                        \
        print_len =                                                                                                   \
            ob_gcvt_opt(obj.get_float(), OB_GCVT_ARG_FLOAT, length - pos, buffer + pos, NULL, lib::is_oracle_mode()); \
      }                                                                                                               \
      if (OB_UNLIKELY(print_len <= 0 || print_len + pos > length)) {                                                  \
        ret = OB_SIZE_OVERFLOW;                                                                                       \
      } else {                                                                                                        \
        pos += print_len;                                                                                             \
        if (lib::is_oracle_mode()) {                                                                                  \
          if (OB_UNLIKELY(pos + 3 > length)) {                                                                        \
            ret = OB_SIZE_OVERFLOW;                                                                                   \
          } else {                                                                                                    \
            buffer[pos++] = (IS_DOUBLE ? 'D' : 'F');                                                                  \
            buffer[pos++] = '\'';                                                                                     \
            buffer[pos] = '\0';                                                                                       \
          }                                                                                                           \
        } else {                                                                                                      \
          if (OB_UNLIKELY(pos + 2 > length)) {                                                                        \
            ret = OB_SIZE_OVERFLOW;                                                                                   \
          } else {                                                                                                    \
            buffer[pos++] = '\'';                                                                                     \
            buffer[pos] = '\0';                                                                                       \
          }                                                                                                           \
        }                                                                                                             \
      }                                                                                                               \
    }                                                                                                                 \
    return ret;                                                                                                       \
  }                                                                                                                   \
                                                                                                                      \
  template <>                                                                                                         \
  inline int obj_print_plain_str<OBJTYPE>(                                                                            \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)                   \
  {                                                                                                                   \
    int ret = OB_SUCCESS;                                                                                             \
    UNUSED(params);                                                                                                   \
    int64_t print_len = 0;                                                                                            \
    if (IS_DOUBLE) {                                                                                                  \
      print_len =                                                                                                     \
          ob_gcvt_opt(obj.get_double(), OB_GCVT_ARG_DOUBLE, length - pos, buffer + pos, NULL, lib::is_oracle_mode()); \
    } else {                                                                                                          \
      print_len =                                                                                                     \
          ob_gcvt_opt(obj.get_float(), OB_GCVT_ARG_FLOAT, length - pos, buffer + pos, NULL, lib::is_oracle_mode());   \
    }                                                                                                                 \
    if (OB_UNLIKELY(print_len <= 0 || print_len + pos > length)) {                                                    \
      ret = OB_SIZE_OVERFLOW;                                                                                         \
    } else {                                                                                                          \
      pos += print_len;                                                                                               \
    }                                                                                                                 \
    return ret;                                                                                                       \
  }                                                                                                                   \
                                                                                                                      \
  template <>                                                                                                         \
  inline int obj_print_json<OBJTYPE>(                                                                                 \
      const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)               \
  {                                                                                                                   \
    UNUSED(params);                                                                                                   \
    J_OBJ_START();                                                                                                    \
    PRINT_META();                                                                                                     \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                                      \
    J_COLON();                                                                                                        \
    BUF_PRINTO(obj.get_##TYPE());                                                                                     \
    J_OBJ_END();                                                                                                      \
    return OB_SUCCESS;                                                                                                \
  }

// general serialization functions generator
#define DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)                                                             \
  template <>                                                                                                 \
  inline int obj_val_serialize<OBJTYPE>(const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos)     \
  {                                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    OB_UNIS_ENCODE(obj.get_##TYPE());                                                                         \
    return ret;                                                                                               \
  }                                                                                                           \
                                                                                                              \
  template <>                                                                                                 \
  inline int obj_val_deserialize<OBJTYPE>(ObObj & obj, const char* buf, const int64_t data_len, int64_t& pos) \
  {                                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    VTYPE v = VTYPE();                                                                                        \
    OB_UNIS_DECODE(v);                                                                                        \
    if (OB_SUCC(ret)) {                                                                                       \
      obj.set_obj_value(v);                                                                                   \
    }                                                                                                         \
    return ret;                                                                                               \
  }                                                                                                           \
                                                                                                              \
  template <>                                                                                                 \
  inline int64_t obj_val_get_serialize_size<OBJTYPE>(const ObObj& obj)                                        \
  {                                                                                                           \
    int64_t len = 0;                                                                                          \
    OB_UNIS_ADD_LEN(obj.get_##TYPE());                                                                        \
    return len;                                                                                               \
  }

// general generator for numeric types
#define DEF_NUMERIC_FUNCS(OBJTYPE, TYPE, VTYPE, SQL_FORMAT, STR_FORMAT, HTYPE) \
  DEF_CS_FUNCS(OBJTYPE, TYPE, VTYPE, HTYPE);                                   \
  DEF_PRINT_FUNCS(OBJTYPE, TYPE, SQL_FORMAT, STR_FORMAT);                      \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

// general generator for numeric types
#define DEF_DOUBLE_FUNCS(OBJTYPE, TYPE, VTYPE, HTYPE) \
  DEF_CS_FUNCS(OBJTYPE, TYPE, VTYPE, HTYPE);          \
  DEF_DOUBLE_FLOAT_PRINT_FUNCS(OBJTYPE, VTYPE, true); \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

// general generator for numeric types
#define DEF_FLOAT_FUNCS(OBJTYPE, TYPE, VTYPE, HTYPE)   \
  DEF_CS_FUNCS(OBJTYPE, TYPE, VTYPE, HTYPE);           \
  DEF_DOUBLE_FLOAT_PRINT_FUNCS(OBJTYPE, VTYPE, false); \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

// ObTinyIntType=1,                // int8, aka mysql boolean type
DEF_NUMERIC_FUNCS(ObTinyIntType, tinyint, int8_t, "%hhd", "'%hhd'", int64_t);
// ObSmallIntType=2,               // int16
DEF_NUMERIC_FUNCS(ObSmallIntType, smallint, int16_t, "%hd", "'%hd'", int64_t);
// ObMediumIntType=3,              // int24
DEF_NUMERIC_FUNCS(ObMediumIntType, mediumint, int32_t, "%d", "'%d'", int64_t);
// ObInt32Type=4,                 // int32
DEF_NUMERIC_FUNCS(ObInt32Type, int32, int32_t, "%d", "'%d'", int64_t);
// ObIntType=5,                    // int64, aka bigint
DEF_NUMERIC_FUNCS(ObIntType, int, int64_t, "%ld", "'%ld'", int64_t);
// ObUTinyIntType=6,                // uint8
DEF_NUMERIC_FUNCS(ObUTinyIntType, utinyint, uint8_t, "%hhu", "'%hhu'", uint64_t);
// ObUSmallIntType=7,               // uint16
DEF_NUMERIC_FUNCS(ObUSmallIntType, usmallint, uint16_t, "%hu", "'%hu'", uint64_t);
// ObUMediumIntType=8,              // uint24
DEF_NUMERIC_FUNCS(ObUMediumIntType, umediumint, uint32_t, "%u", "'%u'", uint64_t);
// ObUInt32Type=9,                    // uint32
DEF_NUMERIC_FUNCS(ObUInt32Type, uint32, uint32_t, "%u", "'%u'", uint64_t);
// ObUInt64Type=10,                 // uint64
DEF_NUMERIC_FUNCS(ObUInt64Type, uint64, uint64_t, "%lu", "'%lu'", uint64_t);
// ObFloatType=11,                  // single-precision floating point
DEF_FLOAT_FUNCS(ObFloatType, float, float, double);
// ObDoubleType=12,                 // double-precision floating point
DEF_DOUBLE_FUNCS(ObDoubleType, double, double, double);
// ObUFloatType=13,            // unsigned single-precision floating point
DEF_FLOAT_FUNCS(ObUFloatType, ufloat, float, double);
// ObUDoubleType=14,           // unsigned double-precision floating point
DEF_DOUBLE_FUNCS(ObUDoubleType, udouble, double, double);

#define DEF_BIT_PRINT_FUNCS                                                                             \
  template <>                                                                                           \
  inline int obj_print_sql<ObBitType>(                                                                  \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    UNUSED(params);                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    if (OB_FAIL(databuff_printf(buffer, length, pos, "b'"))) {                                          \
      _OB_LOG(WARN, "fail to print, ret = %d", ret);                                                    \
    } else if (OB_FAIL(bit_print(obj.get_bit(), buffer, length, pos))) {                                \
      _OB_LOG(WARN, "fail to print, ret = %d", ret);                                                    \
    } else if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {                                    \
      _OB_LOG(WARN, "fail to print, ret = %d", ret);                                                    \
    }                                                                                                   \
    return ret;                                                                                         \
  }                                                                                                     \
  template <>                                                                                           \
  inline int obj_print_str<ObBitType>(                                                                  \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    if (OB_FAIL(obj_print_sql<ObBitType>(obj, buffer, length, pos, params))) {                          \
      _OB_LOG(WARN, "fail to print, ret = %d", ret);                                                    \
    }                                                                                                   \
    return ret;                                                                                         \
  }                                                                                                     \
  template <>                                                                                           \
  inline int obj_print_plain_str<ObBitType>(                                                            \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    UNUSED(params);                                                                                     \
    return databuff_printf(buffer, length, pos, "%lu", obj.get_bit());                                  \
  }                                                                                                     \
  template <>                                                                                           \
  inline int obj_print_json<ObBitType>(                                                                 \
      const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                     \
    UNUSED(params);                                                                                     \
    J_OBJ_START();                                                                                      \
    PRINT_META();                                                                                       \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                        \
    J_COLON();                                                                                          \
    BUF_PRINTO(obj.get_bit());                                                                          \
    J_OBJ_END();                                                                                        \
    return OB_SUCCESS;                                                                                  \
  }

#define DEF_BIT_FUNCS(OBJTYPE, TYPE, VTYPE)  \
  DEF_BIT_PRINT_FUNCS;                       \
  DEF_CS_FUNCS(OBJTYPE, TYPE, VTYPE, VTYPE); \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

DEF_BIT_FUNCS(ObBitType, bit, uint64_t);

////////////////////
// ObEnumType=32
// ObSetType=33
DEF_NUMERIC_FUNCS(ObEnumType, enum, uint64_t, "%lu", "'%lu'", uint64_t);
DEF_NUMERIC_FUNCS(ObSetType, set, uint64_t, "%lu", "'%lu'", uint64_t);

////////////////
// ObNumberType=15, // aka decimal/numeric
// ObUNumberType=16,
#define DEF_NUMBER_PRINT_FUNCS(OBJTYPE, TYPE)                                                       \
  template <>                                                                                       \
  inline int obj_print_sql<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    number::ObNumber nmb;                                                                           \
    if (OB_FAIL(obj.get_##TYPE(nmb))) {                                                             \
    } else if (OB_FAIL(nmb.format(buffer, length, pos, obj.get_scale()))) {                         \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_str<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    UNUSED(params);                                                                                 \
    number::ObNumber nmb;                                                                           \
    if (OB_UNLIKELY(length - pos < number::ObNumber::MAX_APPEND_LEN)) {                             \
      ret = OB_SIZE_OVERFLOW;                                                                       \
    } else if (OB_FAIL(obj.get_##TYPE(nmb))) {                                                      \
    } else {                                                                                        \
      buffer[pos++] = '\'';                                                                         \
      if (OB_FAIL(nmb.format(buffer, length - 3, pos, obj.get_scale()))) {                          \
      } else if (OB_UNLIKELY(pos + 1 > length)) {                                                   \
        ret = OB_SIZE_OVERFLOW;                                                                     \
      } else {                                                                                      \
        buffer[pos++] = '\'';                                                                       \
        buffer[pos] = '\0';                                                                         \
      }                                                                                             \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_plain_str<OBJTYPE>(                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    UNUSED(params);                                                                                 \
    number::ObNumber nmb;                                                                           \
    if (OB_FAIL(obj.get_##TYPE(nmb))) {                                                             \
    } else if (OB_FAIL(nmb.format(buffer, length - 1, pos, obj.get_scale()))) {                     \
    } else {                                                                                        \
      buffer[pos] = '\0';                                                                           \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_json<OBJTYPE>(                                                               \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)   \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    number::ObNumber nmb;                                                                           \
    if (OB_FAIL(obj.get_##TYPE(nmb))) {                                                             \
    } else {                                                                                        \
      J_OBJ_START();                                                                                \
      PRINT_META();                                                                                 \
      BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                  \
      J_COLON();                                                                                    \
      BUF_PRINTO(nmb.format());                                                                     \
      J_OBJ_END();                                                                                  \
    }                                                                                               \
    return ret;                                                                                     \
  }

#define DEF_NUMBER_CS_FUNCS(OBJTYPE)                                                                               \
  template <>                                                                                                      \
  inline int64_t obj_crc64<OBJTYPE>(const ObObj& obj, const int64_t current)                                       \
  {                                                                                                                \
    int type = obj.get_type();                                                                                     \
    int64_t ret = ob_crc64_sse42(current, &type, sizeof(type));                                                    \
    uint8_t se = obj.get_number_desc().se_;                                                                        \
    ret = ob_crc64_sse42(ret, &se, 1);                                                                             \
    return ob_crc64_sse42(ret, obj.get_number_digits(), sizeof(uint32_t) * obj.get_number_desc().len_);            \
  }                                                                                                                \
  template <>                                                                                                      \
  inline int64_t obj_crc64_v2<OBJTYPE>(const ObObj& obj, const int64_t current)                                    \
  {                                                                                                                \
    uint8_t se = obj.get_number_desc().se_;                                                                        \
    int64_t ret = ob_crc64_sse42(current, &se, 1);                                                                 \
    return ob_crc64_sse42(ret, obj.get_number_digits(), sizeof(uint32_t) * obj.get_number_desc().len_);            \
  }                                                                                                                \
  template <>                                                                                                      \
  inline void obj_batch_checksum<OBJTYPE>(const ObObj& obj, ObBatchChecksum& bc)                                   \
  {                                                                                                                \
    int type = obj.get_type();                                                                                     \
    bc.fill(&type, sizeof(type));                                                                                  \
    bc.fill(&obj.get_number_desc().se_, 1);                                                                        \
    bc.fill(obj.get_number_digits(), sizeof(uint32_t) * obj.get_number_desc().len_);                               \
  }                                                                                                                \
  template <>                                                                                                      \
  inline uint64_t obj_murmurhash<OBJTYPE>(const ObObj& obj, const uint64_t hash)                                   \
  {                                                                                                                \
    int type = obj.get_type();                                                                                     \
    uint64_t ret = murmurhash(&type, sizeof(type), hash);                                                          \
    ret = murmurhash(&obj.get_number_desc().se_, 1, ret);                                                          \
    return murmurhash(                                                                                             \
        obj.get_number_digits(), static_cast<int32_t>(sizeof(uint32_t) * obj.get_number_desc().len_), ret);        \
  }                                                                                                                \
  template <typename T, typename P>                                                                                \
  struct ObjHashCalculator<OBJTYPE, T, P> {                                                                        \
    static uint64_t calc_hash_value(const P& param, const uint64_t hash)                                           \
    {                                                                                                              \
      uint64_t ret = T::hash(&param.get_number_desc().se_, 1, hash);                                               \
      return T::hash(                                                                                              \
          param.get_number_digits(), static_cast<uint64_t>(sizeof(uint32_t) * param.get_number_desc().len_), ret); \
    }                                                                                                              \
  };                                                                                                               \
  template <>                                                                                                      \
  inline uint64_t obj_crc64_v3<OBJTYPE>(const ObObj& obj, const uint64_t current)                                  \
  {                                                                                                                \
    uint8_t se = obj.get_number_desc().se_;                                                                        \
    uint64_t ret = ob_crc64_sse42(current, &se, 1);                                                                \
    return ob_crc64_sse42(ret, obj.get_number_digits(), sizeof(uint32_t) * obj.get_number_desc().len_);            \
  }
#define DEF_NUMBER_SERIALIZE_FUNCS(OBJTYPE, TYPE)                                                             \
  template <>                                                                                                 \
  inline int obj_val_serialize<OBJTYPE>(const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos)     \
  {                                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    number::ObNumber::Desc tmp_desc = obj.get_number_desc();                                                  \
    tmp_desc.cap_ = 0;                                                                                        \
    tmp_desc.flag_ = 0;                                                                                       \
    ret = serialization::encode_number_type(buf, buf_len, pos, tmp_desc, obj.get_number_digits());            \
    return ret;                                                                                               \
  }                                                                                                           \
                                                                                                              \
  template <>                                                                                                 \
  inline int obj_val_deserialize<OBJTYPE>(ObObj & obj, const char* buf, const int64_t data_len, int64_t& pos) \
  {                                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    number::ObNumber::Desc tmp_desc;                                                                          \
    uint32_t* nmb_digits = NULL;                                                                              \
    ret = serialization::decode_number_type(buf, data_len, pos, tmp_desc, nmb_digits);                        \
    if (OB_SUCC(ret)) {                                                                                       \
      tmp_desc.cap_ = tmp_desc.len_; /* capacity equals to length*/                                           \
      obj.set_##TYPE##_value(tmp_desc, nmb_digits);                                                           \
    }                                                                                                         \
    return ret;                                                                                               \
  }                                                                                                           \
                                                                                                              \
  template <>                                                                                                 \
  inline int64_t obj_val_get_serialize_size<OBJTYPE>(const ObObj& obj)                                        \
  {                                                                                                           \
    return serialization::encode_length_number_type(obj.get_number_desc());                                   \
  }

#define DEF_NUMBER_FUNCS(OBJTYPE, TYPE)  \
  DEF_NUMBER_PRINT_FUNCS(OBJTYPE, TYPE); \
  DEF_NUMBER_CS_FUNCS(OBJTYPE);          \
  DEF_NUMBER_SERIALIZE_FUNCS(OBJTYPE, TYPE)

DEF_NUMBER_FUNCS(ObNumberType, number);
DEF_NUMBER_FUNCS(ObUNumberType, unumber);
DEF_NUMBER_FUNCS(ObNumberFloatType, number_float);

////////////////
#define DEF_DATETIME_PRINT_FUNCS(OBJTYPE, TYPE)                                                     \
  template <>                                                                                       \
  inline int obj_print_sql<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    const ObTimeZoneInfo* tz_info = params.tz_info_;                                                \
    ObString str(static_cast<int32_t>(length - pos - 1), 0, buffer + pos + 1);                      \
    int ret = databuff_printf(buffer, length, pos, "'");                                            \
    if (OB_SUCC(ret)) {                                                                             \
      if (ObTimestampType != obj.get_type()) {                                                      \
        tz_info = NULL;                                                                             \
      }                                                                                             \
      const ObString nls_format;                                                                    \
      ret = ObTimeConverter::datetime_to_str(                                                       \
          obj.get_datetime(), tz_info, nls_format, obj.get_scale(), buffer, length, pos);           \
    }                                                                                               \
    if (OB_SUCC(ret)) {                                                                             \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
                                                                                                    \
  template <>                                                                                       \
  inline int obj_print_str<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    const ObTimeZoneInfo* tz_info = params.tz_info_;                                                \
    ObString str(static_cast<int32_t>(length - pos - 1), 0, buffer + pos + 1);                      \
    int ret = databuff_printf(buffer, length, pos, "'");                                            \
    if (OB_SUCC(ret)) {                                                                             \
      if (ObTimestampType != obj.get_type()) {                                                      \
        tz_info = NULL;                                                                             \
      }                                                                                             \
      const ObString nls_format;                                                                    \
      ret = ObTimeConverter::datetime_to_str(                                                       \
          obj.get_datetime(), tz_info, nls_format, obj.get_scale(), buffer, length, pos);           \
    }                                                                                               \
    if (OB_SUCC(ret)) {                                                                             \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
                                                                                                    \
  template <>                                                                                       \
  inline int obj_print_plain_str<OBJTYPE>(                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    const ObTimeZoneInfo* tz_info = params.tz_info_;                                                \
    if (ObTimestampType != obj.get_type()) {                                                        \
      tz_info = NULL;                                                                               \
    }                                                                                               \
    const ObString nls_format;                                                                      \
    return ObTimeConverter::datetime_to_str(obj.get_datetime(),                                     \
        tz_info,                                                                                    \
        nls_format,                                                                                 \
        (lib::is_oracle_mode() ? OB_MAX_DATE_PRECISION : OB_MAX_DATETIME_PRECISION),                \
        buffer,                                                                                     \
        length,                                                                                     \
        pos);                                                                                       \
  }                                                                                                 \
                                                                                                    \
  template <>                                                                                       \
  inline int obj_print_json<OBJTYPE>(                                                               \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)   \
  {                                                                                                 \
    const ObTimeZoneInfo* tz_info = params.tz_info_;                                                \
    J_OBJ_START();                                                                                  \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                    \
    J_COLON();                                                                                      \
    J_QUOTE();                                                                                      \
    if (ObTimestampType != obj.get_type()) {                                                        \
      tz_info = NULL;                                                                               \
    }                                                                                               \
    const ObString nls_format;                                                                      \
    int ret = ObTimeConverter::datetime_to_str(obj.get_datetime(),                                  \
        tz_info,                                                                                    \
        nls_format,                                                                                 \
        (lib::is_oracle_mode() ? OB_MAX_DATE_PRECISION : OB_MAX_DATETIME_PRECISION),                \
        buf,                                                                                        \
        buf_len,                                                                                    \
        pos);                                                                                       \
    if (OB_SUCC(ret)) {                                                                             \
      J_QUOTE();                                                                                    \
      J_OBJ_END();                                                                                  \
    }                                                                                               \
    return ret;                                                                                     \
  }

#define DEF_DATETIME_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_CS_FUNCS(OBJTYPE, TYPE, VTYPE, int64_t);   \
  DEF_DATETIME_PRINT_FUNCS(OBJTYPE, TYPE);       \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

// ObDateTimeType=17,
DEF_DATETIME_FUNCS(ObDateTimeType, datetime, int64_t);
// ObTimestampType=18,
DEF_DATETIME_FUNCS(ObTimestampType, timestamp, int64_t);

////////////////
#define DEF_DATE_YEAR_PRINT_FUNCS(OBJTYPE, TYPE)                                                    \
  template <>                                                                                       \
  inline int obj_print_sql<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = databuff_printf(buffer, length, pos, "'");                                            \
    if (OB_SUCC(ret)) {                                                                             \
      ret = ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(), buffer, length, pos);                  \
    }                                                                                               \
    if (OB_SUCC(ret)) {                                                                             \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
                                                                                                    \
  template <>                                                                                       \
  inline int obj_print_str<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = databuff_printf(buffer, length, pos, "'");                                            \
    if (OB_SUCC(ret)) {                                                                             \
      ret = ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(), buffer, length, pos);                  \
    }                                                                                               \
    if (OB_SUCC(ret)) {                                                                             \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
                                                                                                    \
  template <>                                                                                       \
  inline int obj_print_plain_str<OBJTYPE>(                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    return ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(), buffer, length, pos);                   \
  }                                                                                                 \
                                                                                                    \
  template <>                                                                                       \
  inline int obj_print_json<OBJTYPE>(                                                               \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)   \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    J_OBJ_START();                                                                                  \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                    \
    J_COLON();                                                                                      \
    J_QUOTE();                                                                                      \
    int ret = ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(), buf, buf_len, pos);                  \
    if (OB_SUCC(ret)) {                                                                             \
      J_QUOTE();                                                                                    \
      J_OBJ_END();                                                                                  \
    }                                                                                               \
    return ret;                                                                                     \
  }

#define DEF_DATE_YEAR_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_CS_FUNCS(OBJTYPE, TYPE, VTYPE, int64_t);    \
  DEF_DATE_YEAR_PRINT_FUNCS(OBJTYPE, TYPE);       \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

// ObDateType=19
DEF_DATE_YEAR_FUNCS(ObDateType, date, int32_t);

////////////////
#define DEF_TIME_PRINT_FUNCS(OBJTYPE, TYPE)                                                         \
  template <>                                                                                       \
  inline int obj_print_sql<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = databuff_printf(buffer, length, pos, "'");                                            \
    if (OB_SUCC(ret)) {                                                                             \
      ret = ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(), obj.get_scale(), buffer, length, pos); \
    }                                                                                               \
    if (OB_SUCC(ret)) {                                                                             \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
                                                                                                    \
  template <>                                                                                       \
  inline int obj_print_str<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = databuff_printf(buffer, length, pos, "'");                                            \
    if (OB_SUCC(ret)) {                                                                             \
      ret = ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(), obj.get_scale(), buffer, length, pos); \
    }                                                                                               \
    if (OB_SUCC(ret)) {                                                                             \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
                                                                                                    \
  template <>                                                                                       \
  inline int obj_print_plain_str<OBJTYPE>(                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    return ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(), obj.get_scale(), buffer, length, pos);  \
  }                                                                                                 \
                                                                                                    \
  template <>                                                                                       \
  inline int obj_print_json<OBJTYPE>(                                                               \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)   \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    J_OBJ_START();                                                                                  \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                    \
    J_COLON();                                                                                      \
    J_QUOTE();                                                                                      \
    int ret = ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(), obj.get_scale(), buf, buf_len, pos); \
    if (OB_SUCC(ret)) {                                                                             \
      J_QUOTE();                                                                                    \
      J_OBJ_END();                                                                                  \
    }                                                                                               \
    return ret;                                                                                     \
  }

#define DEF_TIME_FUNCS(OBJTYPE, TYPE, VTYPE)   \
  DEF_CS_FUNCS(OBJTYPE, TYPE, VTYPE, int64_t); \
  DEF_TIME_PRINT_FUNCS(OBJTYPE, TYPE);         \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

// ObTimeType=20
DEF_TIME_FUNCS(ObTimeType, time, int64_t);

// ObYearType=21
DEF_DATE_YEAR_FUNCS(ObYearType, year, uint8_t);

////////////////
// ObVarcharType=22,  // charset: utf-8, collation: utf8_general_ci
// ObCharType=23,     // charset: utf-8, collation: utf8_general_ci
template <>
inline int obj_print_sql<ObHexStringType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params);
template <>
inline int obj_print_str<ObHexStringType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params);
template <>
inline int obj_print_plain_str<ObHexStringType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params);

#define DEF_VARCHAR_PRINT_FUNCS(OBJTYPE)                                                                 \
  template <>                                                                                            \
  inline int obj_print_sql<OBJTYPE>(                                                                     \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)      \
  {                                                                                                      \
    int ret = OB_SUCCESS;                                                                                \
    ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());                  \
    ObCharsetType dst_type = ObCharset::charset_type_by_coll(params.cs_type_);                           \
    if (CHARSET_BINARY == src_type && lib::is_mysql_mode()) {                                            \
      ret = obj_print_sql<ObHexStringType>(obj, buffer, length, pos, params);                            \
    } else if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {                                     \
    } else if (src_type == dst_type || src_type == CHARSET_INVALID) {                                    \
      ObHexEscapeSqlStr sql_str(obj.get_string());                                                       \
      pos += sql_str.to_string(buffer + pos, length - pos);                                              \
      ret = databuff_printf(buffer, length, pos, "'");                                                   \
    } else {                                                                                             \
      uint32_t result_len = 0;                                                                           \
      if (OB_FAIL(ObCharset::charset_convert(obj.get_collation_type(),                                   \
              obj.get_string_ptr(),                                                                      \
              obj.get_string_len(),                                                                      \
              params.cs_type_,                                                                           \
              buffer + pos,                                                                              \
              length - pos,                                                                              \
              result_len))) {                                                                            \
      } else {                                                                                           \
        ObHexEscapeSqlStr sql_str(ObString(result_len, buffer + pos));                                   \
        int64_t temp_pos = pos + result_len;                                                             \
        int64_t data_len = sql_str.to_string(buffer + temp_pos, length - temp_pos);                      \
        if (OB_UNLIKELY(temp_pos + data_len >= length)) {                                                \
          ret = OB_SIZE_OVERFLOW;                                                                        \
        } else {                                                                                         \
          MEMMOVE(buffer + pos, buffer + temp_pos, data_len);                                            \
          pos += data_len;                                                                               \
          ret = databuff_printf(buffer, length, pos, "'");                                               \
        }                                                                                                \
      }                                                                                                  \
    }                                                                                                    \
    return ret;                                                                                          \
  }                                                                                                      \
  template <>                                                                                            \
  inline int obj_print_str<OBJTYPE>(                                                                     \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)      \
  {                                                                                                      \
    int ret = OB_SUCCESS;                                                                                \
    ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());                  \
    ObCharsetType dst_type = ObCharset::charset_type_by_coll(params.cs_type_);                           \
    if (CHARSET_BINARY == src_type) {                                                                    \
      if (obj.get_string_len() > obj.get_data_length()) {                                                \
      } else {                                                                                           \
        int64_t zero_length = obj.get_data_length() - obj.get_string_len();                              \
        ret = databuff_printf(buffer, length, pos, "'%.*s", obj.get_string_len(), obj.get_string_ptr()); \
        for (int64_t i = 0; OB_SUCC(ret) && i < zero_length; ++i) {                                      \
          ret = databuff_printf(buffer, length, pos, "\\0");                                             \
        }                                                                                                \
        if (OB_SUCC(ret)) {                                                                              \
          if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {}                                     \
        }                                                                                                \
      }                                                                                                  \
    } else if (src_type == dst_type || src_type == CHARSET_INVALID) {                                    \
      ret = databuff_printf(buffer, length, pos, "'%.*s'", obj.get_string_len(), obj.get_string_ptr());  \
    } else {                                                                                             \
      uint32_t result_len = 0;                                                                           \
      if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {                                          \
      } else if (OB_FAIL(ObCharset::charset_convert(obj.get_collation_type(),                            \
                     obj.get_string_ptr(),                                                               \
                     obj.get_string_len(),                                                               \
                     params.cs_type_,                                                                    \
                     buffer + pos,                                                                       \
                     length - pos,                                                                       \
                     result_len))) {                                                                     \
      } else {                                                                                           \
        pos += result_len;                                                                               \
        ret = databuff_printf(buffer, length, pos, "'");                                                 \
      }                                                                                                  \
    }                                                                                                    \
    return ret;                                                                                          \
  }                                                                                                      \
  template <>                                                                                            \
  inline int obj_print_plain_str<OBJTYPE>(                                                               \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)      \
  {                                                                                                      \
    int ret = OB_SUCCESS;                                                                                \
    ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());                  \
    ObCharsetType dst_type = ObCharset::charset_type_by_coll(params.cs_type_);                           \
    if (src_type == CHARSET_BINARY || src_type == dst_type || src_type == CHARSET_INVALID) {             \
      ret = databuff_printf(buffer, length, pos, "%.*s", obj.get_string_len(), obj.get_string_ptr());    \
    } else {                                                                                             \
      uint32_t result_len = 0;                                                                           \
      if (OB_FAIL(ObCharset::charset_convert(obj.get_collation_type(),                                   \
              obj.get_string_ptr(),                                                                      \
              obj.get_string_len(),                                                                      \
              ObCharset::get_system_collation(),                                                         \
              buffer + pos,                                                                              \
              length - pos,                                                                              \
              result_len))) {                                                                            \
      } else {                                                                                           \
        pos += result_len;                                                                               \
      }                                                                                                  \
    }                                                                                                    \
    return ret;                                                                                          \
  }                                                                                                      \
  template <>                                                                                            \
  inline int obj_print_json<OBJTYPE>(                                                                    \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)        \
  {                                                                                                      \
    UNUSED(params);                                                                                      \
    ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());                  \
    J_OBJ_START();                                                                                       \
    PRINT_META();                                                                                        \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                         \
    J_COLON();                                                                                           \
    BUF_PRINTO(obj.get_varchar());                                                                       \
    J_COMMA();                                                                                           \
    J_KV(N_COLLATION, ObCharset::collation_name(obj.get_collation_type()));                              \
    J_COMMA();                                                                                           \
    J_KV(N_COERCIBILITY, ObCharset::collation_level(obj.get_collation_level()));                         \
    J_OBJ_END();                                                                                         \
    return OB_SUCCESS;                                                                                   \
  }

#define DEF_STRING_CS_FUNCS(OBJTYPE)                                                                                \
  template <>                                                                                                       \
  inline int64_t obj_crc64<OBJTYPE>(const ObObj& obj, const int64_t current)                                        \
  {                                                                                                                 \
    int type = obj.get_type();                                                                                      \
    int cs = obj.get_collation_type();                                                                              \
    int64_t ret = ob_crc64_sse42(current, &type, sizeof(type));                                                     \
    ret = ob_crc64_sse42(ret, &cs, sizeof(cs));                                                                     \
    return ob_crc64_sse42(ret, obj.get_string_ptr(), obj.get_string_len());                                         \
  }                                                                                                                 \
  template <>                                                                                                       \
  inline int64_t obj_crc64_v2<OBJTYPE>(const ObObj& obj, const int64_t current)                                     \
  {                                                                                                                 \
    int cs = obj.get_collation_type();                                                                              \
    int64_t ret = ob_crc64_sse42(current, &cs, sizeof(cs));                                                         \
    return ob_crc64_sse42(ret, obj.get_string_ptr(), obj.get_string_len());                                         \
  }                                                                                                                 \
  template <>                                                                                                       \
  inline void obj_batch_checksum<OBJTYPE>(const ObObj& obj, ObBatchChecksum& bc)                                    \
  {                                                                                                                 \
    int type = obj.get_type();                                                                                      \
    int cs = obj.get_collation_type();                                                                              \
    bc.fill(&type, sizeof(type));                                                                                   \
    bc.fill(&cs, sizeof(cs));                                                                                       \
    bc.fill(obj.get_string_ptr(), obj.get_string_len());                                                            \
  }                                                                                                                 \
  template <>                                                                                                       \
  inline uint64_t obj_murmurhash<OBJTYPE>(const ObObj& obj, const uint64_t hash)                                    \
  {                                                                                                                 \
    return varchar_murmurhash(obj, obj.get_collation_type(), hash);                                                 \
  }                                                                                                                 \
  template <typename T>                                                                                             \
  struct ObjHashCalculator<OBJTYPE, T, ObObj> {                                                                     \
    static uint64_t calc_hash_value(const ObObj& obj, const uint64_t hash)                                          \
    {                                                                                                               \
      return varchar_hash_with_collation(obj, obj.get_collation_type(), hash, T::is_varchar_hash ? T::hash : NULL); \
    }                                                                                                               \
  };                                                                                                                \
  template <>                                                                                                       \
  inline uint64_t obj_crc64_v3<OBJTYPE>(const ObObj& obj, const uint64_t current)                                   \
  {                                                                                                                 \
    int cs = obj.get_collation_type();                                                                              \
    uint64_t ret = ob_crc64_sse42(current, &cs, sizeof(cs));                                                        \
    return ob_crc64_sse42(ret, obj.get_string_ptr(), obj.get_string_len());                                         \
  }

static uint64_t varchar_murmurhash(const ObObj& obj, const ObCollationType cs_type, const uint64_t hash)
{
  ObObjType obj_type = ob_is_nstring_type(obj.get_type()) ? ObNVarchar2Type : ObVarcharType;
  uint64_t ret = murmurhash(&obj_type, sizeof(obj_type), hash);
  return ObCharset::hash(cs_type,
      obj.get_string_ptr(),
      obj.get_string_len(),
      ret,
      obj.is_varying_len_char_type() && lib::is_oracle_mode(),
      NULL);
}

#define DEF_VARCHAR_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_VARCHAR_PRINT_FUNCS(OBJTYPE);             \
  DEF_STRING_CS_FUNCS(OBJTYPE);                 \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

DEF_VARCHAR_FUNCS(ObVarcharType, varchar, ObString);
DEF_VARCHAR_FUNCS(ObCharType, char, ObString);

// ObRawType=39
#define DEF_RAW_PRINT_FUNCS(OBJTYPE)                                                                \
  template <>                                                                                       \
  inline int obj_print_sql<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    ObString str = obj.get_string();                                                                \
    if (OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, "'"))) {                          \
    } else if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buffer, length, pos))) {     \
    } else {                                                                                        \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_str<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    ObString str = obj.get_string();                                                                \
    if (OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, "'"))) {                          \
    } else if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buffer, length, pos))) {     \
    } else {                                                                                        \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_plain_str<OBJTYPE>(                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    ObString str = obj.get_string();                                                                \
    if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buffer, length, pos))) {}           \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_json<OBJTYPE>(                                                               \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)   \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    J_OBJ_START();                                                                                  \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                    \
    J_COLON();                                                                                      \
    ObString str = obj.get_string();                                                                \
    if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buf, buf_len, pos))) {}             \
    J_COMMA();                                                                                      \
    J_KV(N_COLLATION, ObCharset::collation_name(obj.get_collation_type()));                         \
    J_COMMA();                                                                                      \
    J_KV(N_COERCIBILITY, ObCharset::collation_level(obj.get_collation_level()));                    \
    J_OBJ_END();                                                                                    \
    return ret;                                                                                     \
  }

#define DEF_RAW_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_RAW_PRINT_FUNCS(OBJTYPE);             \
  DEF_STRING_CS_FUNCS(OBJTYPE);             \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

DEF_RAW_FUNCS(ObRawType, raw, ObString);

#define DEF_ENUMSET_INNER_PRINT_FUNCS(OBJTYPE)                                                          \
  template <>                                                                                           \
  inline int obj_print_sql<OBJTYPE>(                                                                    \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    UNUSED(obj);                                                                                        \
    UNUSED(buffer);                                                                                     \
    UNUSED(length);                                                                                     \
    UNUSED(pos);                                                                                        \
    UNUSED(params);                                                                                     \
    return OB_ERR_UNEXPECTED;                                                                           \
  }                                                                                                     \
  template <>                                                                                           \
  inline int obj_print_str<OBJTYPE>(                                                                    \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(params);                                                                                     \
    if (OB_SUCC(ret)) {                                                                                 \
      ObEnumSetInnerValue inner_value;                                                                  \
      if (OB_FAIL(obj.get_enumset_inner_value(inner_value))) {                                          \
      } else {                                                                                          \
        ret = databuff_printf(buffer,                                                                   \
            length,                                                                                     \
            pos,                                                                                        \
            "%.*s; %ld",                                                                                \
            inner_value.string_value_.length(),                                                         \
            inner_value.string_value_.ptr(),                                                            \
            inner_value.numberic_value_);                                                               \
      }                                                                                                 \
    }                                                                                                   \
    return ret;                                                                                         \
  }                                                                                                     \
  template <>                                                                                           \
  inline int obj_print_plain_str<OBJTYPE>(                                                              \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    UNUSED(obj);                                                                                        \
    UNUSED(buffer);                                                                                     \
    UNUSED(length);                                                                                     \
    UNUSED(pos);                                                                                        \
    UNUSED(params);                                                                                     \
    return OB_ERR_UNEXPECTED;                                                                           \
  }                                                                                                     \
  template <>                                                                                           \
  inline int obj_print_json<OBJTYPE>(                                                                   \
      const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(params);                                                                                     \
    J_OBJ_START();                                                                                      \
    PRINT_META();                                                                                       \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                        \
    J_COLON();                                                                                          \
    ret = databuff_printf(buf, buf_len, pos, "\"");                                                     \
    if (OB_SUCC(ret)) {                                                                                 \
      ObEnumSetInnerValue inner_value;                                                                  \
      if (OB_FAIL(obj.get_enumset_inner_value(inner_value))) {                                          \
      } else {                                                                                          \
        ret = databuff_printf(buf,                                                                      \
            buf_len,                                                                                    \
            pos,                                                                                        \
            "%.*s; %ld",                                                                                \
            inner_value.string_value_.length(),                                                         \
            inner_value.string_value_.ptr(),                                                            \
            inner_value.numberic_value_);                                                               \
      }                                                                                                 \
    }                                                                                                   \
    if (OB_SUCC(ret)) {                                                                                 \
      ret = databuff_printf(buf, buf_len, pos, "\"");                                                   \
    }                                                                                                   \
    J_OBJ_END();                                                                                        \
    return ret;                                                                                         \
  }

#define DEF_ENUMSET_INNER_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_ENUMSET_INNER_PRINT_FUNCS(OBJTYPE);             \
  DEF_STRING_CS_FUNCS(OBJTYPE);                       \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

DEF_ENUMSET_INNER_FUNCS(ObEnumInnerType, enum_inner, ObString);
DEF_ENUMSET_INNER_FUNCS(ObSetInnerType, set_inner, ObString);

// TODO print and cs func need truncate values
#define DEF_TEXT_PRINT_FUNCS(OBJTYPE)                                                                        \
  template <>                                                                                                \
  inline int obj_print_sql<OBJTYPE>(                                                                         \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)          \
  {                                                                                                          \
    UNUSED(params);                                                                                          \
    int ret = OB_SUCCESS;                                                                                    \
    ObString str = obj.get_print_string(length - pos);                                                       \
    if (CS_TYPE_BINARY == obj.get_collation_type()) {                                                        \
      if (!lib::is_oracle_mode() && OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, "X'"))) {      \
      } else if (lib::is_oracle_mode() && OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, "'"))) { \
      } else if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buffer, length, pos))) {            \
      } else {                                                                                               \
        ret = databuff_printf(buffer, length, pos, "'");                                                     \
      }                                                                                                      \
    } else if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {                                         \
    } else {                                                                                                 \
      ObHexEscapeSqlStr sql_str(str);                                                                        \
      pos += sql_str.to_string(buffer + pos, length - pos);                                                  \
      ret = databuff_printf(buffer, length, pos, "'");                                                       \
    }                                                                                                        \
    return ret;                                                                                              \
  }                                                                                                          \
  template <>                                                                                                \
  inline int obj_print_str<OBJTYPE>(                                                                         \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)          \
  {                                                                                                          \
    UNUSED(params);                                                                                          \
    int ret = OB_SUCCESS;                                                                                    \
    ObString str = obj.get_print_string(length - pos);                                                       \
    if (CS_TYPE_BINARY == obj.get_collation_type()) {                                                        \
      ret = databuff_printf(buffer, length, pos, "'%.*s", str.length(), str.ptr());                          \
      if (OB_SUCC(ret)) {                                                                                    \
        if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {}                                           \
      }                                                                                                      \
    } else {                                                                                                 \
      ret = databuff_printf(buffer, length, pos, "'%.*s'", str.length(), str.ptr());                         \
    }                                                                                                        \
    return ret;                                                                                              \
  }                                                                                                          \
  template <>                                                                                                \
  inline int obj_print_plain_str<OBJTYPE>(                                                                   \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)          \
  {                                                                                                          \
    UNUSED(params);                                                                                          \
    ObString str = obj.get_print_string(length - pos);                                                       \
    return databuff_printf(buffer, length, pos, "%.*s", str.length(), str.ptr());                            \
  }                                                                                                          \
  template <>                                                                                                \
  inline int obj_print_json<OBJTYPE>(                                                                        \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)            \
  {                                                                                                          \
    UNUSED(params);                                                                                          \
    J_OBJ_START();                                                                                           \
    PRINT_META();                                                                                            \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                             \
    J_COLON();                                                                                               \
    BUF_PRINTO(obj.get_print_string(buf_len - pos));                                                         \
    J_COMMA();                                                                                               \
    J_KV(N_COLLATION, ObCharset::collation_name(obj.get_collation_type()));                                  \
    J_COMMA();                                                                                               \
    J_KV(N_COERCIBILITY, ObCharset::collation_level(obj.get_collation_level()));                             \
    J_OBJ_END();                                                                                             \
    return OB_SUCCESS;                                                                                       \
  }

#define DEF_TEXT_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)                                                        \
  template <>                                                                                                 \
  inline int obj_val_serialize<OBJTYPE>(const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos)     \
  {                                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    OB_UNIS_ENCODE(obj.get_##TYPE());                                                                         \
    return ret;                                                                                               \
  }                                                                                                           \
                                                                                                              \
  template <>                                                                                                 \
  inline int obj_val_deserialize<OBJTYPE>(ObObj & obj, const char* buf, const int64_t data_len, int64_t& pos) \
  {                                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    VTYPE v = VTYPE();                                                                                        \
    OB_UNIS_DECODE(v);                                                                                        \
    if (OB_SUCC(ret)) {                                                                                       \
      obj.set_##TYPE(OBJTYPE, v);                                                                             \
    }                                                                                                         \
    return ret;                                                                                               \
  }                                                                                                           \
                                                                                                              \
  template <>                                                                                                 \
  inline int64_t obj_val_get_serialize_size<OBJTYPE>(const ObObj& obj)                                        \
  {                                                                                                           \
    int64_t len = 0;                                                                                          \
    OB_UNIS_ADD_LEN(obj.get_##TYPE());                                                                        \
    return len;                                                                                               \
  }

#define DEF_TEXT_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_TEXT_PRINT_FUNCS(OBJTYPE);             \
  DEF_STRING_CS_FUNCS(OBJTYPE);              \
  DEF_TEXT_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

DEF_TEXT_FUNCS(ObTinyTextType, string, ObString);
DEF_TEXT_FUNCS(ObTextType, string, ObString);
DEF_TEXT_FUNCS(ObMediumTextType, string, ObString);
DEF_TEXT_FUNCS(ObLongTextType, string, ObString);

////////////////
// ObTimestampTZType=36,
// ObTimestampLTZType=37,
// ObTimestampNanoType=38,
#define DEF_ORACLE_TIMESTAMP_COMMON_PRINT_FUNCS(OBJTYPE)                                                      \
  template <>                                                                                                 \
  inline int obj_print_sql<OBJTYPE>(                                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)           \
  {                                                                                                           \
    int ret = databuff_printf(buffer, length, pos, "'");                                                      \
    if (OB_SUCC(ret)) {                                                                                       \
      const ObDataTypeCastParams dtc_params(params.tz_info_);                                                 \
      ret = ObTimeConverter::otimestamp_to_str(                                                               \
          obj.get_otimestamp_value(), dtc_params, obj.get_scale(), OBJTYPE, buffer, length, pos);             \
    }                                                                                                         \
    if (OB_SUCC(ret)) {                                                                                       \
      ret = databuff_printf(buffer, length, pos, "'");                                                        \
    }                                                                                                         \
    return ret;                                                                                               \
  }                                                                                                           \
  template <>                                                                                                 \
  inline int obj_print_str<OBJTYPE>(                                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)           \
  {                                                                                                           \
    int ret = databuff_printf(buffer, length, pos, "'");                                                      \
    if (OB_SUCC(ret)) {                                                                                       \
      const ObDataTypeCastParams dtc_params(params.tz_info_);                                                 \
      ret = ObTimeConverter::otimestamp_to_str(                                                               \
          obj.get_otimestamp_value(), dtc_params, obj.get_scale(), OBJTYPE, buffer, length, pos);             \
    }                                                                                                         \
    if (OB_SUCC(ret)) {                                                                                       \
      ret = databuff_printf(buffer, length, pos, "'");                                                        \
    }                                                                                                         \
    return ret;                                                                                               \
  }                                                                                                           \
  template <>                                                                                                 \
  inline int obj_print_plain_str<OBJTYPE>(                                                                    \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)           \
  {                                                                                                           \
    const ObDataTypeCastParams dtc_params(params.tz_info_);                                                   \
    return ObTimeConverter::otimestamp_to_str(                                                                \
        obj.get_otimestamp_value(), dtc_params, OB_MAX_TIMESTAMP_TZ_PRECISION, OBJTYPE, buffer, length, pos); \
  }                                                                                                           \
  template <>                                                                                                 \
  inline int obj_print_json<OBJTYPE>(                                                                         \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)             \
  {                                                                                                           \
    UNUSED(params);                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    J_OBJ_START();                                                                                            \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                              \
    J_COLON();                                                                                                \
    J_QUOTE();                                                                                                \
    const ObDataTypeCastParams dtc_params(params.tz_info_);                                                   \
    ret = ObTimeConverter::otimestamp_to_str(                                                                 \
        obj.get_otimestamp_value(), dtc_params, OB_MAX_TIMESTAMP_TZ_PRECISION, OBJTYPE, buf, buf_len, pos);   \
    if (OB_SUCC(ret)) {                                                                                       \
      J_QUOTE();                                                                                              \
      J_OBJ_END();                                                                                            \
    }                                                                                                         \
    return ret;                                                                                               \
  }

#define DEF_ORACLE_TIMESTAMP_TZ_CS_FUNCS(OBJTYPE)                                              \
  template <>                                                                                  \
  inline int64_t obj_crc64<OBJTYPE>(const ObObj& obj, const int64_t current)                   \
  {                                                                                            \
    int type = obj.get_type();                                                                 \
    int64_t ret = ob_crc64_sse42(current, &type, sizeof(type));                                \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                    \
    ret = ob_crc64_sse42(ret, &tmp_data.time_us_, sizeof(int64_t));                            \
    ret = ob_crc64_sse42(ret, &tmp_data.time_ctx_.desc_, sizeof(uint32_t));                    \
    return ret;                                                                                \
  }                                                                                            \
  template <>                                                                                  \
  inline int64_t obj_crc64_v2<OBJTYPE>(const ObObj& obj, const int64_t current)                \
  {                                                                                            \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                    \
    int64_t ret = ob_crc64_sse42(current, &tmp_data.time_us_, sizeof(int64_t));                \
    ret = ob_crc64_sse42(ret, &tmp_data.time_ctx_.desc_, sizeof(uint32_t));                    \
    return ret;                                                                                \
  }                                                                                            \
  template <>                                                                                  \
  inline void obj_batch_checksum<OBJTYPE>(const ObObj& obj, ObBatchChecksum& bc)               \
  {                                                                                            \
    int type = obj.get_type();                                                                 \
    bc.fill(&type, sizeof(type));                                                              \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                    \
    bc.fill(&tmp_data.time_us_, sizeof(int64_t));                                              \
    bc.fill(&tmp_data.time_ctx_.desc_, sizeof(uint32_t));                                      \
  }                                                                                            \
  template <>                                                                                  \
  inline uint64_t obj_murmurhash<OBJTYPE>(const ObObj& obj, const uint64_t hash)               \
  {                                                                                            \
    int type = obj.get_type();                                                                 \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                    \
    uint64_t ret = murmurhash(&type, sizeof(type), hash);                                      \
    ret = murmurhash(&tmp_data.time_us_, static_cast<int32_t>(sizeof(int64_t)), ret);          \
    ret = murmurhash(&tmp_data.time_ctx_.desc_, static_cast<int32_t>(sizeof(uint32_t)), ret);  \
    return ret;                                                                                \
  }                                                                                            \
  template <typename T>                                                                        \
  struct ObjHashCalculator<OBJTYPE, T, ObObj> {                                                \
    static uint64_t calc_hash_value(const ObObj& obj, const uint64_t hash)                     \
    {                                                                                          \
      ObOTimestampData tmp_data = obj.get_otimestamp_value();                                  \
      uint64_t ret = T::hash(&tmp_data.time_us_, static_cast<int32_t>(sizeof(int64_t)), hash); \
      ret = T::hash(&tmp_data.time_ctx_.desc_, static_cast<int32_t>(sizeof(uint32_t)), ret);   \
      return ret;                                                                              \
    }                                                                                          \
  };                                                                                           \
  template <>                                                                                  \
  inline uint64_t obj_crc64_v3<OBJTYPE>(const ObObj& obj, const uint64_t current)              \
  {                                                                                            \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                    \
    uint64_t ret = ob_crc64_sse42(current, &tmp_data.time_us_, sizeof(int64_t));               \
    ret = ob_crc64_sse42(ret, &tmp_data.time_ctx_.desc_, sizeof(uint32_t));                    \
    return ret;                                                                                \
  }
#define DEF_ORACLE_TIMESTAMP_TZ_SERIALIZE_FUNCS(OBJTYPE)                                                           \
  template <>                                                                                                      \
  inline int obj_val_serialize<OBJTYPE>(const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos)          \
  {                                                                                                                \
    const ObOTimestampData& ot_data = obj.get_otimestamp_value();                                                  \
    return serialization::encode_otimestamp_tz_type(buf, buf_len, pos, ot_data.time_us_, ot_data.time_ctx_.desc_); \
  }                                                                                                                \
                                                                                                                   \
  template <>                                                                                                      \
  inline int obj_val_deserialize<OBJTYPE>(ObObj & obj, const char* buf, const int64_t buf_len, int64_t& pos)       \
  {                                                                                                                \
    int ret = OB_SUCCESS;                                                                                          \
    ObOTimestampData ot_data;                                                                                      \
    ret = serialization::decode_otimestamp_tz_type(                                                                \
        buf, buf_len, pos, *((int64_t*)&ot_data.time_us_), *((uint32_t*)&ot_data.time_ctx_.desc_));                \
    obj.set_otimestamp_value(OBJTYPE, ot_data);                                                                    \
    return ret;                                                                                                    \
  }                                                                                                                \
                                                                                                                   \
  template <>                                                                                                      \
  inline int64_t obj_val_get_serialize_size<OBJTYPE>(const ObObj& obj)                                             \
  {                                                                                                                \
    UNUSED(obj);                                                                                                   \
    return serialization::encode_length_otimestamp_tz_type();                                                      \
  }

#define DEF_ORACLE_TIMESTAMP_CS_FUNCS(OBJTYPE)                                                     \
  template <>                                                                                      \
  inline int64_t obj_crc64<OBJTYPE>(const ObObj& obj, const int64_t current)                       \
  {                                                                                                \
    int type = obj.get_type();                                                                     \
    int64_t ret = ob_crc64_sse42(current, &type, sizeof(type));                                    \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                        \
    ret = ob_crc64_sse42(ret, &tmp_data.time_us_, sizeof(int64_t));                                \
    ret = ob_crc64_sse42(ret, &tmp_data.time_ctx_.time_desc_, sizeof(uint16_t));                   \
    return ret;                                                                                    \
  }                                                                                                \
  template <>                                                                                      \
  inline int64_t obj_crc64_v2<OBJTYPE>(const ObObj& obj, const int64_t current)                    \
  {                                                                                                \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                        \
    ObOTimestampDataOld timestamp_data(tmp_data.time_us_, tmp_data.time_ctx_.desc_);               \
    return ob_crc64_sse42(current, &timestamp_data, obj.get_otimestamp_store_size());              \
  }                                                                                                \
  template <>                                                                                      \
  inline void obj_batch_checksum<OBJTYPE>(const ObObj& obj, ObBatchChecksum& bc)                   \
  {                                                                                                \
    int type = obj.get_type();                                                                     \
    bc.fill(&type, sizeof(type));                                                                  \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                        \
    bc.fill(&tmp_data.time_us_, sizeof(int64_t));                                                  \
    bc.fill(&tmp_data.time_ctx_.time_desc_, sizeof(uint16_t));                                     \
  }                                                                                                \
  template <>                                                                                      \
  inline uint64_t obj_murmurhash<OBJTYPE>(const ObObj& obj, const uint64_t hash)                   \
  {                                                                                                \
    int type = obj.get_type();                                                                     \
    uint64_t ret = murmurhash(&type, sizeof(type), hash);                                          \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                        \
    ret = murmurhash(&tmp_data.time_us_, static_cast<int32_t>(sizeof(int64_t)), ret);              \
    ret = murmurhash(&tmp_data.time_ctx_.time_desc_, static_cast<int32_t>(sizeof(uint16_t)), ret); \
    return ret;                                                                                    \
  }                                                                                                \
  template <typename T>                                                                            \
  struct ObjHashCalculator<OBJTYPE, T, ObObj> {                                                    \
    static uint64_t calc_hash_value(const ObObj& obj, const uint64_t hash)                         \
    {                                                                                              \
      uint64_t ret = OB_SUCCESS;                                                                   \
      ObOTimestampData tmp_data = obj.get_otimestamp_value();                                      \
      ret = T::hash(&tmp_data.time_us_, static_cast<int32_t>(sizeof(int64_t)), hash);              \
      ret = T::hash(&tmp_data.time_ctx_.time_desc_, static_cast<int32_t>(sizeof(uint16_t)), ret);  \
      return ret;                                                                                  \
    }                                                                                              \
  };                                                                                               \
  template <>                                                                                      \
  inline uint64_t obj_crc64_v3<OBJTYPE>(const ObObj& obj, const uint64_t current)                  \
  {                                                                                                \
    ObOTimestampData tmp_data = obj.get_otimestamp_value();                                        \
    uint64_t ret = ob_crc64_sse42(current, &tmp_data.time_us_, sizeof(int64_t));                   \
    ret = ob_crc64_sse42(ret, &tmp_data.time_ctx_.time_desc_, sizeof(uint16_t));                   \
    return ret;                                                                                    \
  }
#define DEF_ORACLE_TIMESTAMP_SERIALIZE_FUNCS(OBJTYPE)                                                                \
  template <>                                                                                                        \
  inline int obj_val_serialize<OBJTYPE>(const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos)            \
  {                                                                                                                  \
    const ObOTimestampData& ot_data = obj.get_otimestamp_value();                                                    \
    return serialization::encode_otimestamp_type(buf, buf_len, pos, ot_data.time_us_, ot_data.time_ctx_.time_desc_); \
  }                                                                                                                  \
                                                                                                                     \
  template <>                                                                                                        \
  inline int obj_val_deserialize<OBJTYPE>(ObObj & obj, const char* buf, const int64_t buf_len, int64_t& pos)         \
  {                                                                                                                  \
    int ret = OB_SUCCESS;                                                                                            \
    ObOTimestampData ot_data;                                                                                        \
    ret = serialization::decode_otimestamp_type(                                                                     \
        buf, buf_len, pos, *((int64_t*)&ot_data.time_us_), *((uint16_t*)&ot_data.time_ctx_.time_desc_));             \
    obj.set_otimestamp_value(OBJTYPE, ot_data);                                                                      \
    return ret;                                                                                                      \
  }                                                                                                                  \
                                                                                                                     \
  template <>                                                                                                        \
  inline int64_t obj_val_get_serialize_size<OBJTYPE>(const ObObj& obj)                                               \
  {                                                                                                                  \
    UNUSED(obj);                                                                                                     \
    return serialization::encode_length_otimestamp_type();                                                           \
  }

#define DEF_ORACLE_TIMESTAMP_TZ_FUNCS(OBJTYPE)      \
  DEF_ORACLE_TIMESTAMP_COMMON_PRINT_FUNCS(OBJTYPE); \
  DEF_ORACLE_TIMESTAMP_TZ_CS_FUNCS(OBJTYPE);        \
  DEF_ORACLE_TIMESTAMP_TZ_SERIALIZE_FUNCS(OBJTYPE)

#define DEF_ORACLE_TIMESTAMP_FUNCS(OBJTYPE)         \
  DEF_ORACLE_TIMESTAMP_COMMON_PRINT_FUNCS(OBJTYPE); \
  DEF_ORACLE_TIMESTAMP_CS_FUNCS(OBJTYPE);           \
  DEF_ORACLE_TIMESTAMP_SERIALIZE_FUNCS(OBJTYPE)

DEF_ORACLE_TIMESTAMP_TZ_FUNCS(ObTimestampTZType);
DEF_ORACLE_TIMESTAMP_FUNCS(ObTimestampLTZType);
DEF_ORACLE_TIMESTAMP_FUNCS(ObTimestampNanoType);

// ObHexStringType=24,
#define DEF_HEX_STRING_PRINT_FUNCS(OBJTYPE)                                                         \
  template <>                                                                                       \
  inline int obj_print_sql<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    ObString str = obj.get_string();                                                                \
    if (OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, "X'"))) {                         \
    } else if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buffer, length, pos))) {     \
    } else {                                                                                        \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_str<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    ObString str = obj.get_string();                                                                \
    if (OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, "X'"))) {                         \
    } else if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buffer, length, pos))) {     \
    } else {                                                                                        \
      ret = databuff_printf(buffer, length, pos, "'");                                              \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_plain_str<OBJTYPE>(                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    ObString str = obj.get_string();                                                                \
    if (OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, "X"))) {                          \
    } else if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buffer, length, pos))) {     \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_json<OBJTYPE>(                                                               \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)   \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    J_OBJ_START();                                                                                  \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                    \
    J_COLON();                                                                                      \
    ObString str = obj.get_string();                                                                \
    if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, "\""))) {                           \
    } else if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buf, buf_len, pos))) {       \
    } else {                                                                                        \
      ret = databuff_printf(buf, buf_len, pos, "\"");                                               \
    }                                                                                               \
    J_OBJ_END();                                                                                    \
    return ret;                                                                                     \
  }

#define DEF_HEX_STRING_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_HEX_STRING_PRINT_FUNCS(OBJTYPE);             \
  DEF_STRING_CS_FUNCS(OBJTYPE);                    \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

DEF_HEX_STRING_FUNCS(ObHexStringType, hex_string, ObString);
////////////////
// ObExtendType=25,                 // Min, Max, etc.
template <>
inline int obj_print_sql<ObExtendType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  switch (obj.get_ext()) {
    case ObActionFlag::OP_MIN_OBJ:
      ret = databuff_printf(buffer, length, pos, "%s", ObObj::MIN_OBJECT_VALUE_STR);
      break;
    case ObActionFlag::OP_MAX_OBJ:
      ret = databuff_printf(buffer, length, pos, "%s", ObObj::MAX_OBJECT_VALUE_STR);
      break;
    case ObActionFlag::OP_NOP:
      ret = databuff_printf(buffer, length, pos, "%s", ObObj::NOP_VALUE_STR);
      break;
    case ObActionFlag::OP_DEFAULT_NOW_FLAG:
      ret = databuff_printf(buffer, length, pos, "%s", N_UPPERCASE_CUR_TIMESTAMP);
      break;
    default:
      _OB_LOG(WARN, "ext %ld should not be print as sql", obj.get_ext());
      ret = OB_INVALID_ARGUMENT;
      break;
  }
  return ret;
}

template <>
inline int obj_print_str<ObExtendType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  switch (obj.get_ext()) {
    case ObActionFlag::OP_MIN_OBJ:
      ret = databuff_printf(buffer, length, pos, "'%s'", ObObj::MIN_OBJECT_VALUE_STR);
      break;
    case ObActionFlag::OP_MAX_OBJ:
      ret = databuff_printf(buffer, length, pos, "'%s'", ObObj::MAX_OBJECT_VALUE_STR);
      break;
    case ObActionFlag::OP_NOP:
      ret = databuff_printf(buffer, length, pos, "'%s'", ObObj::NOP_VALUE_STR);
      break;
    case ObActionFlag::OP_DEFAULT_NOW_FLAG:
      ret = databuff_printf(buffer, length, pos, "'%s'", N_UPPERCASE_CUR_TIMESTAMP);
      break;
    default:
      _OB_LOG(WARN, "ext %ld should not be print as sql varchar literal", obj.get_ext());
      ret = OB_INVALID_ARGUMENT;
      break;
  }
  return ret;
}

template <>
inline int obj_print_plain_str<ObExtendType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  switch (obj.get_ext()) {
    case ObActionFlag::OP_MIN_OBJ:
      ret = databuff_printf(buffer, length, pos, "%s", ObObj::MIN_OBJECT_VALUE_STR);
      break;
    case ObActionFlag::OP_MAX_OBJ:
      ret = databuff_printf(buffer, length, pos, "%s", ObObj::MAX_OBJECT_VALUE_STR);
      break;
    case ObActionFlag::OP_NOP:
      ret = databuff_printf(buffer, length, pos, "%s", ObObj::NOP_VALUE_STR);
      break;
    case ObActionFlag::OP_DEFAULT_NOW_FLAG:
      ret = databuff_printf(buffer, length, pos, "%s", N_UPPERCASE_CUR_TIMESTAMP);
      break;
    default:
      _OB_LOG(WARN, "ext %ld should not be print as sql", obj.get_ext());
      ret = OB_INVALID_ARGUMENT;
      break;
  }
  return ret;
}

template <>
inline int obj_print_json<ObExtendType>(
    const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  J_OBJ_START();
  BUF_PRINTO(ob_obj_type_str(obj.get_type()));
  J_COLON();
  databuff_printf(buf, buf_len, pos, "\"");
  switch (obj.get_ext()) {
    case ObActionFlag::OP_MIN_OBJ:
      ret = databuff_printf(buf, buf_len, pos, "%s", ObObj::MIN_OBJECT_VALUE_STR);
      break;
    case ObActionFlag::OP_MAX_OBJ:
      ret = databuff_printf(buf, buf_len, pos, "%s", ObObj::MAX_OBJECT_VALUE_STR);
      break;
    case ObActionFlag::OP_NOP:
      ret = databuff_printf(buf, buf_len, pos, "%s", ObObj::NOP_VALUE_STR);
      break;
    case ObActionFlag::OP_DEFAULT_NOW_FLAG:
      ret = databuff_printf(buf, buf_len, pos, "%s", N_UPPERCASE_CUR_TIMESTAMP);
      break;
    default:
      BUF_PRINTO(obj.get_ext());
      break;
  }
  databuff_printf(buf, buf_len, pos, "\"");
  J_OBJ_END();
  return ret;
}
DEF_CS_FUNCS(ObExtendType, ext, int64_t, int64_t);
DEF_SERIALIZE_FUNCS(ObExtendType, ext, int64_t);
////////////////
// 28, ObUnknownType
template <>
inline int obj_print_sql<ObUnknownType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(obj);
  UNUSED(params);
  return databuff_printf(buffer, length, pos, ":%ld", obj.get_unknown());
}
template <>
inline int obj_print_str<ObUnknownType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(obj);
  UNUSED(params);
  return databuff_printf(buffer, length, pos, "'?'");
}
template <>
inline int obj_print_plain_str<ObUnknownType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(obj);
  UNUSED(params);
  return databuff_printf(buffer, length, pos, "?");
}
template <>
inline int obj_print_json<ObUnknownType>(
    const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(params);
  J_OBJ_START();
  BUF_PRINTO(ob_obj_type_str(obj.get_type()));
  J_COLON();
  BUF_PRINTO(obj.get_unknown());
  J_OBJ_END();
  return OB_SUCCESS;
}
template <>
inline int64_t obj_crc64<ObUnknownType>(const ObObj& obj, const int64_t current)
{
  return ob_crc64_sse42(current, &obj.get_meta(), sizeof(obj.get_meta()));
}
template <>
inline int64_t obj_crc64_v2<ObUnknownType>(const ObObj& obj, const int64_t current)
{
  UNUSED(obj);
  return current;
}
template <>
inline void obj_batch_checksum<ObUnknownType>(const ObObj& obj, ObBatchChecksum& bc)
{
  bc.fill(&obj.get_meta(), sizeof(obj.get_meta()));
}
template <>
inline uint64_t obj_murmurhash<ObUnknownType>(const ObObj& obj, const uint64_t hash)
{
  uint64_t ret = 0;
  int64_t value = obj.get_unknown();
  ret = murmurhash(&obj.get_meta(), sizeof(obj.get_meta()), hash);
  return murmurhash(&value, sizeof(obj.get_unknown()), ret);
}
template <typename T, typename P>
struct ObjHashCalculator<ObUnknownType, T, P> {
  static uint64_t calc_hash_value(const P& param, const uint64_t hash)
  {
    int64_t value = param.get_unknown();
    uint64_t ret = T::hash(&value, sizeof(value), hash);
    return ret;
  }
};
template <>
inline uint64_t obj_crc64_v3<ObUnknownType>(const ObObj& obj, const uint64_t current)
{
  UNUSED(obj);
  return current;
}
template <>
inline int obj_val_serialize<ObUnknownType>(const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(obj.get_unknown());
  return ret;
}

template <>
inline int obj_val_deserialize<ObUnknownType>(ObObj& obj, const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t val = 0;
  OB_UNIS_DECODE(val);
  if (OB_SUCC(ret)) {
    obj.set_unknown(val);
  }
  return ret;
}

template <>
inline int64_t obj_val_get_serialize_size<ObUnknownType>(const ObObj& obj)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(obj.get_unknown());
  return len;
}

////////////////
#define DEF_INTERVAL_PRINT_FUNCS(OBJTYPE, TYPE)                                                               \
  template <>                                                                                                 \
  inline int obj_print_sql<OBJTYPE>(                                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)           \
  {                                                                                                           \
    UNUSED(params);                                                                                           \
    const char* start = ob_date_unit_type_str_upper(obj.is_interval_ym() ? DATE_UNIT_YEAR : DATE_UNIT_DAY);   \
    const char* end = ob_date_unit_type_str_upper(obj.is_interval_ym() ? DATE_UNIT_MONTH : DATE_UNIT_SECOND); \
    int ret = OB_SUCCESS;                                                                                     \
    if (OB_FAIL(databuff_printf(buffer, length, pos, "INTERVAL '"))) { /*do nothing*/                         \
    } else if (OB_FAIL(ObTimeConverter::TYPE##_to_str(                                                        \
                   obj.get_##TYPE(), obj.get_scale(), buffer, length, pos, false))) { /*do nothing*/          \
    } else if (obj.is_interval_ym()                                                                           \
                   ? OB_FAIL(databuff_printf(buffer,                                                          \
                         length,                                                                              \
                         pos,                                                                                 \
                         "' %s (%d) TO %s",                                                                   \
                         start,                                                                               \
                         ObIntervalScaleUtil::ob_scale_to_interval_ym_year_scale(obj.get_scale()),            \
                         end))                                                                                \
                   : OB_FAIL(databuff_printf(buffer,                                                          \
                         length,                                                                              \
                         pos,                                                                                 \
                         "' %s (%d) TO %s (%d)",                                                              \
                         start,                                                                               \
                         ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(obj.get_scale()),             \
                         end,                                                                                 \
                         ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(obj.get_scale())))) {      \
      /*do nothing*/                                                                                          \
    }                                                                                                         \
    return ret;                                                                                               \
  }                                                                                                           \
                                                                                                              \
  template <>                                                                                                 \
  inline int obj_print_str<OBJTYPE>(                                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)           \
  {                                                                                                           \
    UNUSED(params);                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) { /*do nothing*/                                  \
    } else if (OB_FAIL(ObTimeConverter::TYPE##_to_str(                                                        \
                   obj.get_##TYPE(), obj.get_scale(), buffer, length, pos, false))) { /*do nothing*/          \
    } else if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {                  /*do nothing*/          \
    }                                                                                                         \
    return ret;                                                                                               \
  }                                                                                                           \
                                                                                                              \
  template <>                                                                                                 \
  inline int obj_print_plain_str<OBJTYPE>(                                                                    \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)           \
  {                                                                                                           \
    UNUSED(params);                                                                                           \
    return ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(), obj.get_scale(), buffer, length, pos, false);     \
  }                                                                                                           \
                                                                                                              \
  template <>                                                                                                 \
  inline int obj_print_json<OBJTYPE>(                                                                         \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)             \
  {                                                                                                           \
    UNUSED(params);                                                                                           \
    int ret = OB_SUCCESS;                                                                                     \
    J_OBJ_START();                                                                                            \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                              \
    J_COLON();                                                                                                \
    J_QUOTE();                                                                                                \
    if (OB_FAIL(ObTimeConverter::TYPE##_to_str(obj.get_##TYPE(),                                              \
            ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][obj.get_type()].get_scale(),                               \
            buf,                                                                                              \
            buf_len,                                                                                          \
            pos,                                                                                              \
            false))) {                                                                                        \
      COMMON_LOG(WARN, "failed to convert TYPE to str", K(ret));                                              \
    } else {                                                                                                  \
      J_QUOTE();                                                                                              \
      J_OBJ_END();                                                                                            \
    }                                                                                                         \
    return ret;                                                                                               \
  }

#define DEF_INTERVAL_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_INTERVAL_PRINT_FUNCS(OBJTYPE, TYPE);       \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

// ObIntervalYMType=39,
DEF_INTERVAL_FUNCS(ObIntervalYMType, interval_ym, ObIntervalYMValue);
// ObIntervalDSType=40,
DEF_INTERVAL_FUNCS(ObIntervalDSType, interval_ds, ObIntervalDSValue);

template <>
inline int64_t obj_crc64<ObIntervalYMType>(const ObObj& obj, const int64_t current)
{
  int type = obj.get_type();
  ObIntervalYMValue value = obj.get_interval_ym();
  int64_t result = current;
  result = ob_crc64_sse42(result, &type, sizeof(type));
  result = ob_crc64_sse42(result, &value.nmonth_, sizeof(value.nmonth_));
  return result;
}
template <>
inline int64_t obj_crc64_v2<ObIntervalYMType>(const ObObj& obj, const int64_t current)
{
  ObIntervalYMValue value = obj.get_interval_ym();
  return ob_crc64_sse42(current, &value.nmonth_, sizeof(value.nmonth_));
}
template <>
inline void obj_batch_checksum<ObIntervalYMType>(const ObObj& obj, ObBatchChecksum& bc)
{
  int type = obj.get_type();
  ObIntervalYMValue value = obj.get_interval_ym();

  bc.fill(&type, sizeof(type));
  bc.fill(&value.nmonth_, sizeof(value.nmonth_));
}
template <>
inline uint64_t obj_murmurhash<ObIntervalYMType>(const ObObj& obj, const uint64_t hash)
{
  int type = obj.get_type();
  ObIntervalYMValue value = obj.get_interval_ym();
  uint64_t result = hash;

  result = murmurhash(&type, sizeof(type), result);
  result = murmurhash(&value.nmonth_, sizeof(value.nmonth_), result);
  return result;
}
template <typename T, typename P>
struct ObjHashCalculator<ObIntervalYMType, T, P> {
  static uint64_t calc_hash_value(const P& param, const uint64_t hash)
  {
    ObIntervalYMValue value = param.get_interval_ym();
    return T::hash(&value.nmonth_, sizeof(value.nmonth_), hash);
  }
};
template <>
inline uint64_t obj_crc64_v3<ObIntervalYMType>(const ObObj& obj, const uint64_t current)
{
  ObIntervalYMValue value = obj.get_interval_ym();
  return ob_crc64_sse42(current, &value.nmonth_, sizeof(value.nmonth_));
}
template <>
inline int64_t obj_crc64<ObIntervalDSType>(const ObObj& obj, const int64_t current)
{
  int type = obj.get_type();
  ObIntervalDSValue value = obj.get_interval_ds();
  int64_t result = current;

  result = ob_crc64_sse42(result, &type, sizeof(type));
  result = ob_crc64_sse42(result, &value.nsecond_, sizeof(value.nsecond_));
  result = ob_crc64_sse42(result, &value.fractional_second_, sizeof(value.fractional_second_));
  return result;
}
template <>
inline int64_t obj_crc64_v2<ObIntervalDSType>(const ObObj& obj, const int64_t current)
{
  int64_t result = current;
  ObIntervalDSValue value = obj.get_interval_ds();

  result = ob_crc64_sse42(result, &value.nsecond_, sizeof(value.nsecond_));
  result = ob_crc64_sse42(result, &value.fractional_second_, sizeof(value.fractional_second_));
  return result;
}
template <>
inline void obj_batch_checksum<ObIntervalDSType>(const ObObj& obj, ObBatchChecksum& bc)
{
  int type = obj.get_type();
  ObIntervalDSValue value = obj.get_interval_ds();

  bc.fill(&type, sizeof(type));
  bc.fill(&value.nsecond_, sizeof(value.nsecond_));
  bc.fill(&value.fractional_second_, sizeof(value.fractional_second_));
}
template <>
inline uint64_t obj_murmurhash<ObIntervalDSType>(const ObObj& obj, const uint64_t hash)
{
  int type = obj.get_type();
  ObIntervalDSValue value = obj.get_interval_ds();
  uint64_t result = hash;

  result = murmurhash(&type, sizeof(type), result);
  result = murmurhash(&value.nsecond_, sizeof(value.nsecond_), result);
  result = murmurhash(&value.fractional_second_, sizeof(value.fractional_second_), result);
  return result;
}
template <typename T, typename P>
struct ObjHashCalculator<ObIntervalDSType, T, P> {
  static uint64_t calc_hash_value(const P& param, const uint64_t hash)
  {
    ObIntervalDSValue value = param.get_interval_ds();
    uint64_t result = hash;
    result = T::hash(&value.nsecond_, sizeof(value.nsecond_), result);
    result = T::hash(&value.fractional_second_, sizeof(value.fractional_second_), result);
    return result;
  }
};
template <>
inline uint64_t obj_crc64_v3<ObIntervalDSType>(const ObObj& obj, const uint64_t current)
{
  uint64_t result = current;
  ObIntervalDSValue value = obj.get_interval_ds();

  result = ob_crc64_sse42(result, &value.nsecond_, sizeof(value.nsecond_));
  result = ob_crc64_sse42(result, &value.fractional_second_, sizeof(value.fractional_second_));
  return result;
}

#define DEF_NVARCHAR_PRINT_FUNCS(OBJTYPE)                                                               \
  template <>                                                                                           \
  inline int obj_print_sql<OBJTYPE>(                                                                    \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());                 \
    ObCharsetType dst_type = ObCharset::charset_type_by_coll(params.cs_type_);                          \
    int ret = OB_SUCCESS;                                                                               \
    if (OB_FAIL(databuff_printf(buffer, length, pos, "n'"))) {                                          \
    } else if (src_type == dst_type) {                                                                  \
      ObHexEscapeSqlStr sql_str(obj.get_string());                                                      \
      pos += sql_str.to_string(buffer + pos, length - pos);                                             \
      ret = databuff_printf(buffer, length, pos, "'");                                                  \
    } else {                                                                                            \
      uint32_t result_len = 0;                                                                          \
      if (OB_FAIL(ObCharset::charset_convert(obj.get_collation_type(),                                  \
              obj.get_string_ptr(),                                                                     \
              obj.get_string_len(),                                                                     \
              params.cs_type_,                                                                          \
              buffer + pos,                                                                             \
              length - pos,                                                                             \
              result_len))) {                                                                           \
      } else {                                                                                          \
        ObHexEscapeSqlStr sql_str(ObString(result_len, buffer + pos));                                  \
        int64_t temp_pos = pos + result_len;                                                            \
        int64_t data_len = sql_str.to_string(buffer + temp_pos, length - temp_pos);                     \
        if (OB_UNLIKELY(temp_pos + data_len >= length)) {                                               \
          ret = OB_SIZE_OVERFLOW;                                                                       \
        } else {                                                                                        \
          MEMMOVE(buffer + pos, buffer + temp_pos, data_len);                                           \
          pos += data_len;                                                                              \
          ret = databuff_printf(buffer, length, pos, "'");                                              \
        }                                                                                               \
      }                                                                                                 \
    }                                                                                                   \
    return ret;                                                                                         \
  }                                                                                                     \
  template <>                                                                                           \
  inline int obj_print_str<OBJTYPE>(                                                                    \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());                 \
    ObCharsetType dst_type = ObCharset::charset_type_by_coll(params.cs_type_);                          \
    if (src_type == dst_type) {                                                                         \
      ret = databuff_printf(buffer, length, pos, "'%.*s'", obj.get_string_len(), obj.get_string_ptr()); \
    } else {                                                                                            \
      uint32_t result_len = 0;                                                                          \
      if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {                                         \
      } else if (OB_FAIL(ObCharset::charset_convert(obj.get_collation_type(),                           \
                     obj.get_string_ptr(),                                                              \
                     obj.get_string_len(),                                                              \
                     params.cs_type_,                                                                   \
                     buffer + pos,                                                                      \
                     length - pos,                                                                      \
                     result_len))) {                                                                    \
      } else {                                                                                          \
        pos += result_len;                                                                              \
        ret = databuff_printf(buffer, length, pos, "'");                                                \
      }                                                                                                 \
    }                                                                                                   \
    return ret;                                                                                         \
  }                                                                                                     \
  template <>                                                                                           \
  inline int obj_print_plain_str<OBJTYPE>(                                                              \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)     \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());                 \
    ObCharsetType dst_type = ObCharset::charset_type_by_coll(params.cs_type_);                          \
    if (src_type == dst_type) {                                                                         \
      ret = databuff_printf(buffer, length, pos, "%.*s", obj.get_string_len(), obj.get_string_ptr());   \
    } else {                                                                                            \
      uint32_t result_len = 0;                                                                          \
      if (OB_FAIL(ObCharset::charset_convert(obj.get_collation_type(),                                  \
              obj.get_string_ptr(),                                                                     \
              obj.get_string_len(),                                                                     \
              ObCharset::get_system_collation(),                                                        \
              buffer + pos,                                                                             \
              length - pos,                                                                             \
              result_len))) {                                                                           \
      } else {                                                                                          \
        pos += result_len;                                                                              \
      }                                                                                                 \
    }                                                                                                   \
    return ret;                                                                                         \
  }                                                                                                     \
  template <>                                                                                           \
  inline int obj_print_json<OBJTYPE>(                                                                   \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)       \
  {                                                                                                     \
    UNUSED(params);                                                                                     \
    ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());                 \
    J_OBJ_START();                                                                                      \
    PRINT_META();                                                                                       \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                        \
    J_COLON();                                                                                          \
    BUF_PRINTO(obj.get_varchar());                                                                      \
    J_COMMA();                                                                                          \
    J_KV(N_COLLATION, ObCharset::collation_name(obj.get_collation_type()));                             \
    J_COMMA();                                                                                          \
    J_KV(N_COERCIBILITY, ObCharset::collation_level(obj.get_collation_level()));                        \
    J_OBJ_END();                                                                                        \
    return OB_SUCCESS;                                                                                  \
  }

#define DEF_NVARCHAR_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_NVARCHAR_PRINT_FUNCS(OBJTYPE);             \
  DEF_STRING_CS_FUNCS(OBJTYPE);                  \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE)

DEF_NVARCHAR_FUNCS(ObNVarchar2Type, nvarchar2, ObString);
DEF_NVARCHAR_FUNCS(ObNCharType, nchar, ObString);

#define DEF_UROWID_RPINT_FUNCS(OBJTYPE)                                                             \
  template <>                                                                                       \
  inline int obj_print_sql<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {                                       \
    } else if (OB_FAIL(obj.get_urowid().get_base64_str(buffer, length, pos))) {                     \
    } else if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {                                \
    }                                                                                               \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_str<OBJTYPE>(                                                                \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    int ret = obj_print_sql<OBJTYPE>(obj, buffer, length, pos, params);                             \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_plain_str<OBJTYPE>(                                                          \
      const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params) \
  {                                                                                                 \
    UNUSED(params);                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    if (OB_FAIL(obj.get_urowid().get_base64_str(buffer, length, pos))) {}                           \
    return ret;                                                                                     \
  }                                                                                                 \
  template <>                                                                                       \
  inline int obj_print_json<OBJTYPE>(                                                               \
      const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)   \
  {                                                                                                 \
    int ret = OB_SUCCESS;                                                                           \
    UNUSED(params);                                                                                 \
    J_OBJ_START();                                                                                  \
    PRINT_META();                                                                                   \
    BUF_PRINTO(ob_obj_type_str(obj.get_type()));                                                    \
    J_COLON();                                                                                      \
    BUF_PRINTO(obj.get_urowid());                                                                   \
    J_COMMA();                                                                                      \
    J_KV(N_COLLATION, ObCharset::collation_name(obj.get_collation_type()));                         \
    J_COMMA();                                                                                      \
    J_KV(N_COERCIBILITY, ObCharset::collation_level(obj.get_collation_level()));                    \
    J_OBJ_END();                                                                                    \
    return ret;                                                                                     \
  }

#define DEF_UROWID_FUNCS(OBJTYPE, TYPE, VTYPE) \
  DEF_UROWID_RPINT_FUNCS(OBJTYPE);             \
  DEF_STRING_CS_FUNCS(OBJTYPE);                \
  DEF_SERIALIZE_FUNCS(OBJTYPE, TYPE, VTYPE);

DEF_UROWID_FUNCS(ObURowIDType, urowid, ObURowIDData);

template <>
inline int obj_print_sql<ObLobType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  ObString str = obj.get_lob_print_string(length - pos);
  if (CS_TYPE_BINARY == obj.get_collation_type()) {
    if (OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, "'"))) {
    } else if (OB_SUCCESS != (ret = hex_print(str.ptr(), str.length(), buffer, length, pos))) {
    } else {
      ret = databuff_printf(buffer, length, pos, "'");
    }
  } else if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {
  } else {
    ObHexEscapeSqlStr sql_str(str);
    pos += sql_str.to_string(buffer + pos, length - pos);
    ret = databuff_printf(buffer, length, pos, "'");
  }
  return ret;
}
template <>
inline int obj_print_str<ObLobType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  ObString str = obj.get_lob_print_string(length - pos);
  if (CS_TYPE_BINARY == obj.get_collation_type()) {
    ret = databuff_printf(buffer, length, pos, "'%.*s", str.length(), str.ptr());
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buffer, length, pos, "'"))) {}
    }
  } else {
    ret = databuff_printf(buffer, length, pos, "'%.*s'", str.length(), str.ptr());
  }
  return ret;
}
template <>
inline int obj_print_plain_str<ObLobType>(
    const ObObj& obj, char* buffer, int64_t length, int64_t& pos, const ObObjPrintParams& params)
{
  UNUSED(params);
  ObString str = obj.get_lob_print_string(length - pos);
  return databuff_printf(buffer, length, pos, "%.*s", str.length(), str.ptr());
}
template <>
inline int obj_print_json<ObLobType>(
    const ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, const ObObjPrintParams& params)
{
  const ObLobLocator* locator = nullptr;
  UNUSED(params);
  J_OBJ_START();
  PRINT_META();
  BUF_PRINTO(ob_obj_type_str(obj.get_type()));
  J_COLON();
  if (OB_ISNULL(locator = obj.get_lob_locator())) {
    J_KV("LobLocator", locator);
  } else {
    J_KV("LobLocator", *locator);
  }
  J_COMMA();
  J_KV(N_COLLATION, ObCharset::collation_name(obj.get_collation_type()));
  J_OBJ_END();
  return OB_SUCCESS;
}
template <>
inline int obj_val_serialize<ObLobType>(const ObObj& obj, char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const ObLobLocator* lob_locator = nullptr;
  if (OB_ISNULL(lob_locator = obj.get_lob_locator())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Unepected null lob locator", K(ret), K(obj));
  } else {
    int32_t val_len = obj.get_val_len();

    if (buf_len - pos < val_len + sizeof(val_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(TRACE, "buf not enough", K(buf_len), K(pos), K(val_len));
    } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, val_len))) {
      COMMON_LOG(WARN, "encode val length failed", K(ret));
    } else {
      MEMCPY(buf + pos, lob_locator, val_len);
      pos += val_len;
    }
  }
  return ret;
}
template <>
inline int obj_val_deserialize<ObLobType>(ObObj& obj, const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int32_t val_len = 0;
  if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &val_len))) {
    COMMON_LOG(WARN, "encode val length failed", K(ret));
  } else if (data_len - pos < val_len) {
    ret = OB_DESERIALIZE_ERROR;
    COMMON_LOG(WARN, "buf is not enough to cover all data", K(ret), K(data_len), K(val_len), K(pos));
  } else {
    obj.set_lob_locator(*(reinterpret_cast<const ObLobLocator*>(buf + pos)));
    pos += val_len;
  }
  return ret;
}
template <>
inline int64_t obj_val_get_serialize_size<ObLobType>(const ObObj& obj)
{
  int64_t len = 0;
  len += sizeof(int32_t) + obj.get_val_len();
  return len;
}
template <>
inline int64_t obj_crc64<ObLobType>(const ObObj& obj, const int64_t current)
{
  int type = obj.get_type();
  int cs = obj.get_collation_type();
  int64_t result = current;
  result = ob_crc64_sse42(result, &type, sizeof(type));
  result = ob_crc64_sse42(result, &cs, sizeof(cs));
  result = ob_crc64_sse42(result, obj.get_lob_locator(), obj.get_val_len());
  return result;
}
template <>
inline int64_t obj_crc64_v2<ObLobType>(const ObObj& obj, const int64_t current)
{
  int cs = obj.get_collation_type();
  int64_t result = ob_crc64_sse42(current, &cs, sizeof(cs));
  return ob_crc64_sse42(result, obj.get_lob_locator(), obj.get_val_len());
}
template <>
inline void obj_batch_checksum<ObLobType>(const ObObj& obj, ObBatchChecksum& bc)
{
  int type = obj.get_type();
  int cs = obj.get_collation_type();
  bc.fill(&type, sizeof(type));
  bc.fill(&cs, sizeof(cs));
  bc.fill(obj.get_lob_locator(), obj.get_val_len());
}
template <>
inline uint64_t obj_murmurhash<ObLobType>(const ObObj& obj, const uint64_t hash)
{
  int type = obj.get_type();
  ObCollationType cs_type = obj.get_collation_type();
  uint64_t result = hash;
  result = murmurhash(&type, sizeof(type), result);
  result = ObCharset::hash(cs_type, obj.get_lob_payload_ptr(), obj.get_lob_payload_size(), result, false, NULL);
  return result;
}
template <typename T>
struct ObjHashCalculator<ObLobType, T, ObObj> {
  static uint64_t calc_hash_value(const ObObj& obj, const uint64_t hash)
  {
    return ObCharset::hash(obj.get_collation_type(),
        obj.get_string_ptr(),
        obj.get_string_len(),
        hash,
        false,
        T::is_varchar_hash ? T::hash : NULL);
  }
};
// use for hash join, only calc payload
template <>
inline uint64_t obj_crc64_v3<ObLobType>(const ObObj& obj, const uint64_t current)
{
  int cs = obj.get_collation_type();
  uint64_t ret = ob_crc64_sse42(current, &cs, sizeof(cs));
  return ob_crc64_sse42(ret, obj.get_lob_payload_ptr(), obj.get_lob_payload_size());
}

}  // namespace common
}  // namespace oceanbase

#endif  //
