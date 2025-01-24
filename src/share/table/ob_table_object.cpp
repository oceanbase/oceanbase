/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "ob_table_object.h"
#include "common/object/ob_obj_funcs.h"

namespace oceanbase
{
namespace table
{

// default template
template <ObTableObjType T>
int ObTableObjFunc<T>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  UNUSEDx(buf, data_len, pos, obj);
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("the table object type is not implemented", K(ret), K(T));
  LOG_USER_ERROR(OB_NOT_IMPLEMENT, "table object type");
  return ret;
}

template <ObTableObjType T>
int ObTableObjFunc<T>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  UNUSEDx(buf, buf_len, pos, obj);
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("the table object type is not supported", K(ret), K(T));
  LOG_USER_ERROR(OB_NOT_IMPLEMENT, "table object type");
  return ret;
}

template <ObTableObjType T>
int64_t ObTableObjFunc<T>::get_serialize_size(const ObObj &obj)
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("the table object type is not supported", K(ret), K(T));
  LOG_USER_ERROR(OB_NOT_IMPLEMENT, "table object type");
  return ret;
}

template <ObTableObjType T1, ObObjType T2>
static int serialize_type_val(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(static_cast<uint8_t>(T1));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(obj_val_serialize<T2>(obj, buf, buf_len, pos))) {
      LOG_WARN("fail to serialize", KP(buf), K(buf_len), K(pos));
    }
  }
  return ret;
}

template <ObTableObjType T1, ObObjType T2>
static int64_t get_serialize_type_val_size(const ObObj &obj)
{
  int64_t len = 0;
  len += ObTableObjFunc<T1>::DEFAULT_TABLE_OBJ_TYPE_SIZE;
  len += obj_val_get_serialize_size<T2>(obj);
  return len;
}

#define DEF_TABLE_OBJ_DESERIALIZE_FUNC(T1, META_TYPE)     \
  int ret = OB_SUCCESS; \
  if (OB_FAIL(obj_val_deserialize<T1>(obj, buf, data_len, pos))) { \
    LOG_WARN("fail to deserialize", K(ret), KP(buf), K(data_len), K(pos)); \
  } else { \
    ObObjMeta meta; \
    meta.set_##META_TYPE(); \
    obj.set_meta_type(meta); \
  } \
  return ret;


// 0. ObTableNullType
template<>
int ObTableObjFunc<ObTableNullType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObNullType, null);
}

template<>
int ObTableObjFunc<ObTableNullType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableNullType, ObNullType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableNullType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableNullType, ObNullType>(obj);
}


// 1. ObTableTinyIntType
template<>
int ObTableObjFunc<ObTableTinyIntType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObTinyIntType, tinyint);
}

template<>
int ObTableObjFunc<ObTableTinyIntType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableTinyIntType, ObTinyIntType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableTinyIntType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableTinyIntType, ObTinyIntType>(obj);
}

// 2. ObTableSmallIntType
template<>
int ObTableObjFunc<ObTableSmallIntType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObSmallIntType, smallint);
}

template<>
int ObTableObjFunc<ObTableSmallIntType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableSmallIntType, ObSmallIntType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableSmallIntType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableSmallIntType, ObSmallIntType>(obj);
}

// 3. ObTableInt32Type
template<>
int ObTableObjFunc<ObTableInt32Type>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObInt32Type, int32);
}

template<>
int ObTableObjFunc<ObTableInt32Type>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableInt32Type, ObInt32Type>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableInt32Type>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableInt32Type, ObInt32Type>(obj);
}

// 4. ObTableInt64Type
template<>
int ObTableObjFunc<ObTableInt64Type>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObIntType, int);
}

template<>
int ObTableObjFunc<ObTableInt64Type>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableInt64Type, ObIntType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableInt64Type>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableInt64Type, ObIntType>(obj);
}

// 5. ObTableVarcharType
template<>
int ObTableObjFunc<ObTableVarcharType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObVarcharType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObVarcharType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableVarcharType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableVarcharType, ObVarcharType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableVarcharType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableVarcharType, ObVarcharType>(obj);
}

// 6. ObTableVarbinaryType
template<>
int ObTableObjFunc<ObTableVarbinaryType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObVarcharType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObVarcharType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_BINARY);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableVarbinaryType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableVarbinaryType, ObVarcharType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableVarbinaryType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableVarbinaryType, ObVarcharType>(obj);
}

// 7. ObTableDoubleType
template<>
int ObTableObjFunc<ObTableDoubleType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObDoubleType, double);
}

template<>
int ObTableObjFunc<ObTableDoubleType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableDoubleType, ObDoubleType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableDoubleType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableDoubleType, ObDoubleType>(obj);
}

// 8. ObTableFloatType
template<>
int ObTableObjFunc<ObTableFloatType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObFloatType, float);
}

template<>
int ObTableObjFunc<ObTableFloatType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableFloatType, ObFloatType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableFloatType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableFloatType, ObFloatType>(obj);
}

// 9. ObTableTimestampType
template<>
int ObTableObjFunc<ObTableTimestampType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  uint8_t cs_level = CS_LEVEL_INVALID;
  uint8_t cs_type = CS_TYPE_INVALID;
  int8_t scale = -1;
  OB_UNIS_DECODE(cs_level);
  OB_UNIS_DECODE(cs_type);
  OB_UNIS_DECODE(scale);

  obj.set_collation_level(static_cast<ObCollationLevel>(cs_level));
  obj.set_collation_type(static_cast<ObCollationType>(cs_type));
  obj.set_scale(scale);
  obj.set_type(ObTimestampType);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(obj_val_deserialize<ObTimestampType>(obj, buf, data_len, pos))) {
      LOG_WARN("fail to deserialize", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableTimestampType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(ObTableTimestampType);
  OB_UNIS_ENCODE(static_cast<uint8_t>(obj.get_collation_level()));
  OB_UNIS_ENCODE(static_cast<uint8_t>(obj.get_collation_type()));
  OB_UNIS_ENCODE(static_cast<int8_t>(obj.get_scale()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(obj_val_serialize<ObTimestampType>(obj, buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize", K(ret), KP(buf), K(buf_len), K(pos));
    }
  }
  return ret;
}

template<>
int64_t ObTableObjFunc<ObTableTimestampType>::get_serialize_size(const ObObj &obj)
{
  int64_t len = 0;
  len += DEFAULT_TABLE_OBJ_META_SIZE;
  len += obj_val_get_serialize_size<ObTimestampType>(obj);
  return len;
}

// 10. ObTableDateTimeType
template<>
int ObTableObjFunc<ObTableDateTimeType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  uint8_t cs_level = CS_LEVEL_INVALID;
  uint8_t cs_type = CS_TYPE_INVALID;
  int8_t scale = -1;
  OB_UNIS_DECODE(cs_level);
  OB_UNIS_DECODE(cs_type);
  OB_UNIS_DECODE(scale);
  obj.set_type(ObDateTimeType);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(obj_val_deserialize<ObDateTimeType>(obj, buf, data_len, pos))) {
      LOG_WARN("fail to deserialize", K(ret), KP(buf), K(data_len), K(pos));
    }
  }

  return ret;
}

template<>
int ObTableObjFunc<ObTableDateTimeType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(ObTableDateTimeType);
  OB_UNIS_ENCODE(static_cast<uint8_t>(obj.get_collation_level()));
  OB_UNIS_ENCODE(static_cast<uint8_t>(obj.get_collation_type()));
  OB_UNIS_ENCODE(static_cast<int8_t>(obj.get_scale()));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(obj_val_serialize<ObDateTimeType>(obj, buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize", K(ret), KP(buf), K(buf_len), K(pos));
    }
  }
  return ret;
}

template<>
int64_t ObTableObjFunc<ObTableDateTimeType>::get_serialize_size(const ObObj &obj)
{
  int64_t len = 0;
  len += DEFAULT_TABLE_OBJ_META_SIZE;
  len += obj_val_get_serialize_size<ObDateTimeType>(obj);
  return len;
}

// 11. ObTableMinType
template<>
int ObTableObjFunc<ObTableMinType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  obj.set_min_value();
  return ret;
}

template<>
int ObTableObjFunc<ObTableMinType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(static_cast<uint8_t>(ObTableMinType));
  return ret;
}

template<>
int64_t ObTableObjFunc<ObTableMinType>::get_serialize_size(const ObObj &obj)
{
  return DEFAULT_TABLE_OBJ_TYPE_SIZE;
}

// 12. ObTableMaxType
template<>
int ObTableObjFunc<ObTableMaxType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  obj.set_max_value();
  return ret;
}

template<>
int ObTableObjFunc<ObTableMaxType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(static_cast<uint8_t>(ObTableMaxType));
  return ret;
}

template<>
int64_t ObTableObjFunc<ObTableMaxType>::get_serialize_size(const ObObj &obj)
{
  return DEFAULT_TABLE_OBJ_TYPE_SIZE;
}

// 13. ObTableUTinyIntType
template<>
int ObTableObjFunc<ObTableUTinyIntType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObUTinyIntType, utinyint);
}

template<>
int ObTableObjFunc<ObTableUTinyIntType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableUTinyIntType, ObUTinyIntType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableUTinyIntType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableUTinyIntType, ObUTinyIntType>(obj);
}

// 14. ObTableUSmallIntType
template<>
int ObTableObjFunc<ObTableUSmallIntType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObUSmallIntType, usmallint);
}

template<>
int ObTableObjFunc<ObTableUSmallIntType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableUSmallIntType, ObUSmallIntType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableUSmallIntType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableUSmallIntType, ObUSmallIntType>(obj);
}

// 15. ObTableUInt32Type
template<>
int ObTableObjFunc<ObTableUInt32Type>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObUInt32Type, uint32);
}

template<>
int ObTableObjFunc<ObTableUInt32Type>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableUInt32Type, ObUInt32Type>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableUInt32Type>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableUInt32Type, ObUInt32Type>(obj);
}

// 16. ObTableUInt64Type
template<>
int ObTableObjFunc<ObTableUInt64Type>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  DEF_TABLE_OBJ_DESERIALIZE_FUNC(ObUInt64Type, uint64);
}

template<>
int ObTableObjFunc<ObTableUInt64Type>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableInt64Type, ObUInt64Type>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableUInt64Type>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableInt64Type, ObUInt64Type>(obj);
}

// 17. ObTableTinyTextType
template<>
int ObTableObjFunc<ObTableTinyTextType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObTinyTextType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObTinyTextType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableTinyTextType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableTinyTextType, ObTinyTextType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableTinyTextType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableTinyTextType, ObTinyTextType>(obj);
}

// 18. ObTableTextType
template<>
int ObTableObjFunc<ObTableTextType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObTextType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObTextType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableTextType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableTextType, ObTextType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableTextType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableTextType, ObTextType>(obj);
}

// 19. ObTableMediumTextType
template<>
int ObTableObjFunc<ObTableMediumTextType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObMediumTextType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObMediumTextType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableMediumTextType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableMediumTextType, ObMediumTextType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableMediumTextType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableMediumTextType, ObMediumTextType>(obj);
}

// 20. ObTableLongTextType
template<>
int ObTableObjFunc<ObTableLongTextType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObLongTextType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObLongTextType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableLongTextType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableLongTextType, ObLongTextType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableLongTextType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableLongTextType, ObLongTextType>(obj);
}

// 21. ObTableTinyBlobType
template<>
int ObTableObjFunc<ObTableTinyBlobType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObTinyTextType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObTinyTextType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_BINARY);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableTinyBlobType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableTinyBlobType, ObTinyTextType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableTinyBlobType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableTinyBlobType, ObTinyTextType>(obj);
}

// 22. ObTableBlobType
template<>
int ObTableObjFunc<ObTableBlobType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObTextType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObTextType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_BINARY);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableBlobType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableBlobType, ObTextType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableBlobType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableBlobType, ObTextType>(obj);
}

// 23. ObTableMediumBlobType
template<>
int ObTableObjFunc<ObTableMediumBlobType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObMediumTextType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObMediumTextType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_BINARY);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableMediumBlobType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableMediumBlobType, ObMediumTextType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableMediumBlobType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableMediumBlobType, ObMediumTextType>(obj);
}

// 24. ObTableLongBlobType
template<>
int ObTableObjFunc<ObTableLongBlobType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObLongTextType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObLongTextType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_BINARY);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableLongBlobType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableLongBlobType, ObLongTextType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableLongBlobType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableLongBlobType, ObLongTextType>(obj);
}

// 25. ObTableCharType
template<>
int ObTableObjFunc<ObTableCharType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_val_deserialize<ObCharType>(obj, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KP(buf), K(data_len), K(pos));
  } else {
    obj.set_type(ObCharType);
    obj.set_collation_level(CS_LEVEL_EXPLICIT);
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

template<>
int ObTableObjFunc<ObTableCharType>::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  return serialize_type_val<ObTableCharType, ObCharType>(buf, buf_len, pos, obj);
}

template<>
int64_t ObTableObjFunc<ObTableCharType>::get_serialize_size(const ObObj &obj)
{
  return get_serialize_type_val_size<ObTableCharType, ObCharType>(obj);
}

using ob_table_obj_deserialize = int (*)(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj);
using ob_table_obj_serialize = int (*)(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj);
using ob_table_obj_get_serialize_size = int64_t (*)(const ObObj &obj);
struct ObTableObjTypeFuncs
{
  ob_table_obj_deserialize deserialize;
  ob_table_obj_serialize serialize;
  ob_table_obj_get_serialize_size get_serialize_size;
};

#define DEF_TABLE_OBJ_FUNC_ENTRY(TABLE_OBJ_TYPE)        \
  {                                                     \
    ObTableObjFunc<TABLE_OBJ_TYPE>::deserialize,        \
    ObTableObjFunc<TABLE_OBJ_TYPE>::serialize,          \
    ObTableObjFunc<TABLE_OBJ_TYPE>::get_serialize_size,          \
  }

static const ObTableObjTypeFuncs OB_TABLE_OBJ_FUNCS[ObTableObjTypeMax] =
{
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableNullType),      // 0
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableTinyIntType),   // 1
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableSmallIntType),  // 2
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableInt32Type),     // 3
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableInt64Type),     // 4
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableVarcharType),   // 5
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableVarbinaryType), // 6
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableDoubleType),    // 7
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableFloatType),     // 8
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableTimestampType), // 9
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableDateTimeType),  // 10
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableMinType),  // 11
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableMaxType),  // 12
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableUTinyIntType),   // 13
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableUSmallIntType),  // 14
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableUInt32Type),     // 15
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableUInt64Type),     // 16
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableTinyTextType),   // 17
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableTextType),       // 18
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableMediumTextType), // 19
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableLongTextType),   // 20
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableTinyBlobType),   // 21
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableBlobType),       // 22
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableMediumBlobType), // 23
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableLongBlobType),   // 24
  DEF_TABLE_OBJ_FUNC_ENTRY(ObTableCharType),       // 25
};

#define OBJ_TABLE_TYPE_CASE(x, y) \
  case (x) : { \
    table_type = y; \
    break;\
  }

#define OBJ_TABLE_TYPE_MAP(arg) OBJ_TABLE_TYPE_CASE arg

#define OBJ_TABLE_TYPE_PAIRS             \
		(ObNullType, ObTableNullType),        \
		(ObTinyIntType, ObTableTinyIntType),     \
		(ObSmallIntType, ObTableSmallIntType),    \
		(ObInt32Type, ObTableInt32Type),       \
		(ObIntType, ObTableInt64Type),       \
		(ObDoubleType, ObTableDoubleType),      \
		(ObFloatType, ObTableFloatType),       \
		(ObTimestampType, ObTableTimestampType),   \
		(ObDateTimeType, ObTableDateTimeType),    \
		(ObUTinyIntType, ObTableUTinyIntType),     \
		(ObUSmallIntType, ObTableUSmallIntType),    \
		(ObUInt32Type, ObTableUInt32Type),       \
		(ObUInt64Type, ObTableUInt64Type),      \
    (ObCharType, ObTableCharType)

static int convert_from_obj_type(const ObObj &obj, ObTableObjType &table_type)
{
  int ret = OB_SUCCESS;
  const ObObjType obj_type = obj.get_type();
  const ObCollationType cs_type = obj.get_collation_type();
  switch(obj_type) {
    LST_DO(OBJ_TABLE_TYPE_MAP, (), OBJ_TABLE_TYPE_PAIRS)
    case (ObVarcharType) : {
      if (obj.is_varbinary()) {
        table_type = ObTableVarbinaryType;
      } else {
        table_type = ObTableVarcharType;
      }
      break;
    }
    case (ObExtendType) : {
      if (obj.is_min_value()) {
        table_type = ObTableMinType;
      } else if (obj.is_max_value()) {
        table_type = ObTableMaxType;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("the obj type is not supported for table obj type", K(ret), K(obj));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the obj type");
      }
      break;
    }
    case (ObTinyTextType) : {
      if (ob_is_blob(obj_type, cs_type)) {
        table_type = ObTableTinyBlobType;
      } else {
        table_type = ObTableTinyTextType;
      }
      break;
    }
    case (ObTextType) : {
      if (ob_is_blob(obj_type, cs_type)) {
        table_type = ObTableBlobType;
      } else {
        table_type = ObTableTextType;
      }
      break;
    }
    case (ObMediumTextType) : {
      if (ob_is_blob(obj_type, cs_type)) {
        table_type = ObTableMediumBlobType;
      } else {
        table_type = ObTableMediumTextType;
      }
      break;
    }
    case (ObLongTextType) : {
      if (ob_is_blob(obj_type, cs_type)) {
        table_type = ObTableLongBlobType;
      } else {
        table_type = ObTableLongTextType;
      }
      break;
    }
    default : {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the obj type is not supported for table obj type", K(ret), K(obj));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "the obj type");
    }
  }
  return ret;
}

int ObTableSerialUtil::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj)
{
  int ret = OB_SUCCESS;
  uint8_t type = uint8_t();
  OB_UNIS_DECODE(type);
  if (OB_SUCC(ret)) {
    ObTableObjType table_type = static_cast<ObTableObjType>(type);
    if (table_type >= ObTableObjTypeMax || table_type < 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("table object type is not supported", K(ret), K(table_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "the table object type");
    } else if (OB_FAIL(OB_TABLE_OBJ_FUNCS[table_type].deserialize(buf, data_len, pos, obj))) {
      LOG_WARN("failed to deserialize table object", K(ret), K(table_type));
    }
  }
  return ret;
}

int ObTableSerialUtil::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObTableObjType table_type;
  if (OB_FAIL(convert_from_obj_type(obj, table_type))) {
    LOG_WARN("fail to convert table type from obj type", K(ret), K(obj));
  } else if (table_type >= ObTableObjTypeMax || table_type < 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table object type is not supported", K(ret), K(table_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the table object type");
  } else if (OB_FAIL(OB_TABLE_OBJ_FUNCS[table_type].serialize(buf, buf_len, pos, obj))) {
    LOG_WARN("failed to serialize table object", K(ret), K(table_type));
  }
  return ret;
}

int64_t ObTableSerialUtil::get_serialize_size(const ObObj &obj)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  ObTableObjType table_type;
  if (OB_FAIL(convert_from_obj_type(obj, table_type))) {
    LOG_WARN("fail to convert table type from obj type", K(ret), K(obj));
  } else if (table_type >= ObTableObjTypeMax || table_type < 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table object type is not supported", K(ret), K(table_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the table object type");
  } else {
    len = OB_TABLE_OBJ_FUNCS[table_type].get_serialize_size(obj);
  }
  return len;
}

int ObTableSerialUtil::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  int8_t flag = 0;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(
      buf, data_len, pos, reinterpret_cast<int64_t *>(&range.table_id_)))) {
    LOG_WARN("deserialize table_id failed.", KP(buf), K(data_len), K(pos), K_(range.table_id), K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &flag))) {
    LOG_WARN("deserialize flag failed.", KP(buf), K(data_len), K(pos), K(flag), K(ret));
  } else if (OB_FAIL(deserialize(buf, data_len, pos, range.start_key_))) {
    LOG_WARN("deserialize start_key failed.", KP(buf), K(data_len), K(pos), K_(range.start_key), K(ret));
  } else if (OB_FAIL(deserialize(buf, data_len, pos, range.end_key_))) {
    LOG_WARN("deserialize end_key failed.", KP(buf), K(data_len), K(pos), K_(range.end_key), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &range.flag_))) {
    LOG_WARN("deserialize flag failed.", KP(buf), K(data_len), K(pos), K_(range.flag), K(ret));
  } else {
    range.border_flag_.set_data(flag);
  }
  return ret;
}

int ObTableSerialUtil::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64( buf, buf_len, pos, static_cast<int64_t>(range.table_id_)))) {
    LOG_WARN("serialize table_id failed.", KP(buf), K(buf_len), K(pos), K_(range.table_id), K(ret));
  } else if (OB_FAIL(serialization::encode_i8( buf, buf_len, pos, range.border_flag_.get_data()))) {
    LOG_WARN("serialize border_flag failed.", KP(buf), K(buf_len), K(pos), K_(range.border_flag), K(ret));
  } else if (OB_FAIL(serialize(buf, buf_len, pos, range.start_key_))) {
    LOG_WARN("serialize start_key failed.", KP(buf), K(buf_len), K(pos), K_(range.start_key), K(ret));
  } else if (OB_FAIL(serialize(buf, buf_len, pos, range.end_key_))) {
    LOG_WARN("serialize end_key failed.", KP(buf), K(buf_len), K(pos), K_(range.end_key), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, range.flag_))) {
    LOG_WARN("serialize flag failed.", KP(buf), K(buf_len), K(pos), K_(range.flag), K(ret));
  }
  return ret;
}

int64_t ObTableSerialUtil::get_serialize_size(const ObNewRange &range)
{
  int64_t total_size = 0;

  total_size += serialization::encoded_length_vi64(range.table_id_);
  total_size += serialization::encoded_length_i8(range.border_flag_.get_data());

  total_size += get_serialize_size(range.start_key_);
  total_size += get_serialize_size(range.end_key_);
  total_size += serialization::encoded_length_vi64(range.flag_);

  return total_size;
}

int ObTableSerialUtil::deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  int64_t obj_cnt = 0;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &obj_cnt))) {
    LOG_WARN("decode object count failed.", KP(buf), K(data_len), K(pos), K(obj_cnt), K(ret));
  } else if (obj_cnt > 0) {
    // If there is no assign external ptr, then dynamically assign
    ObObj *obj_ptr = rowkey.get_obj_ptr();
    const int64_t presrv_obj_cnt = rowkey.get_obj_cnt();
    if (NULL == obj_ptr) {
      if (OB_ISNULL(obj_ptr = static_cast<ObObj *>
          (lib::this_worker().get_sql_arena_allocator().alloc(sizeof(ObObj) * obj_cnt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for decode ObRowKey", K(ret), K(obj_cnt), K(sizeof(ObObj)));
      }
    // Otherwise, check whether the number of reserved primary keys meets the requirements
    } else if (presrv_obj_cnt < obj_cnt) {
      LOG_ERROR("preserved obj count is not enough", K(presrv_obj_cnt), K(obj_cnt), K(obj_ptr));
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      // Use the reserved obj array
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < obj_cnt; i++) {
      if (OB_FAIL(deserialize(buf, data_len, pos, obj_ptr[i]))) {
        LOG_WARN("deserialize object failed.", K(i), KP(buf), K(data_len), K(pos), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      rowkey.assign(obj_ptr, obj_cnt);
    }
  }

  return ret;
}

int ObTableSerialUtil::serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  int64_t obj_cnt = rowkey.get_obj_cnt();
  const ObObj *obj_ptr = rowkey.get_obj_ptr();
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_legal())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("illegal rowkey.", KP(obj_ptr), K(obj_cnt), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, obj_cnt))) {
    LOG_WARN("encode object count failed.", KP(buf), K(buf_len), K(pos), K(obj_cnt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_cnt; i++) {
      if (OB_FAIL(serialize(buf, buf_len, pos, obj_ptr[i]))) {
        LOG_WARN("serialize objects failed.", KP(buf), K(buf_len), K(pos), K(obj_cnt), K(ret));
      }
    }
  }
  return ret;

}

int64_t ObTableSerialUtil::get_serialize_size(const ObRowkey &rowkey)
{
  int64_t obj_cnt = rowkey.get_obj_cnt();
  const ObObj *obj_ptr = rowkey.get_obj_ptr();
  int64_t size = serialization::encoded_length_vi64(obj_cnt);
  for (int64_t i = 0; i < obj_cnt; i++) {
    size += get_serialize_size(obj_ptr[i]);
  }
  return size;
}


}  // namespace table
}  // namespace oceanbase
