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

#ifndef OCEANBASE_SHARE_TABLE_OB_TABLE_OBJECT_
#define OCEANBASE_SHARE_TABLE_OB_TABLE_OBJECT_

#include "common/object/ob_object.h"
#include "common/ob_range.h"

namespace oceanbase
{
using namespace common;
namespace table
{

enum ObTableObjType
{
  ObTableNullType = 0,
  ObTableTinyIntType = 1,
  ObTableSmallIntType = 2,
  ObTableInt32Type = 3,
  ObTableInt64Type = 4,
  ObTableVarcharType = 5,
  ObTableVarbinaryType = 6,
  ObTableDoubleType = 7,
  ObTableFloatType = 8,
  ObTableTimestampType = 9,
  ObTableDateTimeType = 10,
  ObTableMinType = 11,
  ObTableMaxType = 12,
  ObTableUTinyIntType = 13,
  ObTableUSmallIntType = 14,
  ObTableUInt32Type = 15,
  ObTableUInt64Type = 16,
  ObTableTinyTextType = 17,
  ObTableTextType = 18,
  ObTableMediumTextType = 19,
  ObTableLongTextType = 20,
  ObTableTinyBlobType = 21,
  ObTableBlobType = 22,
  ObTableMediumBlobType = 23,
  ObTableLongBlobType = 24,
  ObTableCharType = 25,
  ObTableObjTypeMax = 26,
}; 

/*
  How to add a new ObTableObjType type T:
  1. Add the enumerated value of T type to the ObTableObjType enumeration
  2. Implement the deserialize/seralize/get_serialize_size template class method of the T type
  3. Add the mapping between type T and ObObjType
    3.1 If the type T and ObObjType are one-to-one map, add the relationship directly to OBJ_TABLE_TYPE_PAIRS
    3.2 Otherwise, add the mapping logic to convert_from_obj_type function
  4. Instantiate functions of type T into OB_TABLE_OBJ_FUNCS
*/
template <ObTableObjType T>
class ObTableObjFunc
{
public:
  static int deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj);
  static int serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj);
  static int64_t get_serialize_size(const ObObj &obj);

public:
  static const int64_t DEFAULT_TABLE_OBJ_TYPE_SIZE = 1;
  static const int64_t DEFAULT_TABLE_OBJ_META_SIZE = 4;
};

class ObTableSerialUtil
{
public:
  static int deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObObj &obj);
  static int serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObObj &obj);
  static int64_t get_serialize_size(const ObObj &obj);
  static int deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObNewRange &range);
  static int serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObNewRange &range);
  static int64_t get_serialize_size(const ObNewRange &range);
  static int deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObRowkey &range);
  static int serialize(char *buf, const int64_t buf_len, int64_t &pos, const ObRowkey &rowkey);
  static int64_t get_serialize_size(const ObRowkey &rowkey);
};

}  // namespace table
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_TABLE_OB_TABLE_OBJECT_ */