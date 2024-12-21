/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// register group obj class and then add its define header file in ob_table_group_traits_type.h
#ifdef GROUP_OBJ_DEF
GROUP_OBJ_DEF(TYPE_TABLE_GROUP, "TABLE_GROUP", ObTableOp, ObTableGroupKey, ObTableGroupValue, ObTableGroupMeta, ObTableOpProcessor)
GROUP_OBJ_DEF(TYPE_REDIS_GROUP, "REDIS_GROUP", ObRedisOp, ObRedisCmdKey, ObTableGroupValue, ObTableGroupMeta, ObRedisGroupOpProcessor)
GROUP_OBJ_DEF(TYPE_HBASE_GROUP, "HBASE_GROUP", ObHbaseOp, ObHbaseGroupKey, ObTableGroupValue, ObTableGroupMeta, ObHbaseOpProcessor)
#endif

#ifndef OCEANBASE_OBSERVER_OB_TABLE_GROUP_REGISTER_H_
#define OCEANBASE_OBSERVER_OB_TABLE_GROUP_REGISTER_H_
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace table
{

class ObITableOp;
class ObITableGroupKey;
class ObITableGroupValue;
class ObITableGroupMeta;
class ObITableOpProcessor;

enum ObTableGroupType
{
TYPE_INVALID = 0,
#define GROUP_OBJ_DEF(type, type_name, op, key, value, meta, processor) type,
#include "observer/table/group/ob_table_group_register.h"
#undef GROUP_OBJ_DEF
TYPE_MAX
};

typedef int (*OPAllocFunc) (common::ObIAllocator &allocator,
                            ObITableOp *&op);
typedef int (*KEYAllocFunc) (common::ObIAllocator &allocator,
                             ObITableGroupKey *&key);
typedef int (*VALAllocFunc) (common::ObIAllocator &allocator,
                             ObITableGroupValue *&value);
typedef int (*METAAllocFunc) (common::ObIAllocator &allocator,
                              ObITableGroupMeta *&meta);
typedef int (*PROCAllocFunc) (common::ObIAllocator &allocator,
                              ObITableOpProcessor *&processor);
class GroupAllocHelper
{
public:
  template<class FATHER_CLASS, class CHILD_CLASS>
  static int alloc(ObIAllocator &alloc, FATHER_CLASS *&obj)
  {
    int ret = OB_SUCCESS;
    void *ptr = nullptr;
    if (OB_ISNULL(ptr = alloc.alloc(sizeof(CHILD_CLASS)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      obj = new(ptr) CHILD_CLASS();
    }
    return ret;
  }
};

class ObTableGroupRegister
{
public:
  static void register_group_objs();
  static ObTableGroupType get_op_type_by_name(const ObString &name);

public:
  static OPAllocFunc OP_ALLOC[TYPE_MAX];
  static KEYAllocFunc KEY_ALLOC[TYPE_MAX];
  static VALAllocFunc VAL_ALLOC[TYPE_MAX];
  static METAAllocFunc META_ALLOC[TYPE_MAX];
  static PROCAllocFunc PROC_ALLOC[TYPE_MAX];
  static const char *TYPE_NAMES[TYPE_MAX];
  static lib::ObLabel TYPE_LABELS[TYPE_MAX];
};

#define GROUP_OP_ALLOC (ObTableGroupRegister::OP_ALLOC)
#define GROUP_KEY_ALLOC (ObTableGroupRegister::KEY_ALLOC)
#define GROUP_VAL_ALLOC (ObTableGroupRegister::VAL_ALLOC)
#define GROUP_META_ALLOC (ObTableGroupRegister::META_ALLOC)
#define GROUP_PROC_ALLOC (ObTableGroupRegister::PROC_ALLOC)
#define GROUP_NAMES (ObTableGroupRegister::TYPE_NAMES)
#define GROUP_LABELS (ObTableGroupRegister::TYPE_LABELS)

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_GROUP_REGISTER_H_ */
