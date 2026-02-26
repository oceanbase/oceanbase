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
#define USING_LOG_PREFIX SERVER
#include "ob_table_group_register.h"
#include "ob_table_group_common.h"
#include "observer/table/redis/group/ob_redis_group_processor.h"
#include "observer/table/redis/group/ob_redis_group_struct.h"
#include "observer/table/redis/cmd/ob_redis_cmd.h"
#include "observer/table/hbase/ob_hbase_group_struct.h"
#include "observer/table/hbase/ob_hbase_group_processor.h"
#include "ob_table_group_execute.h"
namespace oceanbase
{

namespace table
{

OPAllocFunc ObTableGroupRegister::OP_ALLOC[TYPE_MAX] = { };
KEYAllocFunc ObTableGroupRegister::KEY_ALLOC[TYPE_MAX] = { };
VALAllocFunc ObTableGroupRegister::VAL_ALLOC[TYPE_MAX] = { };
METAAllocFunc ObTableGroupRegister::META_ALLOC[TYPE_MAX] = { };
PROCAllocFunc ObTableGroupRegister::PROC_ALLOC[TYPE_MAX] = { };
const char *ObTableGroupRegister::TYPE_NAMES[TYPE_MAX] = { };
lib::ObLabel ObTableGroupRegister::TYPE_LABELS[TYPE_MAX] = { };

#define REG_GROUP_OBJ_TYPE(TYPE, TYPE_NAME, OP, KEY, VALUE, META, PROC, LABEL) \
  do {                                                                         \
    if (OB_UNLIKELY(TYPE < TYPE_INVALID || TYPE >= TYPE_MAX)) {                \
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "invalid type", K(TYPE));       \
    } else {                                                                   \
      TYPE_NAMES[TYPE] = TYPE_NAME;                                            \
      TYPE_LABELS[TYPE] = LABEL;                                               \
      OP_ALLOC[TYPE] = GroupAllocHelper::alloc<ObITableOp, OP>;                \
      KEY_ALLOC[TYPE] = GroupAllocHelper::alloc<ObITableGroupKey, KEY>;        \
      VAL_ALLOC[TYPE] = GroupAllocHelper::alloc<ObITableGroupValue, VALUE>;    \
      META_ALLOC[TYPE] = GroupAllocHelper::alloc<ObITableGroupMeta, META>;     \
      PROC_ALLOC[TYPE] = GroupAllocHelper::alloc<ObITableOpProcessor, PROC>;   \
    }                                                                          \
  } while (0)

void ObTableGroupRegister::register_group_objs()
{
  memset(TYPE_NAMES, 0, sizeof(TYPE_NAMES));
  memset(TYPE_LABELS, 0, sizeof(TYPE_LABELS));
  memset(OP_ALLOC, 0, sizeof(OP_ALLOC));
  memset(KEY_ALLOC, 0, sizeof(KEY_ALLOC));
  memset(VAL_ALLOC, 0, sizeof(VAL_ALLOC));
  memset(META_ALLOC, 0, sizeof(META_ALLOC));
  memset(PROC_ALLOC, 0, sizeof(PROC_ALLOC));
  #define GROUP_OBJ_DEF(type, type_name, op, key, value, meta, processor) REG_GROUP_OBJ_TYPE(type, type_name, op, key, value, meta, processor, type_name);
  #include "observer/table/group/ob_table_group_register.h"
  #undef GROUP_OBJ_DEF
}

ObTableGroupType ObTableGroupRegister::get_op_type_by_name(const ObString &name)
{
  bool is_found = false;
  ObTableGroupType op_type = ObTableGroupType::TYPE_INVALID;
  for (uint32_t i = 1; i < ARRAYSIZEOF(TYPE_NAMES) && !is_found; i++) {
    if (static_cast<int32_t>(strlen(TYPE_NAMES[i])) == name.length()
        && strncasecmp(TYPE_NAMES[i], name.ptr(), name.length()) == 0) {
      op_type = static_cast<ObTableGroupType>(i);
      is_found = true;
    }
  }
  return op_type;
}

} // end namespace table
} // end namespace oceanbase
