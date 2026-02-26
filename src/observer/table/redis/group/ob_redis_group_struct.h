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

#pragma once
#include "observer/table/group/ob_i_table_struct.h"
#include "share/table/redis/ob_redis_common.h"
namespace oceanbase
{

namespace table
{
class ObRedisCmdKey : public ObITableGroupKey
{
public:
  ObRedisCmdKey()
      : ObITableGroupKey(ObTableGroupType::TYPE_REDIS_GROUP),
        cmd_type_(RedisCommandType::REDIS_COMMAND_INVALID),
        table_id_(OB_INVALID_ID)
  {}

  ObRedisCmdKey(const RedisCommandType cmd_type, ObTableID table_id)
      : ObITableGroupKey(ObTableGroupType::TYPE_REDIS_GROUP),
        cmd_type_(cmd_type),
        table_id_(table_id)
  {}
  virtual uint64_t hash() const override
  {
    uint64_t hash_val = 0;
    uint64_t seed = 0;
    hash_val = murmurhash(&table_id_, sizeof(table_id_), seed);
    hash_val = murmurhash(&cmd_type_, sizeof(cmd_type_), hash_val);
    return hash_val;
  }

  virtual int deep_copy(common::ObIAllocator &allocator, const ObITableGroupKey &other) override
  {
    int ret = OB_SUCCESS;
    type_ = other.type_;
    table_id_ = static_cast<const ObRedisCmdKey&>(other).table_id_;
    cmd_type_ = static_cast<const ObRedisCmdKey&>(other).cmd_type_;
    return ret;
  }

  virtual bool is_equal(const ObITableGroupKey &other) const override
  {
    return type_ == other.type_
      && cmd_type_ == static_cast<const ObRedisCmdKey &>(other).cmd_type_
      && table_id_ == static_cast<const ObRedisCmdKey &>(other).table_id_;
  }

  RedisCommandType cmd_type_;
  common::ObTableID table_id_;
};

}  // namespace table
}  // namespace oceanbase
