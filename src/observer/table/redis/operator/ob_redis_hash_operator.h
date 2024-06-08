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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_HASH_OPERATOR_
#define OCEANBASE_OBSERVER_OB_REDIS_HASH_OPERATOR_
#include "ob_redis_operator.h"
#include "lib/string/ob_string.h"
#include "src/observer/table/redis/cmd/ob_redis_hash_cmd.h"

namespace oceanbase
{
namespace table
{
class HashCommandOperator : public CommandOperator
{
public:
  explicit HashCommandOperator(ObRedisCtx &redis_ctx) : CommandOperator(redis_ctx)
  {}
  virtual ~HashCommandOperator() = default;

  int do_hset(int64_t db, const ObString &key, const HashCommand::FieldValMap &field_val_map);
  int do_hmset(int64_t db, const ObString &key, const HashCommand::FieldValMap &field_val_map);

private:
  static const ObString VALUE_PROPERTY_NAME;
  static const ObString FIELD_PROPERTY_NAME;

  int do_hset_inner(int64_t db, const ObString &key, const HashCommand::FieldValMap &field_val_map, int64_t &insert_num);
  int build_value_entity(int64_t db, const ObString &key, const ObString &field,
                         const ObString &value, ObITableEntity *&entity);
  DISALLOW_COPY_AND_ASSIGN(HashCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_HASH_OPERATOR_
