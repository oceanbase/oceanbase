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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_STRING_OPERATOR_
#define OCEANBASE_OBSERVER_OB_REDIS_STRING_OPERATOR_
#include "ob_redis_operator.h"
#include "lib/string/ob_string.h"
#include "src/observer/table/redis/cmd/ob_redis_string_cmd.h"

namespace oceanbase
{
namespace table
{
class StringCommandOperator : public CommandOperator
{
public:
  explicit StringCommandOperator(ObRedisCtx &redis_ctx) : CommandOperator(redis_ctx)
  {}
  virtual ~StringCommandOperator() = default;

  int do_get_set(int64_t db, const ObString &key, const ObString &new_value);
  int do_set_bit(int64_t db, const ObString &key, int32_t offset, char value);
  int do_incrby(int64_t db, const ObString &key, const ObString &incr, bool is_incr);

private:
  static const ObString VALUE_PROPERTY_NAME;
  const int64_t STRING_ROWKEY_SIZE = 2;

  int build_rowkey_entity(int64_t db, const ObString &key, ObITableEntity *&entity);
  int build_key_value_entity(int64_t db, const ObString &key, const ObString &value,
                             ObITableEntity *&entity);
  int get_value_from_entity(const ObITableEntity &entity, ObString &value);
  int get_value(int64_t db, const ObString &key, ObString &value, int64_t &expire_ts);
  int set_bit(int32_t offset, char bit_value, char* new_value, char &old_bit_value);
  int get_expire_from_entity(const ObITableEntity &entity, int64_t &expire_ts);
  bool is_incr_out_of_range(int64_t old_val, int64_t incr);
  DISALLOW_COPY_AND_ASSIGN(StringCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_STRING_OPERATOR_
