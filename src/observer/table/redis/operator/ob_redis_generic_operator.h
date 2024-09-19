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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_GENERIC_OPERATOR_
#define OCEANBASE_OBSERVER_OB_REDIS_GENERIC_OPERATOR_
#include "ob_redis_operator.h"
#include "lib/string/ob_string.h"
#include "src/observer/table/redis/cmd/ob_redis_generic_cmd.h"

namespace oceanbase
{
namespace table
{
class GenericCommandOperator : public CommandOperator
{
public:
  enum ExpireStatus {
    NOT_EXISTS, // expired or not exists
    PERSIST, // do not set expire ts
    NOT_EXPIRED, // set expire ts but not expired yet
    INVALID
  };
  explicit GenericCommandOperator(ObRedisCtx &redis_ctx) : CommandOperator(redis_ctx)
  {}
  virtual ~GenericCommandOperator() = default;

  int do_expire_at(int64_t db, const ObString &key, int64_t expire_ts, int64_t conv_unit);
  int do_expire(int64_t db, const ObString &key, int64_t expire_diff, int64_t conv_unit);
  int do_ttl(int64_t db, const ObString &key, int64_t conv_unit);
  int do_exists(int64_t db, const common::ObIArray<common::ObString> &keys);
  int do_del(int64_t db, const common::ObIArray<common::ObString> &keys);
  int do_type(int64_t db, const ObString &key);
  int do_persist(int64_t db, const ObString &key);

private:
  int do_model_ttl(int64_t db, const ObString &key, ObRedisModel model, int64_t conv_unit,
                   int64_t &status);
  int update_model_expire(int64_t db, const ObString &key, int64_t expire_ts,
                          ObRedisModel model, ExpireStatus &expire_status);
  int is_key_exists(int64_t db, const ObString &key, ObRedisModel model, bool &exists);
  int del_key(int64_t db, const ObString &key, ObRedisModel model, bool &exists);
  int do_expire_at_us(int64_t db, const ObString &key, int64_t expire_ts);
  int get_subkey_count_by_meta(
    int64_t db,
    const ObString &key,
    const ObRedisMeta *meta,
    ObRedisModel model,
    int64_t &row_cnt);
  DISALLOW_COPY_AND_ASSIGN(GenericCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_GENERIC_OPERATOR_
