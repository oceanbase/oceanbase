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
  using RedisKeySet = hash::ObHashSet<RedisKeyNode, common::hash::NoPthreadDefendMode>;
  explicit HashCommandOperator(ObRedisCtx &redis_ctx) : CommandOperator(redis_ctx)
  {
    model_ = ObRedisModel::HASH;
  }
  virtual ~HashCommandOperator() = default;

  int do_hset(int64_t db, const ObString &key, const RedisCommand::FieldValMap &field_val_map);
  int do_hmset(int64_t db, const ObString &key, const RedisCommand::FieldValMap &field_val_map);
  int do_hsetnx(int64_t db, const ObString &key, const ObString &field, const ObString &new_value);
  int do_hexists(int64_t db, const ObString &key, const ObString &field);
  int do_hget(int64_t db, const ObString &key, const ObString &field);
  int do_hget_all(int64_t db, const ObString &key, bool need_fields, bool need_vals);
  int do_hlen(int64_t db, const ObString &key);
  int do_hmget(int64_t db, const ObString &key, const ObIArray<ObString> &fields);
  int do_hdel(int64_t db, const ObString &key, const HashCommand::FieldSet &field_set);
  int do_hincrby(int64_t db, const ObString &key, const ObString &field, int64_t increment);
  int do_hincrbyfloat(int64_t db, const ObString &key, const ObString &field, long double increment);

  // for group service
  int do_group_hget();
  int fill_set_batch_op(const ObRedisOp &op,
                        ObIArray<ObTabletID> &tablet_ids,
                        ObTableBatchOperation &batch_op) override;

  int do_group_hsetnx();
private:
  int do_hset_inner(int64_t db, const ObString &key, const RedisCommand::FieldValMap &field_val_map, int64_t &insert_num);
  int build_value_entity(int64_t db, const ObString &key, const ObString &field,
                         const ObString &value, int64_t insert_ts, ObITableEntity *&entity);
  int get_field_value(int64_t db, const ObString &key, const ObString &field, ObString &value, ObRedisMeta *meta = nullptr);
  int put_value_and_meta(int64_t db, const ObString &key, const ObString &field, const ObString &value, ObRedisMeta *meta);
  DISALLOW_COPY_AND_ASSIGN(HashCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_HASH_OPERATOR_
