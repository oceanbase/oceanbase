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
  explicit StringCommandOperator(ObRedisCtx &redis_ctx) :
    CommandOperator(redis_ctx)
  {
    model_ = ObRedisModel::STRING;
  }
  virtual ~StringCommandOperator() = default;
  int is_key_exists(int64_t db, const ObString &key, bool &exists);
  int do_append(int64_t db, const common::ObString &key, const common::ObString &val);
  int do_bit_count(
      int64_t db,
      const common::ObString &key,
      const common::ObString &strat_str,
      const common::ObString &end_str);
  int do_get(int64_t db, const common::ObString &key);
  int do_get_bit(int64_t db, const common::ObString &key, const common::ObString &offset_str);
  int do_get_range(
      int64_t db,
      const common::ObString &key,
      const common::ObString &start_str,
      const common::ObString &end_str);
  int do_incr_by_float(int64_t db, const common::ObString &key, const common::ObString &incr_str);
  int do_mget(int64_t db, const common::ObIArray<common::ObString> &keys);
  int do_mset(int64_t db, const RedisCommand::FieldValMap &field_val_map);
  int do_set(int64_t db, const common::ObString &key, const SetArg &set_arg);
  int do_setrange(
      int64_t db,
      const common::ObString &key,
      const common::ObString &offset_str,
      const common::ObString &value);
  int do_strlen(int64_t db, const common::ObString &key);
  int do_get_set(int64_t db, const common::ObString &key, const common::ObString &new_value);
  int do_set_bit(int64_t db, const common::ObString &key, int32_t offset, char value);
  int do_incrby(int64_t db, const common::ObString &key, const common::ObString &incr, bool is_incr);
  int get_value(int64_t db, const common::ObString &key, common::ObString &value, int64_t &expire_ts, bool& is_exist);
  int del_key(int64_t db, const common::ObString &key, bool& is_exist);
  // for group
  int do_group_get();
  int do_group_set();
  int do_group_incr();
  int do_group_setnx();
  int do_group_incrbyfloat();
  int do_group_append();

  int do_group_analyze(int (StringCommandOperator::*analyze_func)(ObRedisOp *op, ObTableOperationResult &),
                       StringCommandOperator *obj);
  int analyze_bitcount(ObRedisOp *op, ObTableOperationResult &res);
  int analyze_getbit(ObRedisOp *op, ObTableOperationResult &res);
  int analyze_getrange(ObRedisOp *op, ObTableOperationResult &res);
  int analyze_strlen(ObRedisOp *op, ObTableOperationResult &res);

private:
  int build_rowkey_entity(int64_t db, const common::ObString &key, ObITableEntity *&entity);
  int build_key_value_expire_entity(
      int64_t db,
      const common::ObString &key,
      const common::ObString &value,
      int64_t expire_tus,
      ObITableEntity *&entity);

  int build_key_value_entity(
      int64_t db,
      const common::ObString &key,
      const common::ObString &value,
      ObITableEntity *&entity);
  int get_value_from_entity(const ObITableEntity &entity, common::ObString &value);
  int set_bit(int32_t offset, char bit_value, char *new_value, char &old_bit_value);
  int get_expire_from_entity(const ObITableEntity &entity, int64_t &expire_ts);
  int get_bit_at_pos(common::ObString &value, int64_t offset, int64_t &res_bit);
  int get_range_value(const common::ObString &value, int64_t start, int64_t end, common::ObString &res_val);
  int set_range(
      const common::ObString &old_val,
      int64_t offset,
      const common::ObString &value,
      common::ObString &new_val);
  int convert_incr_str_to_int(const common::ObString &incr, bool is_incr, int64_t &incr_num);

  // for group
  int inner_do_group_get(const ObString &prop_name, ResultFixedArray &batch_res);

private:
  const static int64_t MAX_RANGE = (2 << 9) - 1;  // 1M -1
  DISALLOW_COPY_AND_ASSIGN(StringCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_STRING_OPERATOR_
