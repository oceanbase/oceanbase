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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_SET_OPERATOR_
#define OCEANBASE_OBSERVER_OB_REDIS_SET_OPERATOR_
#include "ob_redis_operator.h"
#include "lib/string/ob_string.h"
#include "observer/table/redis/cmd/ob_redis_set_cmd.h"

namespace oceanbase
{
namespace table
{
class SetCommandOperator : public CommandOperator
{
public:
  explicit SetCommandOperator(ObRedisCtx &redis_ctx) : CommandOperator(redis_ctx)
  {}
  virtual ~SetCommandOperator() = default;
  int do_aggregate(int64_t db, const ObIArray<ObString> &keys, SetCommand::AggFunc agg_func);
  int do_sadd(int64_t db, const ObString &key, const SetCommand::MemberSet &members);
  int do_aggregate_store(int64_t db, const ObString &dest, const ObIArray<ObString> &keys,
                         SetCommand::AggFunc agg_func);

protected:
  static const int64_t MEMBER_IDX = 2;
  static const ObString MEMBER_PROPERTY_NAME;

  int add_member_scan_range(int64_t db, const ObString &key, bool is_data, ObTableQuery &query);
  int build_range(const common::ObRowkey &start_key, const common::ObRowkey &end_key,
                  ObNewRange *&range, bool inclusive_start = true, bool inclusive_end = true);
  int is_member_in_one_key(int64_t db, const ObIArray<ObString> &keys, int start_idx, int end_idx,
                           const ObString &member_obj, bool &is_member);
  int is_member_in_all_keys(int64_t db, const ObIArray<ObString> &keys, int start_idx, int end_idx,
                            const ObString &member_obj, bool &is_member);
  int is_key_member(int64_t db, const ObString &key, const ObString &member_obj, bool &is_member);
  int do_sadd_inner(int64_t db, const ObString &key, const SetCommand::MemberSet &members,
                    int64_t &insert_num);
  int do_aggregate_inner(int64_t db, const ObIArray<ObString> &keys, SetCommand::AggFunc agg_func,
                         SetCommand::MemberSet &members);
  int delete_set(int db, const ObString &key);
  int get_member_from_entity(ObIAllocator &allocator, const ObITableEntity &entity,
                             ObString &member_str);
  int agg_need_push_member(int64_t db, const ObIArray<ObString> &keys, const ObString &member,
                           SetCommand::AggFunc agg_func, bool &is_needed);
  int do_union(int64_t db, const ObString &key, SetCommand::MemberSet &members);

private:
  DISALLOW_COPY_AND_ASSIGN(SetCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_SET_OPERATOR_
