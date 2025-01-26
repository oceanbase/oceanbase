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
struct SrandResult {
  SrandResult(ObIAllocator &allocator)
      : is_get_all_(false),
        res_members_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisSPop")),
        res_insert_ts_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisInsTs"))
  {}
  TO_STRING_KV(K_(is_get_all), K_(res_members), K_(res_insert_ts));
  bool is_get_all_;
  ObArray<ObString> res_members_;
  ObArray<int64_t> res_insert_ts_;
};

class SetCommandOperator : public CommandOperator
{
public:
  explicit SetCommandOperator(ObRedisCtx &redis_ctx) : CommandOperator(redis_ctx), is_zset_(false)
  {
    model_ = ObRedisModel::SET;
  }
  virtual ~SetCommandOperator() = default;
  int do_aggregate(int64_t db, const ObIArray<ObString> &keys, SetCommand::AggFunc agg_func);
  int do_sadd(int64_t db, const ObString &key, const SetCommand::MemberSet &members);
  int do_scard(int64_t db, const ObString &key);
  int do_sismember(int64_t db, const ObString &key, const ObString &member);
  int do_smembers(int64_t db, const ObString &key);
  int do_smove(int64_t db, const ObString &src, const ObString &dest, const ObString &member);
  int do_aggregate_store(
      int64_t db,
      const ObString &dest,
      const ObIArray<ObString> &keys,
      SetCommand::AggFunc agg_func);
  int do_spop(int64_t db, const common::ObString &key, const common::ObString &count_str);
  int do_srand_member(int64_t db, const common::ObString &key, const common::ObString &count_str);
  int do_srem(int64_t db, const common::ObString &key, const SetCommand::MemberSet &members);
  int do_srem(int64_t db, const ObString &key, const common::ObIArray<ObString> &members);
  int fill_set_batch_op(const ObRedisOp &op,
                        ObIArray<ObTabletID> &tablet_ids,
                        ObTableBatchOperation &batch_op) override;

protected:
  int is_member_in_one_key(
      int64_t db,
      const ObIArray<ObString> &keys,
      int start_idx,
      int end_idx,
      const ObString &member_obj,
      bool &is_member);
  int is_member_in_all_keys(
      int64_t db,
      const ObIArray<ObString> &keys,
      int start_idx,
      int end_idx,
      const ObString &member_obj,
      bool &is_member);
  int is_key_member(int64_t db, const ObString &key, const ObString &member_obj, bool &is_member);
  int insert_single_data(int64_t db, const ObString &key, const ObString &member, bool &is_duplicated);
  int do_sadd_inner(int64_t db, const ObString &key, const SetCommand::MemberSet &members,
                    int64_t &insert_num);
  int do_sadd_data(int64_t db, const ObString &key, const SetCommand::MemberSet &members,
                    bool is_new_meta, int64_t &insert_num);
  int do_aggregate_inner(int64_t db, const ObIArray<ObString> &keys, SetCommand::AggFunc agg_func,
                         SetCommand::MemberSet &members);
  int agg_need_push_member(
      int64_t db,
      const ObIArray<ObString> &keys,
      const ObString &member,
      SetCommand::AggFunc agg_func,
      bool &is_needed);
  int do_union(int64_t db, const ObString &key, SetCommand::MemberSet &members);
  int do_srem_inner(
      int64_t db,
      const common::ObString &key,
      const SetCommand::MemberSet &members,
      int64_t &del_num);

  int do_srem_inner(
      int64_t db,
      const common::ObString &key,
      const common::ObIArray<ObString> &members,
      int64_t &del_num);
  int do_smembers_inner(int64_t db, const common::ObString &key, bool get_insert_ts, SrandResult &srand_result);
  int do_srand_mem_inner(
      int64_t db,
      const common::ObString &key,
      bool get_insert_ts,
      int64_t count,
      SrandResult &srand_result);
  int do_srand_mem_repeat_inner(int64_t db, const common::ObString &key, int64_t count, SrandResult &srand_result);
  int query_member_at_indexes(
      bool get_insert_ts,
      const ObTableQuery &query,
      const ObArray<int64_t> &target_idxs,
      SrandResult &srand_result);
  int del_member_after_spop(
      int64_t db,
      const common::ObString &key,
      const ObArray<ObString> &members,
      const ObArray<int64_t> &res_insert_ts);
  int hashset_to_array(const SetCommand::MemberSet &hash_set,
                       ObIArray<ObString> &ret_arr);

protected:
  bool is_zset_;

private:
  DISALLOW_COPY_AND_ASSIGN(SetCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_SET_OPERATOR_
