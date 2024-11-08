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

#ifndef OCEANBASE_OBSERVER_OB_REDIS_ZSET_OPERATOR_
#define OCEANBASE_OBSERVER_OB_REDIS_ZSET_OPERATOR_
#include "lib/string/ob_string.h"
#include "src/observer/table/redis/cmd/ob_redis_zset_cmd.h"
#include "src/observer/table/redis/operator/ob_redis_set_operator.h"

namespace oceanbase
{
namespace table
{
class ZSetScoreUpdater
{
public:
  explicit ZSetScoreUpdater(double new_score, double weight, ZSetAggCommand::AggType agg_type)
      : new_score_(new_score), weight_(weight), agg_type_(agg_type)
  {}
  virtual ~ZSetScoreUpdater()
  {}
  void operator()(common::hash::HashMapPair<common::ObString, double> &entry);
  static int aggregate_scores(ZSetAggCommand::AggType agg_type, double old_score, double new_score,
                              double new_score_weight, double &res_score);

private:
  double new_score_;
  double weight_;
  ZSetAggCommand::AggType agg_type_;
  DISALLOW_COPY_AND_ASSIGN(ZSetScoreUpdater);
};

class ZSetCommandOperator : public SetCommandOperator
{
public:
  explicit ZSetCommandOperator(ObRedisCtx &redis_ctx) : SetCommandOperator(redis_ctx)
  {
    model_ = ObRedisModel::ZSET;
    is_zset_ = true;
  }
  virtual ~ZSetCommandOperator() = default;
  int do_zadd(int64_t db, const ObString &key, const ZSetCommand::MemberScoreMap &mem_score_map);
  int do_zadd_data(int64_t db, const ObString &key, const ZSetCommand::MemberScoreMap &mem_score_map, bool is_new_meta);
  int do_zcard(int64_t db, const ObString &key);
  int do_zincrby(int64_t db, const ObString &key, const ObString &member, double increment);
  int do_zscore(int64_t db, const ObString &key, const ObString &member);
  int do_zrank(int64_t db, const ObString &member, ZRangeCtx &zrange_ctx);
  int do_zrange(int64_t db, const ZRangeCtx &zrange_ctx);
  int do_zrem_range_by_rank(int64_t db, const ZRangeCtx &zrange_ctx);
  int do_zrange_by_score(int64_t db, const ZRangeCtx &zrange_ctx);
  int do_zrem_range_by_score(int64_t db, const ZRangeCtx &zrange_ctx);
  int do_zcount(int64_t db, const ObString &key, double min, bool min_inclusive, double max,
                bool max_inclusive);
  int do_zunion_store(int64_t db, const ObString &dest, const ObIArray<ObString> &keys,
                      const ObIArray<double> &weights, ZSetAggCommand::AggType agg_type);

  int do_zinter_store(int64_t db, const ObString &dest, const ObIArray<ObString> &keys,
                      const ObIArray<double> &weights, ZSetAggCommand::AggType agg_type);
  int fill_set_batch_op(const ObRedisOp &op,
                        ObIArray<ObTabletID> &tablet_ids,
                        ObTableBatchOperation &batch_op) override;
  int do_group_zscore();

private:
  static const ObString SCORE_INDEX_NAME;
  static const int64_t SCORE_INDEX_COL_NUM = 3;

  int build_score_entity(int64_t db, const ObString &key, const ObString &member, double score,
                         int64_t time, ObITableEntity *&entity);
  int do_zscore_inner(int64_t db, const ObString &key, const ObString &member, double &score);
  int do_zcard_inner(int64_t db, const ObString &key, int64_t &card);
  int do_zrange_inner(int64_t db, const ZRangeCtx &zrange_ctx, ObIArray<ObString> &ret_arr);
  int get_score_str_from_entity(ObIAllocator &allocator, const ObITableEntity &entity,
                                ObString &score_str);
  int build_rank_scan_query(ObIAllocator &allocator, int64_t start, int64_t end,
                            const ZRangeCtx &zrange_ctx, ObTableQuery &query);

  int build_score_scan_query(ObIAllocator &allocator, const ZRangeCtx &zrange_ctx,
                             ObTableQuery &query);
  int do_zrange_by_score_inner(int64_t db, const ZRangeCtx &zrange_ctx,
                               ObIArray<ObString> &ret_arr);
  int union_zset(int64_t db, const ObString &key, double weight, ZSetAggCommand::AggType agg_type,
                 ZSetCommand::MemberScoreMap &ms_map);
  int get_score_from_entity(const ObITableEntity &entity, double &score);

  int exec_member_score_query(const ObTableQuery &query, bool with_scores,
                              ObIArray<ObString> &ret_arr,
                              ObRedisZSetMeta *meta = nullptr);
  int aggregate_scores_in_all_keys(int64_t db, const ObIArray<ObString> &keys,
                                   const ObIArray<double> &weights,
                                   ZSetAggCommand::AggType agg_type, int start_idx, int end_idx,
                                   const ObString &member, bool &is_member, double &res_score);
  int get_rank_in_same_score(
    int64_t db,
    ZRangeCtx &zrange_ctx,
    const ObString &member,
    int64_t score,
    ObRedisZSetMeta *set_meta,
    int64_t &rank);
  DISALLOW_COPY_AND_ASSIGN(ZSetCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_REDIS_ZSET_OPERATOR_
