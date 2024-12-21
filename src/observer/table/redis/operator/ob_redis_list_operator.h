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

#pragma once
#include "ob_redis_operator.h"
#include "lib/string/ob_string.h"
#include "src/observer/table/redis/cmd/ob_redis_list_cmd.h"
#include "src/observer/table/group/ob_i_table_struct.h"

namespace oceanbase
{
namespace table
{
class ListQueryCond
{
public:
  explicit ListQueryCond(
      const ObString &key,
      int64_t db,
      int64_t left_border,
      int64_t right_border,
      int64_t limit,
      int64_t offset,
      bool is_query_forward,
      bool need_query_all,
      bool include_start = true,
      bool include_end = true)
      : key_(key),
        db_(db),
        left_border_(left_border),
        right_border_(right_border),
        limit_(limit),
        offset_(offset),
        is_query_forward_(is_query_forward),
        need_query_all_(need_query_all),
        include_start_(include_start),
        include_end_(include_end)
  {}

  explicit ListQueryCond(
      const ObString &key,
      int64_t db,
      int64_t left_border,
      int64_t right_border)
      : key_(key),
        db_(db),
        left_border_(left_border),
        right_border_(right_border),
        limit_(-1),
        offset_(0),
        is_query_forward_(true),
        need_query_all_(true),
        include_start_(true),
        include_end_(true)
  {}
  TO_STRING_KV(
      K_(db),
      K_(key),
      K_(left_border),
      K_(right_border),
      K_(limit),
      K_(offset),
      K_(is_query_forward),
      K_(need_query_all),
      K_(include_start),
      K_(include_end));

public:
  const ObString &key_;
  int64_t db_;
  int64_t left_border_;
  int64_t right_border_;
  int64_t limit_;
  int64_t offset_;
  bool is_query_forward_;
  bool need_query_all_;
  bool include_start_;
  bool include_end_;

private:
  DISALLOW_COPY_AND_ASSIGN(ListQueryCond);
};

struct ListData {
  int64_t db_;
  int64_t index_;
  int64_t insert_ts_;
  ObString key_;
  ObString value_;
  TO_STRING_KV(K_(db), K_(index), K_(insert_ts), K_(key), K_(value));
};

class ListCommandOperator : public CommandOperator
{
public:
  using ListElement = std::pair<int64_t, ObObj>;
  using ListElemEntity = std::pair<int64_t, ObITableEntity*>;

public:
  explicit ListCommandOperator(ObRedisCtx &redis_ctx) : CommandOperator(redis_ctx)
  {
    model_ = ObRedisModel::LIST;
  }
  virtual ~ListCommandOperator() = default;
  int do_push(
      int64_t db,
      const common::ObString &key,
      const common::ObIArray<common::ObString> &values,
      bool push_left,
      bool need_exit);
  int do_pop(int64_t db, const common::ObString &key, bool pop_left);
  int do_index(int64_t db, const common::ObString &key, int64_t offset);
  int do_set(int64_t db, const common::ObString &key, int64_t offset, const common::ObString &value);
  int do_range(int64_t db, const common::ObString &key, int64_t start, int64_t end);
  int do_trim(int64_t db, const common::ObString &key, int64_t start, int64_t end);
  int do_insert(
      int64_t db,
      const common::ObString &key,
      bool is_before_pivot,
      const common::ObString &pivot,
      const common::ObString &value);
  int do_get_len(int64_t db, const common::ObString &key);
  int do_rem(int64_t db, const common::ObString &key, int64_t count, const common::ObString &value);
  int do_del(int64_t db, const common::ObString &key, bool &is_exist);
  // for group
  int do_group_push();
  int do_group_pop();
public:
  int gen_entity_with_rowkey(
      int64_t db,
      const ObString &key,
      int64_t idx,
      bool is_data,
      ObITableEntity *&entity,
      ObITableEntityFactory *entity_factory);

  int build_scan_range(const ListQueryCond &query_cond, bool is_data, ObTableQuery &query);
  int build_scan_range(
      int64_t db,
      const ObString &key,
      bool is_data,
      const int64_t *less_index,
      const int64_t *max_index,
      ObTableQuery &query);
  static int build_list_type_rowkey(
      ObIAllocator &allocator,
      int64_t db,
      const ObString &key,
      bool is_data,
      int64_t idx,
      common::ObRowkey &rowkey);
  int build_range_query(
      int64_t db,
      const ObString &key,
      const ObRedisListMeta &list_meta,
      int64_t start,
      int64_t end,
      ObTableQuery &query);

private:
  int put_data_to_table(
      int64_t db,
      const ObString &key,
      ObRedisListMeta &list_meta,
      const ObIArray<ObString> &values,
      bool push_left);
  int build_push_ops(
      ObTableBatchOperation &push_value_ops,
      int64_t db,
      const ObString &key,
      ObRedisListMeta &list_meta,
      int64_t cur_ts,
      const ObIArray<ObString> &values,
      bool push_left,
      bool need_push_meta = true);
  int do_same_keys_push(
      ObTableBatchOperation &batch_op,
      ObIArray<ObTabletID> &tablet_ids,
      ObIArray<ObITableOp *> &ops,
      const ObIArray<ObRedisMeta *> &metas,
      const ObString &key,
      int64_t same_key_start_pos,
      int64_t same_key_end_pos);
  int do_same_keys_pop(
      ObTableBatchOperation &batch_op_insup,
      ObTableBatchOperation &batch_op_del,
      ObIArray<ObTabletID> &tablet_insup_ids,
      ObIArray<ObTabletID> &tablet_del_ids,
      const ObIArray<ObRedisMeta *> &metas,
      const ObString &key,
      int64_t same_key_start_pos,
      int64_t same_key_end_pos);

  int do_same_keys_multi_pop(
      const ResultFixedArray &batch_res,
      ObTableBatchOperation &batch_op_insup,
      ObTableBatchOperation &batch_op_del,
      ObIArray<ObTabletID> &tablet_insup_ids,
      ObIArray<ObTabletID> &tablet_del_ids,
      const ObIArray<ObRedisMeta *> &metas,
      const ObString &key,
      int64_t same_key_start_pos,
      int64_t same_key_end_pos);
  OB_INLINE bool idx_overflowed(int64_t idx);
  int query_index_result(const ObTableQuery &query, ObString &res_value, const ObRedisListMeta *list_meta);
  int build_rowkey(int64_t db, const ObString &key, bool is_data, const int64_t *index, bool is_min, ObRowkey &rowkey);
  int pop_single_count_list(int64_t db, const ObString &key, ObRedisListMeta &list_meta, const bool pop_left, common::ObString &res_value);
  int pop_multi_count_list(
      int64_t db,
      const common::ObString &key,
      const bool pop_left,
      ObRedisListMeta &list_meta,
      common::ObString &res_value);
  int update_list_after_pop(
      int64_t db,
      const ObString &key,
      const ListElement &old_borded,
      const ListElement &new_borded,
      const bool pop_left,
      ObRedisListMeta &list_meta);
  int get_varbinary_by_deep(const ObObj &src_obj, common::ObString &res_value);
  int get_query_forward_offset(const int64_t count, int64_t offset, bool &is_query_forward, int64_t &query_offset);
  int query_set_index(const ObTableQuery &query, int64_t &res_idx, ObRedisListMeta *meta = nullptr);
  int adjust_range_param(const int64_t count, int64_t &start, int64_t &end);
  int query_range_result(const ObTableQuery &query, ObIArray<ObString> &res_values, ObRedisListMeta *meta = nullptr);
  int build_index_query(
      int64_t db,
      const common::ObString &key,
      const int64_t offset,
      bool is_query_value,
      const ObRedisListMeta &list_meta,
      ObTableQuery &query);
  int build_list_query(const ListQueryCond &query_cond, ObTableQuery &query);
  int trim_list(int64_t db, const ObString &key, ObRedisListMeta &list_meta, int64_t start, int64_t end);
  int adjust_trim_param(const int64_t count, int64_t &start, int64_t &end, bool& is_del_all);
  int build_trim_querys(
      int64_t db,
      const ObString &key,
      const ObRedisListMeta &list_meta,
      int64_t &start,
      int64_t &end,
      bool& is_del_all,
      ObIArray<ObTableQuery> &querys);
  int update_element(int64_t db, ObRedisMeta *meta, const ObString &key, int64_t index, const ObString &value);

  int build_trim_ops(
      const ObIArray<ObTableQuery> &querys,
      ObTableBatchOperation &del_ops,
      ObIArray<int64_t> &new_border_idxs,
      ObRedisListMeta *meta = nullptr);

  int update_meta_after_trim(
      int64_t db,
      const ObString &key,
      ObRedisListMeta &list_meta,
      const ObIArray<int64_t> &new_border_idxs,
      const int64_t start,
      const int64_t end);
  int build_rem_querys(
      int64_t db,
      const common::ObString &key,
      const int64_t count,
      const common::ObString &value,
      const ObRedisListMeta &list_meta,
      ObTableQuery &query);
  int update_meta_after_rem(
      int64_t db,
      const ObString &key,
      ObRedisListMeta &list_meta,
      const int64_t rem_count,
      int64_t del_leftmost_idx,
      int64_t del_rightmost_idx);

  int get_new_border_idxs(
      int64_t db,
      const ObString &key,
      bool need_update_left_idx,
      bool need_update_right_idx,
      ObRedisListMeta &list_meta,
      ObIArray<int64_t> &new_border_idxs);

  int get_pivot_index(
      const int64_t db,
      const ObString &key,
      const ObString &pivot,
      const ObRedisListMeta &list_meta,
      int64_t &pivot_inx);
  int get_insert_index(
      const int64_t db,
      const ObString &key,
      const int64_t pivot_inx,
      const bool is_before_pivot,
      ObRedisListMeta &list_meta,
      int64_t &insert_idx);
  int get_adjacent_index(
      const int64_t db,
      const ObString &key,
      const ObRedisListMeta &list_meta,
      const int64_t pivot_inx,
      const bool is_before_pivot,
      int64_t &adjacent_idx);
  int redict_idx(
      const int64_t db,
      const ObString &key,
      const int64_t pivot_inx,
      const bool is_before_pivot,
      ObRedisListMeta &list_meta,
      int64_t &insert_idx);
  int do_redict_idx(
      const int64_t db,
      const ObString &key,
      const int64_t pivot_idx,
      const int64_t left_bord_idx,
      const int64_t right_bord_idx,
      const int64_t index_step,
      const bool is_redict_left,
      const bool is_before_pivot,
      ObRedisListMeta &list_meta,
      int64_t &insert_idx);
  void update_border_after_redict(
      const int64_t pivot_bord_idx,
      const int64_t row_cnt,
      const int64_t index_step,
      const bool is_redict_left,
      ObRedisListMeta &list_meta);
  int list_entity_set_value(const ObString &value, const int64_t insert_ts, ObITableEntity *value_entity);
  int build_del_ops(const ObTableQuery &query, ObTableBatchOperation &del_ops, ObRedisListMeta *meta = nullptr);
  int build_list_type_rowkey(int64_t db, const ObString &key, bool is_data, int64_t idx, common::ObRowkey &rowkey);
  int gen_entity_from_other(
      common::ObIAllocator &alloc,
      const ObITableEntity &src_entity,
      ObITableEntity &dst_entity);
  int gen_entity_from_other(const ObITableEntity &src_entity, ObITableEntity &dst_entity);
  int query_count(
      const int64_t db,
      const ObString &key,
      const ObRedisListMeta &list_meta,
      const int64_t left_inx,
      const int64_t right_inx,
      int64_t &row_cnt);
  int del_key(const ObString &key, int64_t db, ObRedisListMeta *list_meta);
  int64_t get_region_left_bord_idx(int64_t pivot_idx);
  int64_t get_region_right_bord_idx(int64_t pivot_idx);
  void sort_group_ops();
  int deep_copy_list_entity(
      const ObITableEntity *result_entity,
      ObITableEntity *&result_copy_entity,
      ListData &list_data);
  int copy_list_entity(const ObITableEntity *result_entity, ObITableEntity *&result_copy_entity, ListData &list_data);
  int gen_group_pop_res(
      const ObIArray<ListElemEntity> &res_idx_entitys,
      ObRedisListMeta *list_meta,
      int64_t same_key_start_pos,
      int64_t same_key_end_pos,
      ObTableBatchOperation &batch_op_insup,
      ObTableBatchOperation &batch_op_del,
      ObIArray<ObTabletID> &tablet_insup_ids,
      ObIArray<ObTabletID> &tablet_del_ids);

  int gen_same_key_batch_get_op(
      const ObRedisListMeta &meta,
      const ObRedisOp &redis_op,
      ObString key,
      int64_t same_key_start_pos,
      int64_t same_key_end_pos,
      ObTableBatchOperation &batch_op_get,
      ObIArray<ObTabletID> &tablet_get_ids);

  int check_can_use_multi_get(
      const ObArray<ObRedisMeta *> &metas,
      ObIArray<ObITableOp *> &ops,
      ObTableBatchOperation &batch_op_get,
      ObIArray<ObTabletID> &tablet_get_ids,
      bool &can_use_multi_get);

  int do_group_pop_use_multi_get(
      const ObArray<ObRedisMeta *> &metas,
      const ObTableBatchOperation &batch_op_get,
      ObIArray<ObTabletID> &tablet_get_ids,
      ObTableBatchOperation &batch_op_insup,
      ObIArray<ObTabletID> &tablet_insup_ids,
      ObTableBatchOperation &batch_op_del,
      ObIArray<ObTabletID> &tablet_del_ids);

  int do_group_pop_use_query(
      const ObArray<ObRedisMeta *> &metas,
      const ObTableBatchOperation &batch_op_get,
      ObIArray<ObTabletID> &tablet_get_ids,
      ObTableBatchOperation &batch_op_insup,
      ObIArray<ObTabletID> &tablet_insup_ids,
      ObTableBatchOperation &batch_op_del,
      ObIArray<ObTabletID> &tablet_del_ids);

  int update_meta_after_multi_pop(
      ObRedisListMeta *list_meta,
      ObTableBatchOperation &batch_op_insup,
      ObTableBatchOperation &batch_op_del,
      ObIArray<ObTabletID> &tablet_insup_ids,
      ObIArray<ObTabletID> &tablet_del_ids,
      int64_t same_key_start_pos,
      int64_t same_key_end_pos);

  int ops_return_nullstr(ObIArray<ObITableOp *> &ops, int64_t same_key_start_pos, int64_t same_key_end_pos);

  void format_ins_region(ObRedisListMeta& list_meta);
  void update_ins_region_after_pop(bool pop_left, ObRedisListMeta &list_meta);
  void update_ins_region_after_trim(ObRedisListMeta &list_meta);
  void updata_ins_region_after_insert(
      bool is_before_pivot,
      int64_t adjacent_idx,
      int64_t pivot_idx,
      ObRedisListMeta &list_meta);
  void update_ins_region_after_rdct(bool is_redict_left, int64_t pivot_bord_idx, ObRedisListMeta &list_meta);
  void after_ins_region_after_rem(int64_t del_leftmost_idx, int64_t del_rightmost_idx, ObRedisListMeta &list_meta);
  static bool compare_ob_redis_ops(ObITableOp *&op_a, ObITableOp *&op_b);

private:
  static const int64_t REDICT_MEM_LIMIT = 100 * 1024 * 1024;  // 100M
  DISALLOW_COPY_AND_ASSIGN(ListCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
