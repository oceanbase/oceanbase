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

namespace oceanbase
{
namespace table
{

class ListMetaData
{
public:
  ListMetaData() : count_(0), left_idx_(INIT_INDEX), right_idx_(INIT_INDEX), ttl_(0)
  {}
  ~ListMetaData()
  {}
  TO_STRING_KV(K_(count), K_(left_idx), K_(right_idx), K_(ttl));
  void reset()
  {
    count_ = 0;
    left_idx_ = INIT_INDEX;
    right_idx_ = INIT_INDEX;
    ttl_ = 0;
  }

  int get_meta_from_entity(const ObITableEntity &meta_entity);
  int put_meta_into_entity(ObIAllocator &allocator, ObITableEntity &meta_entity);
  int gen_meta_entity(ObITableEntityFactory *entity_factory, const ObRowkey &row_key, ObITableEntity &meta_entity);
  bool is_expired();

private:
  // meta_value: count,left_idx,right_idx
  int decode_meta_value(const ObObj &meta_value_obj);
  int encode_meta_value(ObIAllocator &allocator, ObObj &meta_value_obj);
  OB_INLINE void get_ttl_obj(ObObj &ttl_obj)
  {
    ttl_obj.set_timestamp(ttl_);
  }

public:
  static const int64_t INIT_INDEX = 0;
  static const int64_t INDEX_STEP = 1 << 20;
  static const int64_t META_INDEX = INT64_MIN;
  static const int64_t INDEX_SEQNUM_IN_ROWKEY = 2;
  static const char META_SPLIT_FLAG = ObRedisUtil::INT_FLAG;

  int64_t count_;
  int64_t left_idx_;
  int64_t right_idx_;
  int64_t ttl_;
};

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
      bool need_query_value)
      : key_(key),
        db_(db),
        left_border_(left_border),
        right_border_(right_border),
        limit_(limit),
        offset_(offset),
        is_query_forward_(is_query_forward),
        need_query_value_(need_query_value)
  {}
  TO_STRING_KV(
      K_(db),
      K_(key),
      K_(left_border),
      K_(right_border),
      K_(limit),
      K_(offset),
      K_(is_query_forward),
      K_(need_query_value));

public:
  const ObString &key_;
  int64_t db_;
  int64_t left_border_;
  int64_t right_border_;
  int64_t limit_;
  int64_t offset_;
  bool is_query_forward_;
  bool need_query_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(ListQueryCond);
};

class ListCommandOperator : public CommandOperator
{
public:
  using ListElement = std::pair<int64_t, ObObj>;

public:
  explicit ListCommandOperator(ObRedisCtx &redis_ctx) : CommandOperator(redis_ctx), have_redis_err_(false)
  {}
  virtual ~ListCommandOperator() = default;
  int do_push(
      const common::ObString &key,
      const common::ObIArray<common::ObString> &values,
      bool push_left,
      bool need_exit);
  int do_pop(const common::ObString &key, bool pop_left);
  int do_index(const common::ObString &key, int64_t offset);
  int do_set(const common::ObString &key, int64_t offset, const common::ObString &value);
  int do_range(const common::ObString &key, int64_t start, int64_t end);
  int do_trim(const common::ObString &key, int64_t start, int64_t end);
  int do_insert(
      const common::ObString &key,
      bool is_before_pivot,
      const common::ObString &pivot,
      const common::ObString &value);
  int do_get_len(const common::ObString &key);
  int do_rem(const common::ObString &key, int64_t count, const common::ObString &value);
  int do_del(const common::ObString &key);

private:
  int get_list_meta(ListMetaData &list_meta);
  int put_data_to_table(ListMetaData &list_meta, const ObIArray<ObString> &values, bool push_left);
  int delete_list_by_key(const ObString &key);
  OB_INLINE bool idx_overflowed(int64_t idx);
  // gen err_msg which need return to redis client
  void gen_fmt_redis_err(const common::ObString &err_msg);
  int build_index_scan_range(
      int64_t db,
      const ObString &key,
      const int64_t *less_index,
      const int64_t *max_index,
      ObTableQuery &query);
  int query_index_result(const ObTableQuery &query, ObString &res_value);
  int build_rowkey(int64_t db, const ObString &key, const int64_t *index, bool is_min, ObRowkey &rowkey);
  int gen_entity_with_rowkey(int64_t idx, ObITableEntity *&entity);
  int pop_single_count_list(ListMetaData &list_meta, const bool pop_left, common::ObString &res_value);
  int pop_multi_count_list(
      const common::ObString &key,
      const bool pop_left,
      ListMetaData &list_meta,
      common::ObString &res_value);
  int update_list_after_pop(
      const ListElement &old_borded,
      const ListElement &new_borded,
      const bool pop_left,
      ListMetaData &list_meta);
  int get_res_value_by_deep(const ObObj &src_obj, common::ObString &res_value);
  int get_query_forward_offset(const int64_t count, int64_t offset, bool &is_query_forward, int64_t &query_offset);
  int query_set_index(const ObTableQuery &query, int64_t &res_idx);
  int adjust_range_param(const int64_t count, int64_t &start, int64_t &end);
  int query_range_result(const ObTableQuery &query, ObIArray<ObString> &res_values);
  int do_list_expire_if_needed(const common::ObString &key, ListMetaData &list_meta);
  int build_index_query(
      const common::ObString &key,
      const int64_t offset,
      bool is_query_value,
      const ListMetaData &list_meta,
      ObTableQuery &query);
  int build_range_query(
      const ObString &key,
      const ListMetaData &list_meta,
      int64_t start,
      int64_t end,
      ObTableQuery &query);
  int build_list_query(const ListQueryCond &query_cond, ObTableQuery &query);
  int trim_list(const ObString &key, ListMetaData &list_meta, int64_t start, int64_t end);
  int adjust_trim_param(const int64_t count, int64_t &start, int64_t &end);
  int build_trim_querys(
      const ObString &key,
      const ListMetaData &list_meta,
      int64_t &start,
      int64_t &end,
      ObIArray<ObTableQuery> &querys);
  int update_element(int64_t index, const ObString &value);

  int build_trim_ops(
      const ObIArray<ObTableQuery> &querys,
      ObTableBatchOperation &del_ops,
      ObIArray<int64_t> &new_border_idxs);

  int update_meta_after_trim(
      ListMetaData &list_meta,
      const ObIArray<int64_t> &new_border_idxs,
      const int64_t start,
      const int64_t end);
  int build_rem_querys(
      const common::ObString &key,
      const int64_t count,
      const common::ObString &value,
      const ListMetaData &list_meta,
      ObTableQuery &query);
  int update_meta_after_rem(
      const ObString &key,
      ListMetaData &list_meta,
      const int64_t rem_count,
      bool need_update_left_idx,
      bool need_update_right_idx);

  int get_new_border_idxs(
      const ObString &key,
      bool need_update_left_idx,
      bool need_update_right_idx,
      ListMetaData &list_meta,
      ObIArray<int64_t> &new_border_idxs);

  int get_pivot_index(
      const int64_t db,
      const ObString &key,
      const ObString &pivot,
      const ListMetaData &list_meta,
      int64_t &pivot_inx);
  int get_insert_index(
      const int64_t db,
      const ObString &key,
      const ListMetaData &list_meta,
      const int64_t pivot_inx,
      const bool is_before_pivot,
      int64_t &insert_idx);
  int get_adjacent_index(
      const int64_t db,
      const ObString &key,
      const ListMetaData &list_meta,
      const int64_t pivot_inx,
      const bool is_before_pivot,
      int64_t &adjacent_idx);
  int build_del_ops(const ObTableQuery &query, ObTableBatchOperation &del_ops);

private:
  // flag whether there is err_msg that need to be returned to redis cient
  bool have_redis_err_;
  // fmt_err_msg that need to be returned to redis cient
  common::ObString fmt_redis_err_msg_;
  DISALLOW_COPY_AND_ASSIGN(ListCommandOperator);
};

}  // namespace table
}  // namespace oceanbase
