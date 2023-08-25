/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_INDEX_INFO_CACHE_H
#define OCEANBASE_SQL_OPTIMIZER_OB_INDEX_INFO_CACHE_H 1

#include "sql/rewrite/ob_query_range.h"
#include "sql/ob_sql_define.h"

namespace oceanbase
{
namespace sql
{

/* 缓存query range的信息 */
class QueryRangeInfo {
public:
  QueryRangeInfo() : is_valid_(false),
      contain_always_false_(false),
      query_range_(NULL),
      ranges_(),
      ss_ranges_(),
      equal_prefix_count_(0),
      equal_prefix_null_count_(0),
      range_prefix_count_(0),
      index_column_count_(0),
      range_columns_(),
      expr_constraints_() {}
  const ObQueryRange* get_query_range() const { return query_range_; }
  const ObQueryRangeArray& get_ranges() const { return ranges_; }
  const common::ObIArray<ColumnItem> &get_range_columns() const { return range_columns_; }
  const common::ObIArray<ObExprConstraint> &get_expr_constraints() const
  {
    return expr_constraints_;
  }
  uint64_t get_equal_prefix_count() const { return equal_prefix_count_; };
  uint64_t get_equal_prefix_null_count() const { return equal_prefix_null_count_; };
  uint64_t get_range_prefix_count() const { return range_prefix_count_; };
  uint64_t get_index_column_count() const { return index_column_count_; };
  bool get_contain_always_false() const { return contain_always_false_; }
  
  ObQueryRangeArray& get_ranges() { return ranges_; }
  ObQueryRangeArray& get_ss_ranges() { return ss_ranges_; }
  const ObQueryRangeArray& get_ss_ranges() const { return ss_ranges_; }
  void set_query_range(ObQueryRange *query_range) { query_range_ = query_range; }
  common::ObIArray<ColumnItem> &get_range_columns() { return range_columns_; }
  common::ObIArray<ObExprConstraint> &get_expr_constraints() { return expr_constraints_; }
  void set_valid() { is_valid_ = true; }
  bool is_valid() const { return is_valid_; }
  bool is_get() const
  {
    return is_valid_ && (equal_prefix_count_ == range_columns_.count());
  }
  bool has_valid_range_condition() const
  {
    return equal_prefix_count_ > 0 || range_prefix_count_ > 0;
  }
  bool is_index_column_get() const
  {
    return is_valid_ && (equal_prefix_count_ >= index_column_count_);
  }
  bool equal_prefix_has_null() const
  {
    return equal_prefix_null_count_ > 0;
  }
  bool equal_prefix_all_null() const
  {
    return is_valid_ && equal_prefix_null_count_ == equal_prefix_count_;
  }
  void set_equal_prefix_count(const int64_t equal_prefix_count)
  { equal_prefix_count_ = equal_prefix_count; }
  void set_equal_prefix_null_count(const int64_t equal_prefix_null_count)
  { equal_prefix_null_count_ = equal_prefix_null_count; }
  void set_range_prefix_count(const int64_t range_prefix_count)
  { range_prefix_count_ = range_prefix_count; }
  void set_index_column_count(const int64_t index_column_count)
  { index_column_count_ = index_column_count; };
  void set_contain_always_false(const bool contain_always_false)
  { contain_always_false_ = contain_always_false; }

  TO_STRING_KV(K_(is_valid), K_(contain_always_false), K_(range_columns), K_(equal_prefix_count),
               K_(equal_prefix_null_count), K_(range_prefix_count),
               K_(index_column_count), K_(expr_constraints));
private:
  bool is_valid_;
  bool contain_always_false_;
  ObQueryRange *query_range_;
  ObQueryRangeArray ranges_;
  ObQueryRangeArray ss_ranges_; // for index skip scan, postfix range
  int64_t equal_prefix_count_;
  int64_t equal_prefix_null_count_;
  int64_t range_prefix_count_;
  int64_t index_column_count_; // index column count without adding primary key
  common::ObArray<ColumnItem> range_columns_;
  common::ObArray<ObExprConstraint> expr_constraints_;
  DISALLOW_COPY_AND_ASSIGN(QueryRangeInfo);
};


/* 索引的ordering 信息 */
class OrderingInfo {
public:
  OrderingInfo() :
    scan_direction_(default_asc_direction()),
    index_keys_(),
    ordering_(){
  }
  ObOrderDirection get_scan_direction() const { return scan_direction_; }
  const common::ObIArray<ObRawExpr *> &get_index_keys() const { return index_keys_; }
  const common::ObIArray<ObRawExpr *> &get_ordering() const { return ordering_; }

  common::ObIArray<ObRawExpr *> &get_index_keys() { return index_keys_; }
  common::ObIArray<ObRawExpr *> &get_ordering() { return ordering_; }
  void set_scan_direction(const ObOrderDirection direction) { scan_direction_ = direction; }
  TO_STRING_KV(K_(scan_direction), K_(index_keys));
private:
  ObOrderDirection scan_direction_;
  common::ObArray<ObRawExpr*> index_keys_;
  common::ObArray<ObRawExpr*> ordering_;
  DISALLOW_COPY_AND_ASSIGN(OrderingInfo);
};

// 每个索引作为一个entry
class IndexInfoEntry {
public:
  IndexInfoEntry() :
    index_id_(common::OB_INVALID_ID),
    is_unique_index_(false),
    is_index_back_(false),
    is_index_global_(false),
    is_geo_index_(false),
    range_info_(),
    ordering_info_(),
    interesting_order_info_(OrderingFlag::NOT_MATCH),
    interesting_order_prefix_count_(0) {
  }
  virtual ~IndexInfoEntry() {}
  QueryRangeInfo &get_range_info() { return range_info_; }
  const QueryRangeInfo &get_range_info() const { return range_info_; }
  OrderingInfo &get_ordering_info() { return ordering_info_; }
  const OrderingInfo &get_ordering_info() const { return ordering_info_; }
  int64_t get_interesting_order_info() const { return interesting_order_info_; }
  void set_interesting_order_info(int64_t info) { interesting_order_info_ = info; }
  int64_t get_interesting_order_prefix_count() const { return interesting_order_prefix_count_; }
  void set_interesting_order_prefix_count(int64_t count) { interesting_order_prefix_count_ = count; }
  uint64_t get_index_id() const { return index_id_; }
  void set_index_id(const uint64_t index_id) { index_id_ = index_id; }
  bool is_unique_index() const { return is_unique_index_; }
  bool is_valid_unique_index() const //唯一索引在query range中能否保持唯一
  {
    return is_unique_index_
           && !((lib::is_oracle_mode() && range_info_.equal_prefix_all_null())
                || (lib::is_mysql_mode() && (range_info_.equal_prefix_has_null()
                                             || !range_info_.is_index_column_get())));
  }
  void set_is_unique_index(const bool is_unique_index) { is_unique_index_ = is_unique_index; }
  bool is_index_back() const { return is_index_back_; }
  void set_is_index_back(const bool is_index_back) { is_index_back_ = is_index_back; }
  bool is_index_global() const { return is_index_global_; }
  void set_is_index_global(const bool is_index_global) { is_index_global_ = is_index_global; }
  bool is_index_geo() const { return is_geo_index_; }
  void set_is_index_geo(const bool is_index_geo) { is_geo_index_ = is_index_geo; }
  TO_STRING_KV(K_(index_id), K_(is_unique_index), K_(is_index_back), K_(is_index_global),
               K_(range_info), K_(ordering_info), K_(interesting_order_info),
               K_(interesting_order_prefix_count));
private:
  uint64_t index_id_;
  bool is_unique_index_;
  bool is_index_back_;
  bool is_index_global_;
  bool is_geo_index_;
  QueryRangeInfo range_info_;
  OrderingInfo ordering_info_;
  int64_t interesting_order_info_;  // 记录索引的序在stmt中的哪些地方用到 e.g. join, group by, order by
  // 索引序在stmt中使用到的最大前缀数
  // idx1(a,b,c), idx2(a,b),  对于order by a,b 两个索引使用到的最大前缀都是2
  int64_t interesting_order_prefix_count_;
  DISALLOW_COPY_AND_ASSIGN(IndexInfoEntry);
};


/* 对优化器过程中抽取的query range 做一些缓存，减少计算的次数 */
class ObIndexInfoCache
{
public:
  ObIndexInfoCache() : table_id_(common::OB_INVALID_ID),
    base_table_id_(common::OB_INVALID_ID),
    entry_count_(0)
  {
    MEMSET(index_entrys_, 0, sizeof(index_entrys_));
  }
  virtual ~ObIndexInfoCache();
  int get_query_range(const uint64_t table_id,
                      const uint64_t index_id,
                      const QueryRangeInfo *&query_range_info) const;
  int get_access_path_ordering(const uint64_t table_id,
                               const uint64_t index_id,
                               const OrderingInfo *&ordering_info) const;
  int get_index_info_entry(const uint64_t table_id,
                           const uint64_t index_id,
                           IndexInfoEntry *&entry) const;
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  void set_base_table_id(const uint64_t base_table_id) { base_table_id_ = base_table_id; }
  uint64_t get_table_id() const { return table_id_; }
  int add_index_info_entry(IndexInfoEntry *);
  TO_STRING_KV(K_(table_id), K_(base_table_id), K_(entry_count));
private:
  uint64_t table_id_;
  uint64_t base_table_id_;
  int64_t entry_count_;
  IndexInfoEntry *index_entrys_[common::OB_MAX_INDEX_PER_TABLE + 1]; //including table and index table
  DISALLOW_COPY_AND_ASSIGN(ObIndexInfoCache);
};

} //end of namespace sql
} //end of namespace oceanbase

#endif
