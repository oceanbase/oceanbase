/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef _OB_HBASE_COMMON_STRUCT_H
#define _OB_HBASE_COMMON_STRUCT_H

#include "share/table/ob_table_rpc_struct.h"
#include "observer/table/common/ob_table_common_struct.h"
#include "observer/table/ob_table_filter.h"

namespace oceanbase
{
namespace table
{

class ObHbaseTabletCells
{
public:
  ObHbaseTabletCells()
  : tablet_id_(ObTabletID::INVALID_TABLET_ID),
    cells_()
  {
    cells_.set_attr(ObMemAttr(MTL_ID(), "HbaseTblCells"));
  }
  ~ObHbaseTabletCells() = default;
  ObHbaseTabletCells(const ObHbaseTabletCells &other)
  : tablet_id_(other.tablet_id_),
    cells_(other.cells_)
  {
  }


  OB_INLINE const common::ObTabletID get_tablet_id() const { return tablet_id_; }
  OB_INLINE const common::ObIArray<const table::ObITableEntity *> &get_cells() const { return cells_; }
  OB_INLINE common::ObIArray<const table::ObITableEntity *> &get_cells() { return cells_; }
  OB_INLINE void set_tablet_id(common::ObTabletID tablet_id) { tablet_id_ = tablet_id; }
  TO_STRING_KV(K_(tablet_id), K_(cells));
private:
  common::ObTabletID tablet_id_;
  ObSEArray<const table::ObITableEntity *, 4> cells_;
};

class ObHbaseTableCells
{
public:
  ObHbaseTableCells()
  : table_id_(OB_INVALID_ID),
    tablet_cells_(),
    table_name_()
  {
    tablet_cells_.set_attr(ObMemAttr(MTL_ID(), "HbaseTableCells"));
  }
  ~ObHbaseTableCells() = default;
  OB_INLINE uint64_t get_table_id() const { return table_id_; }
  OB_INLINE const ObString &get_table_name() const { return table_name_; }
  OB_INLINE const common::ObIArray<table::ObHbaseTabletCells *> &get_tablet_cells_array() const { return tablet_cells_; }
  OB_INLINE common::ObIArray<table::ObHbaseTabletCells *> &get_tablet_cells_array() { return tablet_cells_; }
  OB_INLINE void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  OB_INLINE void set_table_name(const ObString &table_name) { table_name_ = table_name; }
  TO_STRING_KV(K_(table_id), K_(table_name), K_(tablet_cells));
private:
  uint64_t table_id_;
  ObSEArray<table::ObHbaseTabletCells *, 4> tablet_cells_;
  ObString table_name_;
};

struct ObHbaseTabletCellResults
{
public:
  OB_INLINE ObIArray<ObTableOperationResult> &get_cells_result() { return cells_result; }
private:
  ObSEArray<ObTableOperationResult, 4> cells_result;
};

class ObHbaseQuery
{
public:
  ObHbaseQuery(uint64_t table_id, common::ObTabletID tablet_id, ObTableQuery &query)
  : ObHbaseQuery(table_id, tablet_id, query, false)
  {}
  ObHbaseQuery(uint64_t table_id, common::ObTabletID tablet_id, ObTableQuery &query, bool qualifier_with_family)
  : table_id_(table_id),
    tablet_id_(tablet_id),
    query_(query),
    qualifier_with_family_(qualifier_with_family),
    use_wildcard_column_tracker_(false)
  {}
  ObHbaseQuery(ObTableQuery &query, bool qualifier_with_family)
  : table_id_(OB_INVALID_ID),
    tablet_id_(),
    query_(query),
    table_name_(),
    qualifier_with_family_(qualifier_with_family),
    use_wildcard_column_tracker_(false)
  {}
  virtual ~ObHbaseQuery() = default;

  OB_INLINE uint64_t get_table_id() const { return table_id_; }
  OB_INLINE common::ObTabletID get_tablet_id() const { return tablet_id_; }
  OB_INLINE ObTableQuery &get_query() { return query_; }
  OB_INLINE const ObTableQuery &get_query() const { return query_; }
  OB_INLINE bool get_qualifier_with_family() const { return qualifier_with_family_; }

  OB_INLINE void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  OB_INLINE void set_tablet_id(const common::ObTabletID &tablet_id) { tablet_id_ = tablet_id; }

  OB_INLINE const ObString &get_table_name() const { return table_name_; }
  OB_INLINE void set_table_name(const ObString &table_name) { table_name_ = table_name; }

  OB_INLINE void set_use_wildcard_column_tracker(bool is_enable) { use_wildcard_column_tracker_ = is_enable; }
  OB_INLINE bool use_wildcard_column_tracker() const { return use_wildcard_column_tracker_; }
  TO_STRING_KV(K_(table_id), K_(tablet_id), K_(query), K_(table_name), K_(qualifier_with_family));
private:
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  ObTableQuery &query_;
  ObString table_name_;
  bool qualifier_with_family_; // e.g., cf1.c1
  // note: when it is enable, force to use wildcard_column_tracker_ which will get all qualify
  bool use_wildcard_column_tracker_;
};

class ObTableMergeFilterCompare
{
public:
  ObTableMergeFilterCompare() = default;
  virtual ~ObTableMergeFilterCompare() = default;

  virtual int compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const = 0;
  virtual bool operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs) = 0;

  int get_error_code() const noexcept { return result_code_; }
  TO_STRING_KV(K_(result_code));

protected:
  int result_code_ = OB_SUCCESS;
};

class ObHbaseQueryResultIterator : public ObTableQueryIResultIterator
{
public:
  ObHbaseQueryResultIterator(const ObHbaseQuery &query, const ObTableExecCtx &exec_ctx) {}

  virtual ~ObHbaseQueryResultIterator() = default;

  virtual int init() = 0;

  virtual int get_next_result(ObTableQueryResult &hbase_wide_rows) = 0;

  virtual int get_next_result(table::ObTableQueryIterableResult &hbase_wide_rows) = 0;
  // virtual int get_next_result(table::ObTableQueryIterableResult *&hbase_wide_rows) = 0;

  virtual hfilter::Filter *get_filter() const = 0;

  virtual bool has_more_result() const = 0;
  virtual void close() = 0;
};

class LimitScope
{
public:
  enum class Scope
  {
    BETWEEN_ROWS = 0,
    BETWEEN_CELLS = 1
  };
  LimitScope() : scope_(Scope::BETWEEN_ROWS), depth_(0) {}
  LimitScope(Scope scope) : scope_(scope), depth_(static_cast<int>(scope)) {}
  ~LimitScope() {};
  OB_INLINE int depth() const { return depth_; }
  OB_INLINE void set_scope(Scope scope) {scope_ = scope; depth_ = static_cast<int>(scope);}
  OB_INLINE bool can_enforce_limit_from_scope(const LimitScope& checker_scope) const { return checker_scope.depth() <= depth_; }
  TO_STRING_KV(K_(scope), K_(depth));
private:
  Scope scope_;
  int depth_;
};

class LimitFields
{
public:
  LimitFields(): batch_(-1), size_(-1), time_(-1), size_scope_(LimitScope::Scope::BETWEEN_ROWS), time_scope_(LimitScope::Scope::BETWEEN_ROWS) {}
  LimitFields(int32_t batch, int64_t size, int64_t time, LimitScope limit_scope) { set_fields(batch, size, time, limit_scope); }
  void set_fields(int64_t size, int64_t time, LimitScope limit_scope) {
    set_size_scope(limit_scope);
    set_time_scope(limit_scope);
    set_size(size);
    set_time(time);
  }
  void set_fields(int64_t batch_size, int64_t size, int64_t time, LimitScope limit_scope) {
    set_batch(batch_size);
    set_size_scope(limit_scope);
    set_time_scope(limit_scope);
    set_size(size);
    set_time(time);
  }
  ~LimitFields() {};
  void reset() { batch_ = 0; size_ =0; time_ = 0; size_scope_.set_scope(LimitScope::Scope::BETWEEN_ROWS);}
  void set_batch(int32_t batch) { batch_ = batch; }
  void set_size(int64_t size) { size_ = size; }
  void set_time(int64_t time) { time_ = time; }
  void set_time_scope(LimitScope scope) { time_scope_ = scope; }
  void set_size_scope(LimitScope scope) { size_scope_ = scope; }
  int32_t get_batch() { return batch_; }
  int64_t get_size() { return size_; }
  int64_t get_time() { return time_; }
  LimitScope get_size_scope() { return size_scope_; }
  LimitScope get_time_scope() { return time_scope_; }
  bool can_enforce_batch_from_scope(LimitScope checker_scope) { return LimitScope(LimitScope::Scope::BETWEEN_CELLS).can_enforce_limit_from_scope(checker_scope);}
  bool can_enforce_size_from_scope(LimitScope checker_scope) { return size_scope_.can_enforce_limit_from_scope(checker_scope); }
  bool can_enforce_time_from_scope(LimitScope checker_scope) { return time_scope_.can_enforce_limit_from_scope(checker_scope); }
  TO_STRING_KV(K_(batch), K_(size), K_(time), K_(size_scope), K_(time_scope));
private:
  int32_t batch_;
  int64_t size_;
  int64_t time_;
  LimitScope size_scope_;
  LimitScope time_scope_;
};


class ScannerContext
{
public:
  ScannerContext()
  {
    limits_.set_fields(LIMIT_DEFAULT_VALUE, LIMIT_DEFAULT_VALUE, LIMIT_DEFAULT_VALUE, LimitScope::Scope::BETWEEN_ROWS);
    progress_.set_fields(
        PROGRESS_DEFAULT_VALUE, PROGRESS_DEFAULT_VALUE, PROGRESS_DEFAULT_VALUE, LimitScope::Scope::BETWEEN_ROWS);
  }
  ScannerContext(int32_t batch, int64_t size, int64_t time, LimitScope limit_scope)
  {
    limits_.set_fields(batch, size, time, limit_scope);
  }
  ~ScannerContext() {};
  OB_INLINE void increment_batch_progress(int32_t batch)
  {
    int32_t current_batch = progress_.get_batch();
    progress_.set_batch(current_batch + batch);
  }
  OB_INLINE void increment_size_progress(int64_t size)
  {
    int64_t current_size = progress_.get_size();
    progress_.set_size(current_size + size);
  }
  OB_INLINE void update_time_progress()
  {
    progress_.set_time(ObTimeUtility::current_time());
  }
  OB_INLINE bool check_batch_limit(LimitScope checker_scope)
  {
    bool ret = false;
    if (limits_.can_enforce_batch_from_scope(checker_scope) && limits_.get_batch() > 0) {
      ret = progress_.get_batch() >= limits_.get_batch();
    }
    return ret;
  }
  OB_INLINE bool check_size_limit(LimitScope checker_scope)
  {
    bool ret = false;
    if (limits_.can_enforce_size_from_scope(checker_scope) && limits_.get_size() > 0) {
      ret = progress_.get_size() >= limits_.get_size();
    }
    return ret;
  }
  OB_INLINE bool check_time_limit(LimitScope checker_scope)
  {
    bool ret = false;
    if (limits_.can_enforce_time_from_scope(checker_scope) && limits_.get_time() > 0) {
      ret = progress_.get_time() >= limits_.get_time();
    }
    return ret;
  }
  OB_INLINE bool check_any_limit(LimitScope checker_scope)
  {
    return check_batch_limit(checker_scope) || check_size_limit(checker_scope) || check_time_limit(checker_scope);
  }

  LimitFields limits_;
  LimitFields progress_;
  TO_STRING_KV(K_(limits),
                K_(progress));
private:
  static const int LIMIT_DEFAULT_VALUE = -1;
  static const int PROGRESS_DEFAULT_VALUE = 0;
};

class ObHbaseMergeCompare : public ObTableMergeFilterCompare
{
public:
  ObHbaseMergeCompare() = default;
  virtual ~ObHbaseMergeCompare() {}
  virtual bool operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs) override;
};

class ObHbaseRowForwardCompare final : public ObHbaseMergeCompare
{
public:
  ObHbaseRowForwardCompare(bool need_compare_ts = false)
  : ObHbaseMergeCompare(),
    need_compare_ts_(need_compare_ts)
  {}
  virtual ~ObHbaseRowForwardCompare() override = default;
  virtual int compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const override;
private:
  bool need_compare_ts_;
};

class ObHbaseRowReverseCompare final : public table::ObHbaseMergeCompare
{
public:
  ObHbaseRowReverseCompare(bool need_compare_ts = false)
  : ObHbaseMergeCompare(),
    need_compare_ts_(need_compare_ts)
  {}
  virtual ~ObHbaseRowReverseCompare() override = default;
  virtual int compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const override;
private:
  bool need_compare_ts_;
};

class ObHbaseRowKeyForwardCompare final : public ObHbaseMergeCompare
{
public:
  ObHbaseRowKeyForwardCompare() = default;
  virtual ~ObHbaseRowKeyForwardCompare() override = default;
  virtual int compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const override;
};

class ObHbaseRowKeyReverseCompare final : public ObHbaseMergeCompare
{
public:
  ObHbaseRowKeyReverseCompare() = default;
  virtual ~ObHbaseRowKeyReverseCompare() override = default;
  virtual int compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const override;
};

} // end of namespace table
} // end of namespace oceanbase

#endif
