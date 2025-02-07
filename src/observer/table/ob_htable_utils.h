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

#ifndef _OB_HTABLE_UTILS_H
#define _OB_HTABLE_UTILS_H 1
#include "common/row/ob_row.h"
#include "lib/string/ob_string.h"
#include "share/table/ob_table.h"
#include "ob_htable_lock_mgr.h"
#include "share/table/ob_table_rpc_struct.h"
#include <stdint.h>
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace table
{
// Interface ObHTableCell
class ObHTableCell
{
public:
  ObHTableCell() {}
  virtual ~ObHTableCell() {}

  virtual common::ObString get_rowkey() const = 0;
  virtual common::ObString get_qualifier() const = 0;
  virtual int64_t get_timestamp() const = 0;
  virtual common::ObString get_value() const = 0;
  virtual common::ObString get_family() const = 0;
  enum class Type
  {
    FIRST_ON_ROW = 0 /*virtual cell which present the first cell on row*/,
    FIRST_ON_COL = 1 /*virtual cell which present the first cell on column*/,
    NORMAL = 2,
    LAST_ON_COL = 3 /*virtual cell which present the last cell on column, "bigger" than other cells*/,
    LAST_ON_ROW = 4 /*virtual cell which present the last cell on row, "bigger" than other cells*/
  };
  // true for
  virtual Type get_type() const = 0;
  TO_STRING_KV(ObHTableConstants::ROWKEY_CNAME, get_rowkey(),
               ObHTableConstants::CQ_CNAME, get_qualifier(),
               ObHTableConstants::VERSION_CNAME, get_timestamp(),
               ObHTableConstants::VALUE_CNAME, get_value());
private:
  DISALLOW_COPY_AND_ASSIGN(ObHTableCell);
};

class ObHTableCellEntity: public ObHTableCell
{
public:
  explicit ObHTableCellEntity(common::ObNewRow *ob_row);
  ObHTableCellEntity(common::ObNewRow *ob_row, Type type);
  ObHTableCellEntity();
  virtual ~ObHTableCellEntity()
  {
    if (OB_NOT_NULL(ob_row_)) {
      ob_row_->~ObNewRow();
    }
  };

  void set_ob_row(common::ObNewRow *ob_row) { ob_row_ = ob_row; }
  int deep_copy_ob_row(const common::ObNewRow *ob_row, common::ObArenaAllocator &allocator);
  virtual void reset(common::ObArenaAllocator &allocator);
  virtual common::ObNewRow* get_ob_row() { return ob_row_; }
  virtual const common::ObNewRow* get_ob_row() const { return ob_row_; }

  virtual common::ObString get_rowkey() const override;
  virtual common::ObString get_qualifier() const override;
  virtual int64_t get_timestamp() const override;
  virtual common::ObString get_value() const override;
  virtual common::ObString get_family() const override;

  void set_value(ObString value) const;
  void set_family(const ObString family);
  virtual Type get_type() const { return type_; }
private:
  common::ObNewRow *ob_row_;
  Type type_;
  ObString family_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableCellEntity);
};

class KeyOnlyCell : public ObHTableCellEntity {
public:
  KeyOnlyCell(const ObHTableCell& cell,bool len_as_val ) : cell_(cell), len_as_val_(len_as_val)
  {}
  ~KeyOnlyCell() {}
  virtual common::ObString get_rowkey() const override { return cell_.get_rowkey(); };
  virtual common::ObString get_qualifier() const override { return cell_.get_qualifier(); };
  virtual int64_t get_timestamp() const override {return cell_.get_timestamp(); };
  virtual common::ObString get_value() const override;
  virtual Type get_type() const override { return cell_.get_type(); }
private:
  const ObHTableCell& cell_;
  bool len_as_val_;
};

class ObHTableEmptyCell: public ObHTableCell
{
public:
  ObHTableEmptyCell() {}
  virtual ~ObHTableEmptyCell() {}

  virtual common::ObString get_rowkey() const override { return common::ObString(); }
  virtual common::ObString get_qualifier() const override { return common::ObString(); }
  virtual common::ObString get_value() const override { return common::ObString(); }
  virtual common::ObString get_family() const override { return common::ObString(); }
private:
  DISALLOW_COPY_AND_ASSIGN(ObHTableEmptyCell);
};

class ObHTableFirstOnRowCell: public ObHTableEmptyCell
{
public:
  ObHTableFirstOnRowCell(const common::ObString &rowkey)
      :rowkey_(rowkey)
  {}
  virtual ~ObHTableFirstOnRowCell() {}

  virtual common::ObString get_rowkey() const override { return rowkey_; }
  virtual int64_t get_timestamp() const override { return ObHTableConstants::LATEST_TIMESTAMP; }
  virtual Type get_type() const { return Type::FIRST_ON_ROW; }
private:
  common::ObString rowkey_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableFirstOnRowCell);
};

class ObHTableFirstOnRowColCell: public ObHTableFirstOnRowCell
{
public:
  ObHTableFirstOnRowColCell(const common::ObString &rowkey, const common::ObString &qualifier)
      :ObHTableFirstOnRowCell(rowkey),
       qualifier_(qualifier)
  {}
  virtual ~ObHTableFirstOnRowColCell() {}

  virtual common::ObString get_qualifier() const override { return qualifier_; }
  virtual Type get_type() const { return Type::FIRST_ON_COL; }
private:
  common::ObString qualifier_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableFirstOnRowColCell);
};

class ObHTableLastOnRowCell: public ObHTableEmptyCell
{
public:
  ObHTableLastOnRowCell(const common::ObString &rowkey)
      :rowkey_(rowkey)
  {}
  virtual ~ObHTableLastOnRowCell() {}

  virtual common::ObString get_rowkey() const override { return rowkey_; }
  virtual int64_t get_timestamp() const override { return ObHTableConstants::OLDEST_TIMESTAMP; }
  virtual Type get_type() const { return Type::LAST_ON_ROW; }
private:
  common::ObString rowkey_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableLastOnRowCell);
};

class ObHTableLastOnRowColCell: public ObHTableLastOnRowCell
{
public:
  ObHTableLastOnRowColCell(const common::ObString &rowkey, const common::ObString &qualifier)
      :ObHTableLastOnRowCell(rowkey),
       qualifier_(qualifier)
  {}
  virtual ~ObHTableLastOnRowColCell() {}

  virtual common::ObString get_qualifier() const override { return qualifier_; }
  virtual Type get_type() const { return Type::LAST_ON_COL; }
private:
  common::ObString qualifier_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableLastOnRowColCell);
};

class ObHTableCellEntity2: public ObHTableCell
{
public:
  explicit ObHTableCellEntity2(const ObITableEntity *entity)
      :entity_(entity)
  {}
  virtual ~ObHTableCellEntity2() {}

  virtual common::ObString get_rowkey() const override;
  virtual common::ObString get_qualifier() const override;
  virtual int64_t get_timestamp() const override;
  virtual common::ObString get_value() const override;
  virtual common::ObString get_family() const override { return common::ObString(); }
  virtual Type get_type() const { return Type::NORMAL; }
  int get_value(ObString &str) const;
private:
  const ObITableEntity *entity_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableCellEntity2);
};

class ObHTableCellEntity3: public ObHTableCell
{
public:
  explicit ObHTableCellEntity3(const ObITableEntity *entity)
      :entity_(entity),
       last_get_is_null_(false)
  {}
  virtual ~ObHTableCellEntity3() {}

  virtual common::ObString get_rowkey() const override;
  virtual common::ObString get_qualifier() const override;
  virtual int64_t get_timestamp() const override;
  virtual common::ObString get_value() const override;
  virtual common::ObString get_family() const override { return common::ObString(); }
  bool last_get_is_null() const { return last_get_is_null_; }
  virtual Type get_type() const { return Type::NORMAL; }
private:
  const ObITableEntity *entity_;
  mutable bool last_get_is_null_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableCellEntity3);
};

/// Represents an interval of version timestamps.
/// [min_stamp, max_stamp), e.g. [3, 5)
class ObTimeRange final
{
public:
  ObTimeRange()
      :min_stamp_(ObHTableConstants::INITIAL_MIN_STAMP),
       max_stamp_(ObHTableConstants::INITIAL_MAX_STAMP),
       is_all_time_(true)
  {}

  ObTimeRange(int64_t min, int64_t max)
      :min_stamp_(min),
       max_stamp_(max)
  {
    is_all_time_ = is_all_time();
  }
  /**
   * Compare the timestamp to timerange.
   * @return -1 if timestamp is less than timerange,
   * 0 if timestamp is within timerange,
   * 1 if timestamp is greater than timerange
   */
  inline int compare(int64_t timestamp) const
  {
    int cmp_ret = 0;
    if (is_all_time()) {
      cmp_ret = 0;
    } else if (timestamp < min_stamp_) {
      cmp_ret = -1;
    } else if (timestamp >= max_stamp_) {
      cmp_ret = 1;
    }
    return cmp_ret;
  }
private:
  bool is_all_time() const
  {
    return ObHTableConstants::INITIAL_MIN_STAMP == min_stamp_
        && ObHTableConstants::INITIAL_MAX_STAMP == max_stamp_;
  }
private:
  int64_t min_stamp_;
  int64_t max_stamp_;
  bool is_all_time_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTimeRange);
};

/// (min_stamp, max_stamp], e.g. (-5, -3]
class ObNegativeTimeRange final
{
  static constexpr int64_t INITIAL_NEG_MIN_STAMP = -INT64_MAX;
  static constexpr int64_t INITIAL_NEG_MAX_STAMP = 0;
public:
  ObNegativeTimeRange()
      :min_stamp_(INITIAL_NEG_MIN_STAMP),
      max_stamp_(INITIAL_NEG_MAX_STAMP)
      {
        is_all_time_ = is_all_time();
      }
  ObNegativeTimeRange(int64_t min, int64_t max)
      :min_stamp_(min),
       max_stamp_(max)
  {
    is_all_time_ = is_all_time();
  }  /**
   * Compare the timestamp to timerange.
   * @return -1 if timestamp is less than timerange,
   * 0 if timestamp is within timerange,
   * 1 if timestamp is greater than timerange
   */
  inline int compare(int64_t timestamp) const
  {
    int cmp_ret = 0;
    if (is_all_time()) {
      cmp_ret = 0;
    } else if (timestamp <= min_stamp_) {
      cmp_ret = 1;
    } else if (timestamp > max_stamp_) {
      cmp_ret = -1;
    }
    return cmp_ret;
  }
  TO_STRING_KV(K_(min_stamp), K_(max_stamp), K_(is_all_time));
private:
  bool is_all_time() const
  {
    return INITIAL_NEG_MIN_STAMP == min_stamp_
        && INITIAL_NEG_MAX_STAMP == max_stamp_;
  }
private:
  int64_t min_stamp_;
  int64_t max_stamp_;
  bool is_all_time_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObNegativeTimeRange);
};

class KeyRange
{
public:
  KeyRange()
      : min_(),
        min_inclusive_(false),
        max_(),
        max_inclusive_(false)
  {}
  KeyRange(ObString min, bool min_inclusive, ObString max, bool max_inclusive)
      : min_(min),
        min_inclusive_(min_inclusive),
        max_(max),
        max_inclusive_(max_inclusive)
  {}
  ~KeyRange() {}

  ObString min() const { return min_; }
  bool min_inclusive() const { return min_inclusive_; }

  ObString max() const { return max_; }
  bool max_inclusive() const { return max_inclusive_; }
  void set_min(ObString value) { min_ = value; }
  void set_min_inclusive(bool value) { min_inclusive_ = value; }
  void set_max(ObString value) { max_ = value; }
  void set_max_inclusive(bool value) { max_inclusive_ = value; }
  bool valid()
  {
    return min_.empty()
    | max_.empty()
    | (min_.compare(max_) < 0)
    | (min_.compare(max_) == 0 && max_inclusive_);
  }
  bool contain(ObString row_key)
  {
    int l_cmp = min_.compare(row_key);
    int r_cmp = max_.empty()? 1 : max_.compare(row_key);
    bool l_contain = l_cmp < 0 || (l_cmp == 0 && min_inclusive_);
    bool r_contain = r_cmp > 0 || (r_cmp == 0 && max_inclusive_);
    return l_contain & r_contain;
  }

  int compare(const KeyRange* other) const
  {
    int ret = 0;
    if (nullptr == other) {
    } else if (min() == other->min()) {
      ret = static_cast<int>(other->min_inclusive() - (int)min_inclusive());
    } else {
      ret = min().compare(other->min());
    }
    return ret;
  }
  TO_STRING_KV(K_(min), K_(min_inclusive), K_(max), K_(max_inclusive));
private:
  ObString min_;
  bool min_inclusive_;
  ObString max_;
  bool max_inclusive_;
};

class ObKeyRangeNode {
public:
  ObKeyRangeNode(): value_(nullptr) {};
  ObKeyRangeNode(KeyRange* value): value_(value) {};
  ~ObKeyRangeNode() {};
  RBNODE(ObKeyRangeNode, rblink);
  KeyRange* get_value() const { return value_; }
  int compare(const ObKeyRangeNode *node) const
  {
    int ret = 0;
    if (nullptr != value_ && nullptr != node) {
      KeyRange* other_range = node->get_value();
      ret = value_->compare(other_range);
    }
    return ret;
  }
  TO_STRING_KV(K_(value));
private:
  KeyRange* value_;
};

typedef container::ObRbTree<ObKeyRangeNode, container::ObDummyCompHelper<ObKeyRangeNode>> ObKeyRangeTree;

class ObHTableUtils
{
public:
  /// Create a Cell that is larger than all other possible Cells for the given Cell's rk:cf:q
  static int create_last_cell_on_row_col(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell);
  /// Create a Cell that is smaller than all other possible Cells for the given Cell's rk:cf and passed qualifier.
  static int create_first_cell_on_row_col(common::ObIAllocator &allocator,
                                          const ObHTableCell &cell,
                                          const common::ObString &qualifier,
                                          ObHTableCell *&new_cell);
  /// Create a Cell that is smaller than all other possible Cells for the given Cell's rk:ts and passed timestamp.
  static int create_first_cell_on_row_col_ts(common::ObIAllocator &allocator,
                                             const ObHTableCell &cell,
                                             const int64_t timestamp,
                                             ObHTableCell *&new_cell);
  /// Create a Cell that is larger than all other possible Cells for the given Cell's row.
  static int create_last_cell_on_row(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell);
  /// Create a Cell that is smaller than all other possible Cells for the given Cell's row.
  static int create_first_cell_on_row(common::ObIAllocator &allocator, const ObHTableCell &cell, ObHTableCell *&new_cell);
  static int create_first_cell_on_row(common::ObIAllocator &allocator, const ObString &row_key, ObHTableCell *&new_cell);

  static int compare_qualifier(const common::ObString &cq1, const common::ObString &cq2);
  static int compare_rowkey(const common::ObString &rk1, const common::ObString &rk2);
  static int compare_rowkey(const ObHTableCell &cell1, const ObHTableCell &cell2);
  static int compare_cell(const ObHTableCell &cell1, const ObHTableCell &cell2, common::ObQueryFlag::ScanOrder &scan_order);
  static int compare_cell(const ObHTableCell &cell1, const ObHTableCell &cell2, bool is_reversed);
  static int64_t current_time_millis() { return common::ObTimeUtility::current_time() / 1000; }
  static int java_bytes_to_int64(const ObString &bytes, int64_t &val);
  static int int64_to_java_bytes(int64_t val, char bytes[8]);
  // lock all rows of mutations in the given lock mode with the given lock handle,
  // for put, delete, mutations in check_and_xxx
  static int lock_htable_rows(uint64_t table_id, const ObIArray<table::ObTableOperation> &ops, ObHTableLockHandle &handle, ObHTableLockMode lock_mode);
  // lock the check row in the given lock mode with the given lock hanle,
  // for increment, append, and check operation in check_and_xxx
  static int lock_htable_row(uint64_t table_id, const ObTableQuery &htable_query, ObHTableLockHandle &handle, ObHTableLockMode lock_mode);
  static int lock_redis_key(uint64_t table_id, const ObString &lock_key, ObHTableLockHandle &handle, ObHTableLockMode lock_mode);
  static int check_htable_schema(const share::schema::ObTableSchema &table_schema);
  static OB_INLINE bool is_tablegroup_req(const ObString &table_name, ObTableEntityType entity_type)
  {
    return entity_type == ObTableEntityType::ET_HKV && table_name.find('$') == nullptr;
  }

  static int get_format_filter_string(char *buf, int64_t buf_len, int64_t &pos, const char *name);
  static int kv_hbase_client_scanner_timeout_period(uint64_t tenant_id);
  static int get_kv_hbase_client_scanner_timeout_period(uint64_t tenant_id);
  static int get_hbase_scanner_timeout(uint64_t tenant_id);
  static int generate_hbase_bytes(ObIAllocator& allocator, int32_t len, char*& val);
  static int get_prefix_key_range(common::ObIAllocator &allocator, ObString prefix, KeyRange *range);
  static int merge_key_range(ObKeyRangeTree& tree);
private:
  ObHTableUtils() = delete;
  ~ObHTableUtils() = delete;
  static const int HBASE_SCANNER_TIMEOUT_DEFAULT_VALUE = 60000;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_HTABLE_UTILS_H */
