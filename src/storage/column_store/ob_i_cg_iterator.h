/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_STORAGE_COLUMN_STORE_OB_I_CG_ITERATOR_H_
#define OB_STORAGE_COLUMN_STORE_OB_I_CG_ITERATOR_H_
#include <stdint.h>
#include "ob_column_store_util.h"
#include "ob_cg_bitmap.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
namespace sql
{
struct PushdownFilterInfo;
}
namespace storage
{
class ObSSTableWrapper;
class ObGroupByCell;

class ObICGIterator
{
public:
  enum ObCGIterType
  {
    OB_CG_SCANNER = 0,
    OB_CG_ROW_SCANNER,
    OB_CG_SINGLE_ROW_SCANNER,
    OB_CG_AGGREGATED_SCANNER,
    OB_CG_GROUP_BY_SCANNER,
    OB_CG_GROUP_BY_DEFAULT_SCANNER,
    OB_CG_VIRTUAL_SCANNER,
    OB_CG_DEFAULT_SCANNER,
    OB_CG_TILE_SCANNER,
  };
  static bool is_valid_cg_scanner(const int cg_iter_type)
  {
    return OB_CG_SCANNER <= cg_iter_type && cg_iter_type < OB_CG_TILE_SCANNER;
  }
  static bool is_valid_cg_projector(
      const int cg_iter_type,
      const bool project_single_row)
  {
    return project_single_row ? (OB_CG_SINGLE_ROW_SCANNER || OB_CG_DEFAULT_SCANNER) : (OB_CG_ROW_SCANNER <= cg_iter_type && cg_iter_type < OB_CG_TILE_SCANNER);
  }
  static bool is_valid_cg_row_scanner(const int cg_iter_type)
  {
    return OB_CG_ROW_SCANNER == cg_iter_type || OB_CG_GROUP_BY_SCANNER == cg_iter_type;
  }
  static bool is_valid_group_by_cg_scanner(const int cg_iter_type)
  {
    return OB_CG_GROUP_BY_SCANNER == cg_iter_type || OB_CG_GROUP_BY_DEFAULT_SCANNER == cg_iter_type;
  }
  ObICGIterator() : cg_idx_(OB_CS_INVALID_CG_IDX) {};
  virtual ~ObICGIterator() {};
  virtual void reset() = 0;
  virtual void reuse() = 0;
  /*
   * iter_param: iter param for this column group
   * table: this column group sstable
   */
  virtual int init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObSSTableWrapper &wrapper) = 0;
  /*
   * rescan interface
   */
  virtual int switch_context(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper) = 0;
  /*
   * range: locate row index range
   * bitmap: only used for projection when filter applied
   */
  virtual int locate(
      const ObCSRange &range,
      const ObCGBitmap *bitmap = nullptr) = 0;
  /*
   * filter_info: pushdown filter tree info
   * row_count: no of rows needed to be calculated in cg
   * result_bitmap: calculated bitmap result of the filter tree in cg
   * notice: valid result bitmap maybe returned with OB_ITER_END
   */
  virtual int apply_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::PushdownFilterInfo &filter_info,
      const int64_t row_count,
      const ObCGBitmap *parent_bitmap,
      ObCGBitmap &result_bitmap) = 0;
  /*
   * notice: valid row count maybe returned with OB_ITER_END
   */
  virtual int get_next_rows(uint64_t &count, const uint64_t capacity) = 0;
  virtual ObCGIterType get_type() = 0;
  virtual int get_next_row(const blocksstable::ObDatumRow *&datum_row)
  { return OB_NOT_IMPLEMENT; }
  OB_INLINE uint32_t get_cg_idx() const { return cg_idx_; }
  OB_INLINE void set_cg_idx(uint32_t cg_idx) { cg_idx_ = cg_idx; }
  OB_INLINE bool is_valid() const { return cg_idx_ != OB_CS_INVALID_CG_IDX; }
  DECLARE_PURE_VIRTUAL_TO_STRING;

private:
  uint32_t cg_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObICGIterator);
};

/*
 * interface for group by pushdown to column group:
 * 1. decide group size(one micro block of group by column) and whether use pushdown or not;
 * 2. read distinct values and indexes from group by column;
 * 3. calculate aggregate on this column group
 */
class ObICGGroupByProcessor
{
public:
  /*
   * initialize aggregate info on this column group
   */
  virtual int init_group_by_info() = 0;
  /*
   * calculate the group size to do filter, projection and group by
   */
  virtual int decide_group_size(int64_t &group_size) = 0;
  /*
   * decide whether use pushdown or not
   * group_by_col: the group by column offset
   */
  virtual int decide_can_group_by(const int32_t group_by_col, bool &can_group_by) = 0;
  /*
   * read distinct values in this group
   * group_by_col: the group by column offset
   */
  virtual int read_distinct(const int32_t group_by_col) = 0;
  /*
   * read reference to distinct value of rows in this group
   * group_by_col: the group by column offset
   */
  virtual int read_reference(const int32_t group_by_col) = 0;
  /*
   * calculate aggregate on this column group
   * is_group_by_col: current column is group by column?
   */
  virtual int calc_aggregate(const bool is_group_by_col) = 0;

  virtual int locate_micro_index(const ObCSRange &range) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

}
}

#endif
