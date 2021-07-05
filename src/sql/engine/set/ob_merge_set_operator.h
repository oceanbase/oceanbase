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

#ifndef OCEANBASE_SQL_OB_MERGE_SET_OPERATOR_H_
#define OCEANBASE_SQL_OB_MERGE_SET_OPERATOR_H_

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_double_children_phy_operator.h"
#include "sql/engine/set/ob_set_operator.h"
#include "common/row/ob_row.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
enum ObCTEPseudoColumn { CTE_SEARCH = 0, CTE_CYCLE = 1, CTE_PSEUDO_COLUMN_CNT = 2 };

class ObMergeSetOperator : public ObSetOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class ObMergeSetOperatorCtx : public ObPhyOperatorCtx {
  public:
    explicit ObMergeSetOperatorCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx), last_output_row_(), last_row_buf_(nullptr), need_skip_init_row_(false)
    {}

    inline int alloc_last_row_buf(const int64_t column_count)
    {
      void* ptr = NULL;
      int ret = common::OB_SUCCESS;
      int64_t row_size = column_count * sizeof(common::ObObj);
      if (OB_UNLIKELY(NULL == (last_row_buf_ = exec_ctx_.get_allocator().alloc(OB_ROW_BUF_SIZE)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(ERROR, "alloc last row buffer failed", K(OB_ROW_BUF_SIZE));
      } else if (OB_UNLIKELY(NULL == (ptr = exec_ctx_.get_allocator().alloc(row_size)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(ERROR, "alloc last row cell memory failed", K(row_size));
      } else {
        last_output_row_.cells_ = new (ptr) common::ObObj[column_count];
        last_output_row_.count_ = column_count;
      }
      return ret;
    }

    inline int store_last_row(const common::ObNewRow& row)
    {
      int ret = common::OB_SUCCESS;
      common::ObDataBuffer data_buf(static_cast<char*>(last_row_buf_), OB_ROW_BUF_SIZE);

      if (row.is_invalid() || last_output_row_.is_invalid() || row.get_count() > last_output_row_.get_count()) {
        ret = common::OB_INVALID_ARGUMENT;
        SQL_ENG_LOG(WARN, "invalid_argument", K(row), K_(last_output_row));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count(); ++i) {
        if (OB_FAIL(common::ob_write_obj(data_buf, row.get_cell(i), last_output_row_.cells_[i]))) {
          SQL_ENG_LOG(WARN, "write obj failed", K(ret), K(i), K(row));
        }
      }
      return ret;
    }
    void free_last_row_buf()
    {
      if (last_row_buf_) {
        exec_ctx_.get_allocator().free(last_row_buf_);
        last_row_buf_ = NULL;
      }
      if (last_output_row_.cells_) {
        exec_ctx_.get_allocator().free(last_output_row_.cells_);
        last_output_row_.cells_ = NULL;
      }
    }
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }

    inline bool get_need_skip_init_row()
    {
      return need_skip_init_row_;
    }
    inline void set_need_skip_init_row(bool b)
    {
      need_skip_init_row_ = b;
    }

  protected:
    common::ObNewRow last_output_row_;
    void* last_row_buf_;
    static const int64_t OB_ROW_BUF_SIZE;
    bool need_skip_init_row_;
  };

public:
  explicit ObMergeSetOperator(common::ObIAllocator& alloc);
  ~ObMergeSetOperator();
  virtual void reset();
  virtual void reuse();
  int init(int64_t count);

  int add_set_direction(ObOrderDirection direction);
  int add_cte_pseudo_column(const ObPseudoColumnRawExpr* expr, int64_t pos);
  int init_cte_pseudo_column();
  int set_map_array(const ObIArray<int64_t>& map_array);

protected:
  int init_set_directions(int64_t count);
  int strict_compare(const common::ObNewRow& row1, const common::ObNewRow& row2, int& cmp) const;
  int strict_compare(const common::ObNewRow& row1, const common::ObNewRow& row2, bool& equal) const;
  /**
   * @brief get a row distinct with the specified row
   * @param child_op[in], the child operator
   * @param compare_row[in], the specified row
   * @param row[out], the output row
   * @return if success, return OB_SUCCESS,
   * if iterator end, return OB_ITER_END, otherwise, return errno
   */
  int do_strict_distinct(ObPhyOperator& child_op, ObExecContext& ctx, const common::ObNewRow& compare_row,
      const common::ObNewRow*& row) const;
  int do_strict_distinct(ObPhyOperator& child_op, ObExecContext& ctx, const common::ObNewRow& compare_row,
      const common::ObNewRow*& row, int& cmp) const;
  /**
   * @brief for specified phy operator to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;

  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMergeSetOperator);

public:
  static const int64_t UNUSED_POS;

protected:
  static const int32_t CMP_DIRECTION_ASC = 1;
  static const int32_t CMP_DIRECTION_DESC = -1;
  common::ObFixedArray<int32_t, common::ObIAllocator> set_directions_;
  common::ObFixedArray<int64_t, common::ObIAllocator> cte_pseudo_column_row_desc_;
  common::ObFixedArray<int64_t, common::ObIAllocator> map_array_;
};
inline int ObMergeSetOperator::init_set_directions(int64_t count)
{
  return init_array_size<>(set_directions_, count);
}
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_SET_OPERATOR_H_ */
