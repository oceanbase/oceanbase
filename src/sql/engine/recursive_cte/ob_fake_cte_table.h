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

#ifndef _OB_FAKE_CTE_TABLE_H
#define _OB_FAKE_CTE_TABLE_H 1
#include "sql/engine/ob_phy_operator.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/table/ob_table_scan.h"

namespace oceanbase {
namespace sql {

class ObExecContext;
class ObFakeCTETable : public ObNoChildrenPhyOperator {
  OB_UNIS_VERSION_V(1);

public:
  class ObFakeCTETableOperatorCtx : public ObPhyOperatorCtx {
  public:
    explicit ObFakeCTETableOperatorCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx), empty_(false), pump_row_(nullptr), alloc_(ctx.get_allocator())
    {}
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }
    inline bool has_valid_data()
    {
      return !empty_;
    }
    int get_next_row(const common::ObNewRow*& row);
    int add_row(common::ObNewRow*& row);
    void reuse();

  public:
    bool empty_;
    common::ObNewRow* pump_row_;
    common::ObSEArray<int64_t, 64> column_involved_offset_;
    ObIAllocator& alloc_;
  };

public:
  explicit ObFakeCTETable(common::ObIAllocator& alloc) : ObNoChildrenPhyOperator(alloc), column_involved_offset_(alloc)
  {}
  virtual ~ObFakeCTETable()
  {}
  virtual void reset();
  virtual void reuse();

  int has_valid_data(ObExecContext& ctx, bool& result) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  OB_INLINE virtual bool need_filter_row() const
  {
    return true;
  }
  // virtual int get_next_row(ObExecContext &ctx, const common::ObNewRow *&row) const;
  virtual int rescan(ObExecContext& ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
  int open_self(ObExecContext& ctx) const;
  int set_empty(ObExecContext& exec_ctx) const;
  int add_row(ObExecContext& exec_ctx, common::ObNewRow*& row) const;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObFakeCTETable);

  // index refers to index in output_, and value refers to offset of column in cte table.
  common::ObFixedArray<int64_t, common::ObIAllocator> column_involved_offset_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_FAKE_CTE_TABLE_H */
