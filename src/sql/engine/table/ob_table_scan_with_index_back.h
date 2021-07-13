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

#ifndef OCEANBASE_SRC_SQL_ENGINE_TABLE_OB_TABLE_SCAN_WITH_INDEX_BACK_H_
#define OCEANBASE_SRC_SQL_ENGINE_TABLE_OB_TABLE_SCAN_WITH_INDEX_BACK_H_
#include "sql/engine/table/ob_table_scan.h"
namespace oceanbase {
namespace sql {
class ObDomainIndex;
class ObTableScanWithIndexBack : public ObTableScan {
private:
  enum READ_ACTION { INVALID_ACTION, READ_ITERATOR, READ_TABLE_PARTITION, READ_ITER_END };

public:
  class ObTableScanWithIndexBackCtx : public ObTableScanCtx {
    friend class ObDomainIndex;
    friend class ObTableScanWithIndexBack;

  public:
    explicit ObTableScanWithIndexBackCtx(ObExecContext& ctx)
        : ObTableScanCtx(ctx), is_index_end_(false), use_table_allocator_(false), read_action_(INVALID_ACTION)
    {}

  private:
    bool is_index_end_;
    bool use_table_allocator_;
    READ_ACTION read_action_;
  };

public:
  explicit ObTableScanWithIndexBack(common::ObIAllocator& allocator);
  virtual ~ObTableScanWithIndexBack();
  inline void set_index_scan_tree(ObPhyOperator* index_scan_tree)
  {
    index_scan_tree_ = index_scan_tree;
  }
  inline const ObPhyOperator* get_index_scan_tree() const
  {
    return index_scan_tree_;
  }
  int rescan(ObExecContext& ctx) const;

protected:
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  int extract_range_from_index(ObExecContext& ctx) const;
  int do_table_scan_with_index(ObExecContext& ctx) const;
  int do_table_rescan_with_index(ObExecContext& ctx) const;
  int open_index_scan(ObExecContext& ctx) const;

private:
  ObPhyOperator* index_scan_tree_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_TABLE_OB_TABLE_SCAN_WITH_INDEX_BACK_H_ */
