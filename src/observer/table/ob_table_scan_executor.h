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

#ifndef OCEANBASE_OBSERVER_TABLE_OB_TABLE_SCAN_EXECUTOR_H_
#define OCEANBASE_OBSERVER_TABLE_OB_TABLE_SCAN_EXECUTOR_H_

#include "ob_table_executor.h" // for ObTableApiExecutor

namespace oceanbase
{
namespace table
{
class ObTableGlobalIndexLookupExecutor;
class ObTableApiScanSpec : public ObTableApiSpec
{
public:
  ObTableApiScanSpec(common::ObIAllocator &allocator, const ObTableExecutorType type)
      : ObTableApiSpec(allocator, type),
        tsc_ctdef_(allocator)
  {
  }
  virtual ~ObTableApiScanSpec() {}
public:
  // getter
  OB_INLINE ObTableApiScanCtDef& get_ctdef() { return tsc_ctdef_; }
  OB_INLINE const ObTableApiScanCtDef& get_ctdef() const { return tsc_ctdef_; }
private:
  ObTableApiScanCtDef tsc_ctdef_;
};

class ObTableApiScanExecutor : public ObTableApiExecutor
{
  friend class ObTableGlobalIndexLookupExecutor;
public:
  ObTableApiScanExecutor(ObTableCtx &ctx, const ObTableApiScanSpec &spec)
      : ObTableApiExecutor(ctx),
        scan_spec_(spec),
        tsc_rtdef_(allocator_),
        das_ref_(eval_ctx_, ctx.get_exec_ctx()),
        global_index_lookup_executor_(nullptr)
  {
    scan_ops_.set_attr(ObMemAttr(MTL_ID(), "KvscanOps"));
    reset();
  }
  virtual ~ObTableApiScanExecutor() { destroy(); }
  int open() override;
  int get_next_row() override;
  int close() override;
  virtual int rescan();
  int close_and_reopen();
  int local_iter_rescan();
  void destroy() override;
  virtual void clear_evaluated_flag() override;
public:
  OB_INLINE const ObTableApiScanSpec& get_spec() const { return scan_spec_; }
  OB_INLINE common::ObIAllocator& get_allocator() { return allocator_; }
  OB_INLINE void reset()
  {
    scan_ops_.reset();
    input_row_cnt_ = 0;
    output_row_cnt_ = 0;
    need_do_init_ = true;
  }
protected:
  const ObTableApiScanSpec &scan_spec_;
  ObTableApiScanRtDef tsc_rtdef_;
  sql::DASOpResultIter scan_result_;
  sql::ObDASRef das_ref_;
  common::ObSEArray<sql::ObDASScanOp*, 4> scan_ops_;
  int64_t input_row_cnt_;
  int64_t output_row_cnt_;
  bool need_do_init_;
  ObTableGlobalIndexLookupExecutor *global_index_lookup_executor_;
private:
  int init_tsc_rtdef();
  int init_das_scan_rtdef(const sql::ObDASScanCtDef &das_ctdef,
                          sql::ObDASScanRtDef &das_rtdef,
                          const sql::ObDASTableLocMeta *loc_meta);
  int init_attach_scan_rtdef(const sql::ObDASBaseCtDef *attach_ctdef,
                             sql::ObDASBaseRtDef *&attach_rtdef);
  int pushdown_normal_lookup_to_das(sql::ObDASScanOp &target_op);
  int pushdown_attach_task_to_das(sql::ObDASScanOp &target_op);
  int attach_related_taskinfo(sql::ObDASScanOp &target_op, sql::ObDASBaseRtDef *attach_rtdef);
  int do_init_before_get_row();
  int prepare_scan_range();
  int prepare_das_task();
  int prepare_multi_tablets_das_task();
  int prepare_single_tablet_das_task(const common::ObTabletID &tablet_id, const common::ObNewRange *range);
  int do_table_scan();
  int get_next_row_with_das();
  int check_filter(bool &filter);
  int get_next_row_for_tsc();
  int gen_scan_ranges(common::ObIArray<common::ObNewRange> &scan_ranges);
  bool has_das_scan_task(const sql::ObDASTabletLoc *tablet_loc, sql::ObDASScanOp *&das_op)
  {
    das_op = static_cast<sql::ObDASScanOp*>(das_ref_.find_das_task( tablet_loc, DAS_OP_TABLE_SCAN));
    return das_op != nullptr;
  }
  int write_search_text_datum();
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiScanExecutor);
};

class ObTableApiScanRowIterator
{
public:
  ObTableApiScanRowIterator()
      : scan_executor_(nullptr),
        row_allocator_("TbScanRowIter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        is_opened_(false)
  {
  }
  virtual ~ObTableApiScanRowIterator() {};
public:
  virtual int open(ObTableApiScanExecutor *executor);
  virtual int get_next_row(common::ObNewRow *&row);
  virtual int get_next_row(common::ObNewRow *&row, common::ObIAllocator &allocator);
  virtual ObTableApiScanExecutor *get_scan_executor() { return scan_executor_; };
  virtual int close();
private:
  int adjust_output_obj_type(ObObj &obj);
protected:
  ObTableApiScanExecutor *scan_executor_;
  common::ObArenaAllocator row_allocator_; // alloc the memory of result row
  bool is_opened_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiScanRowIterator);
};

} // table
} // oceanbase
#endif // OCEANBASE_OBSERVER_TABLE_OB_TABLE_SCAN_EXECUTOR_H_
