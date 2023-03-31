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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_SCAN_OP_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_SCAN_OP_H_
#include "sql/das/ob_das_task.h"
#include "storage/access/ob_dml_param.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
namespace oceanbase
{
namespace sql
{
class ObDASExtraData;
class ObLocalIndexLookupOp;

struct ObDASScanCtDef : ObDASBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScanCtDef(common::ObIAllocator &alloc)
    : ObDASBaseCtDef(DAS_OP_TABLE_SCAN),
      ref_table_id_(common::OB_INVALID_ID),
      access_column_ids_(alloc),
      schema_version_(-1),
      table_param_(alloc),
      pd_expr_spec_(alloc),
      aggregate_column_ids_(alloc),
      group_id_expr_(nullptr),
      result_output_(alloc),
      is_get_(false)
  { }
  //in das scan op, column described with column expr
  virtual bool has_expr() const override { return true; }
  virtual bool has_pdfilter_or_calc_expr() const override
  {
    return (!pd_expr_spec_.pushdown_filters_.empty() ||
            !pd_expr_spec_.calc_exprs_.empty());
  }
  virtual bool has_pl_udf() const override
  {
    bool has_pl_udf = false;
    for (int64_t i = 0; !has_pl_udf && i < pd_expr_spec_.calc_exprs_.count(); ++i) {
      const ObExpr *calc_expr = pd_expr_spec_.calc_exprs_.at(i);
      has_pl_udf = (calc_expr->type_ == T_FUN_UDF);
    }
    return has_pl_udf;
  }
  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       K_(ref_table_id),
                       K_(access_column_ids),
                       K_(aggregate_column_ids),
                       K_(schema_version),
                       K_(table_param),
                       K_(pd_expr_spec),
                       KPC_(group_id_expr),
                       K_(result_output),
                       K_(is_get));
  common::ObTableID ref_table_id_;
  UIntFixedArray access_column_ids_;
  int64_t schema_version_;
  share::schema::ObTableParam table_param_;
  ObPushdownExprSpec pd_expr_spec_;
  UIntFixedArray aggregate_column_ids_;
  ObExpr *group_id_expr_;
  //different from access expr, since we may eliminate some exprs
  //which could not be output in access exprs,
  //result_output_ indicate exprs that the storage layer will fill in the value
  sql::ExprFixedArray result_output_;
  bool is_get_;
};

struct ObDASScanRtDef : ObDASBaseRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASScanRtDef()
    : ObDASBaseRtDef(DAS_OP_TABLE_SCAN),
      p_row2exprs_projector_(nullptr),
      p_pd_expr_op_(nullptr),
      tenant_schema_version_(-1),
      limit_param_(),
      need_scn_(false),
      force_refresh_lc_(false),
      need_check_output_datum_(false),
      frozen_version_(-1),
      fb_snapshot_(transaction::ObTransVersion::INVALID_TRANS_VERSION),
      timeout_ts_(-1),
      tx_lock_timeout_(-1),
      sql_mode_(SMO_DEFAULT),
      scan_flag_(),
      pd_storage_flag_(false),
      stmt_allocator_("StmtScanAlloc"),
      scan_allocator_("TableScanAlloc"),
      sample_info_(nullptr)
  { }
  virtual ~ObDASScanRtDef();
  INHERIT_TO_STRING_KV("ObDASBaseRtDef", ObDASBaseRtDef,
                       K_(tenant_schema_version),
                       K_(limit_param),
                       K_(need_scn),
                       K_(force_refresh_lc),
                       K_(frozen_version),
                       K_(fb_snapshot),
                       K_(timeout_ts),
                       K_(tx_lock_timeout),
                       K_(sql_mode),
                       K_(scan_flag));
  int init_pd_op(ObExecContext &exec_ctx, const ObDASScanCtDef &scan_ctdef);
  storage::ObRow2ExprsProjector *p_row2exprs_projector_;
  ObPushdownOperator *p_pd_expr_op_;
  int64_t tenant_schema_version_;
  common::ObLimitParam limit_param_;
  bool need_scn_;
  bool force_refresh_lc_;
  bool need_check_output_datum_;
  int64_t frozen_version_;
  int64_t fb_snapshot_;
  int64_t timeout_ts_;
  int64_t tx_lock_timeout_;
  ObSQLMode sql_mode_;
  ObQueryFlag scan_flag_;
  int32_t pd_storage_flag_;
  common::ObWrapperAllocatorWithAttr stmt_allocator_;
  common::ObWrapperAllocatorWithAttr scan_allocator_;
  const common::SampleInfo *sample_info_; //Block(Row)SampleScan, only support local das scan
private:
  union {
    storage::ObRow2ExprsProjector row2exprs_projector_;
  };
  union {
    ObPushdownOperator pd_expr_op_;
  };
};

class ObDASScanOp : public ObIDASTaskOp
{
  friend class DASOpResultIter;
  OB_UNIS_VERSION(1);
public:
  ObDASScanOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASScanOp();

  virtual int open_op() override;
  virtual int release_op() override;
  storage::ObTableScanParam &get_scan_param() { return scan_param_; }
  const storage::ObTableScanParam &get_scan_param() const { return scan_param_; }
  virtual int decode_task_result(ObIDASTaskResult *task_result) override;
  virtual int fill_task_result(ObIDASTaskResult &task_result, bool &has_more) override;
  virtual int fill_extra_result() override;
  virtual int init_task_info() override { return common::OB_SUCCESS; }
  virtual int swizzling_remote_task(ObDASRemoteInfo *remote_info) override;
  virtual const ObDASBaseCtDef *get_ctdef() const override { return scan_ctdef_; }
  virtual ObDASBaseRtDef *get_rtdef() override { return scan_rtdef_; }
  bool need_check_output_datum() const { return scan_rtdef_->need_check_output_datum_; }
  virtual const ExprFixedArray &get_result_outputs() const
  { return get_lookup_ctdef() != nullptr ? get_lookup_ctdef()->result_output_ : scan_ctdef_->result_output_; }
  void set_scan_ctdef(const ObDASScanCtDef *scan_ctdef) { scan_ctdef_ = scan_ctdef; }
  void set_scan_rtdef(ObDASScanRtDef *scan_rtdef) { scan_rtdef_ = scan_rtdef; }
  //only used in local index lookup, it it nullptr when scan data table or scan index table
  int set_lookup_ctdef(const ObDASScanCtDef *lookup_ctdef)
  { related_ctdefs_.set_capacity(1); return related_ctdefs_.push_back(lookup_ctdef); }
  int set_lookup_rtdef(ObDASScanRtDef *lookup_rtdef)
  { related_rtdefs_.set_capacity(1); return related_rtdefs_.push_back(lookup_rtdef); }
  //only used in local index lookup, it it nullptr when scan data table or scan index table
  const ObDASScanCtDef *get_lookup_ctdef() const
  { return related_ctdefs_.empty() ? nullptr : static_cast<const ObDASScanCtDef*>(related_ctdefs_.at(0)); }
  ObDASScanRtDef *get_lookup_rtdef()
  { return related_rtdefs_.empty() ? nullptr : static_cast<ObDASScanRtDef*>(related_rtdefs_.at(0)); }
  int set_lookup_tablet_id(const common::ObTabletID &tablet_id);
  int init_scan_param();
  virtual int rescan();
  virtual int reuse_iter();
  virtual void reset_access_datums_ptr() override;
  virtual ObLocalIndexLookupOp *get_lookup_op();
  ObExpr *get_group_id_expr() { return scan_ctdef_->group_id_expr_; }
  bool is_group_scan() { return NULL != scan_ctdef_->group_id_expr_; }
  virtual bool need_all_output() { return false; }
  virtual int switch_scan_group() { return common::OB_SUCCESS; };
  INHERIT_TO_STRING_KV("parent", ObIDASTaskOp,
                       KPC_(scan_ctdef),
                       KPC_(scan_rtdef),
                       "scan_range", scan_param_.key_ranges_,
                       KPC_(result));
protected:
  common::ObITabletScan &get_tsc_service();
  virtual int do_local_index_lookup();
  virtual common::ObNewRowIterator *get_storage_scan_iter();
  virtual common::ObNewRowIterator *get_output_result_iter() { return result_; }
protected:
  //对于DASScanOp，本质上是对PartitionService的table_scan()接口的封装，
  //参数为scan_param,结果为result iterator
  storage::ObTableScanParam scan_param_;
  const ObDASScanCtDef *scan_ctdef_;
  ObDASScanRtDef *scan_rtdef_;
  common::ObNewRowIterator *result_;
  //Indicates the number of remaining rows currently that need to be sent through DTL
  int64_t remain_row_cnt_;
};

class ObDASScanResult : public ObIDASTaskResult, public common::ObNewRowIterator
{
  OB_UNIS_VERSION(1);
public:
  ObDASScanResult();
  virtual ~ObDASScanResult();
  virtual int init(const ObIDASTaskOp &op) override;
  virtual int reuse() override;
  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset() override;
  virtual int link_extra_result(ObDASExtraData &extra_result) override;
  int init_result_iter(const ExprFixedArray *output_exprs, ObEvalCtx *eval_ctx);
  ObChunkDatumStore &get_datum_store() { return datum_store_; }
  INHERIT_TO_STRING_KV("ObIDASTaskResult", ObIDASTaskResult,
                       K_(datum_store),
                       KPC_(output_exprs));
private:
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator result_iter_;
  const ExprFixedArray *output_exprs_;
  ObEvalCtx *eval_ctx_;
  ObDASExtraData *extra_result_;
  bool need_check_output_datum_;
};

class ObLocalIndexLookupOp : public common::ObNewRowIterator
{
protected:
  enum LookupState : int32_t
  {
    INDEX_SCAN,
    DO_LOOKUP,
    OUTPUT_ROWS,
    FINISHED
  };
public:
  ObLocalIndexLookupOp()
    : ObNewRowIterator(ObNewRowIterator::IterType::ObLocalIndexLookupIterator),
      lookup_ctdef_(nullptr),
      lookup_rtdef_(nullptr),
      index_ctdef_(nullptr),
      index_rtdef_(nullptr),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      rowkey_iter_(nullptr),
      lookup_iter_(),
      tablet_id_(),
      ls_id_(),
      scan_param_(),
      lookup_rowkey_cnt_(0),
      lookup_row_cnt_(0),
      lookup_memctx_(),
      status_(0)
  {}
  ObLocalIndexLookupOp(const ObNewRowIterator::IterType iter_type)
    : ObNewRowIterator(iter_type),
      lookup_ctdef_(nullptr),
      lookup_rtdef_(nullptr),
      index_ctdef_(nullptr),
      index_rtdef_(nullptr),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      rowkey_iter_(nullptr),
      lookup_iter_(),
      tablet_id_(),
      ls_id_(),
      scan_param_(),
      lookup_rowkey_cnt_(0),
      lookup_row_cnt_(0),
      lookup_memctx_(),
      status_(0)
  {}

  virtual ~ObLocalIndexLookupOp();

  int init(const ObDASScanCtDef *lookup_ctdef,
           ObDASScanRtDef *lookup_rtdef,
           const ObDASScanCtDef *index_ctdef,
           ObDASScanRtDef *index_rtdef,
           transaction::ObTxDesc *tx_desc,
           transaction::ObTxReadSnapshot *snapshot);
  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset() override { }

  // for lookup group scan
  virtual int64_t get_index_group_cnt() { return 0; }
  virtual int64_t get_lookup_group_cnt() { return 0; }
  virtual void inc_index_group_cnt() { /*do nothing*/ }
  virtual void inc_lookup_group_cnt() { /*do nothing*/ }
  virtual int init_group_range(int64_t cur_group_idx, int64_t group_size)
  { return common::OB_NOT_IMPLEMENT; }
  virtual bool need_next_index_batch() const;
  virtual int switch_rowkey_scan_group() { return common::OB_NOT_IMPLEMENT; }
  virtual int switch_lookup_scan_group() { return common::OB_NOT_IMPLEMENT; }
  virtual ObNewRowIterator *&get_lookup_storage_iter() { return lookup_iter_; }
  virtual ObNewRowIterator *get_lookup_iter() { return lookup_iter_; }
  int check_lookup_row_cnt();
  void set_is_group_scan(bool v) { is_group_scan_ = v; }
  bool is_group_scan() { return is_group_scan_; }
  // for lookup group scan end

  void set_tablet_id(const common::ObTabletID &tablet_id) { tablet_id_ = tablet_id; }
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  void set_rowkey_iter(common::ObNewRowIterator *rowkey_iter) {rowkey_iter_ = rowkey_iter;}
  common::ObNewRowIterator *get_rowkey_iter() { return rowkey_iter_; }
  int reuse_iter();
  int reset_lookup_state();
  int revert_iter();
  VIRTUAL_TO_STRING_KV(KPC_(lookup_ctdef),
                       KPC_(lookup_rtdef),
                       KPC_(tx_desc),
                       KPC_(snapshot),
                       K_(tablet_id),
                       K_(ls_id),
                       K_(state),
                       K_(index_end));
private:
  int init_scan_param();
  int process_data_table_rowkey();
  int process_data_table_rowkeys(int64_t batch_count);
  int do_index_lookup();
  common::ObITabletScan &get_tsc_service();
private:
  const static int64_t DEFAULT_BATCH_ROW_COUNT = 1000;
protected:
  const ObDASScanCtDef *lookup_ctdef_; //lookup ctdef
  ObDASScanRtDef *lookup_rtdef_; //lookup rtdef
  const ObDASScanCtDef *index_ctdef_;
  ObDASScanRtDef *index_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  // for group scan:
  //      local das :
  //        rowkey_iter_ is ObGroupScanIter
  //      remote das :
  //        local server: rowkey_iter_ not used
  //        remote server: rowkey_iter_ is ObGroupScanIter
  // for normal scan:
  //      local das :
  //        rowkey_iter_ is storage_iter
  //      remote das:
  //        local server: rowkey_iter_ not used
  //        remote server: rowkey_iter_ is storage_iter
  common::ObNewRowIterator *rowkey_iter_;
  // for group scan:
  //     local das:
  //       lookup_iter_ is ObGroupScanIter
  //     remote das:
  //       local server:
  //         lookup_iter_ is ObGroupScanIter, and the input of ObGroupScanIter is ObDASScanResult
  //       remote server:
  //         lookup_iter_ is ObGroupScanIter, and the input of ObGroupScanIter is storage iter,
  //         Here, TODO shengle: the lookup_iter_ can use storage iter directly for opt;
  // for normal scan,
  //     local das: lookup_iter_ is storage_iter
  //     remote das:
  //       local server: lookup_iter_ not used
  //       remote server: lookup_iter_ is storage_iter
  common::ObNewRowIterator *lookup_iter_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  storage::ObTableScanParam scan_param_;
  int64_t lookup_rowkey_cnt_;
  int32_t lookup_row_cnt_;
  lib::MemoryContext lookup_memctx_;
  union {
    uint32_t status_;
    struct {
      uint32_t state_             : 8; // index lookup state
      uint32_t index_end_         : 1; // if index reach iterator end
      uint32_t is_group_scan_     : 1;
      //add status here
    };
  };
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_SCAN_OP_H_ */
