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
#include "sql/engine/table/ob_index_lookup_op_impl.h"
#include "sql/das/ob_group_scan_iter.h"
#include "sql/das/iter/ob_das_iter.h"

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
      group_by_column_ids_(alloc),
      group_id_expr_(nullptr),
      result_output_(alloc),
      is_get_(false),
      is_external_table_(false),
      external_file_access_info_(alloc),
      external_file_location_(alloc),
      external_files_(alloc),
      external_file_format_str_(alloc),
      trans_info_expr_(nullptr),
      ir_scan_type_(ObTSCIRScanType::OB_NOT_A_SPEC_SCAN),
      rowkey_exprs_(alloc),
      table_scan_opt_(),
      doc_id_idx_(-1),
      vec_vid_idx_(-1)
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
                       K_(group_by_column_ids),
                       K_(schema_version),
                       K_(table_param),
                       K_(pd_expr_spec),
                       KPC_(group_id_expr),
                       K_(result_output),
                       K_(is_get),
                       K_(is_external_table),
                       K_(external_files),
                       K_(external_file_format_str),
                       K_(external_file_location),
                       KPC_(trans_info_expr),
                       K_(ir_scan_type),
                       K_(rowkey_exprs),
                       K_(table_scan_opt));
  common::ObTableID ref_table_id_;
  UIntFixedArray access_column_ids_;
  int64_t schema_version_;
  share::schema::ObTableParam table_param_;
  ObPushdownExprSpec pd_expr_spec_;
  UIntFixedArray aggregate_column_ids_;
  UIntFixedArray group_by_column_ids_;
  ObExpr *group_id_expr_;
  //different from access expr, since we may eliminate some exprs
  //which could not be output in access exprs,
  //result_output_ indicate exprs that the storage layer will fill in the value
  sql::ExprFixedArray result_output_;
  bool is_get_;
  bool is_external_table_;
  ObExternalFileFormat::StringData external_file_access_info_;
  ObExternalFileFormat::StringData external_file_location_;
  ExternalFileNameArray external_files_; //for external table scan TODO jim.wjh remove
  ObExternalFileFormat::StringData external_file_format_str_;
  ObExpr *trans_info_expr_; // transaction information pseudo-column
  ObTSCIRScanType ir_scan_type_; // specify retrieval scan type
  sql::ExprFixedArray rowkey_exprs_; // store rowkey exprs for index lookup
  ObTableScanOption table_scan_opt_;
  int64_t doc_id_idx_;
  int64_t vec_vid_idx_;
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
      fb_read_tx_uncommitted_(false),
      frozen_version_(-1),
      fb_snapshot_(),
      timeout_ts_(-1),
      tx_lock_timeout_(-1),
      sql_mode_(SMO_DEFAULT),
      scan_flag_(),
      pd_storage_flag_(false),
      stmt_allocator_("StmtScanAlloc"),
      scan_allocator_("TableScanAlloc"),
      sample_info_(nullptr),
      is_for_foreign_check_(false)
  { }
  virtual ~ObDASScanRtDef();
  bool enable_rich_format() const { return scan_flag_.enable_rich_format_; }
  INHERIT_TO_STRING_KV("ObDASBaseRtDef", ObDASBaseRtDef,
                       K_(tenant_schema_version),
                       K_(limit_param),
                       K_(need_scn),
                       K_(force_refresh_lc),
                       K_(frozen_version),
                       K_(fb_snapshot),
                       K_(fb_read_tx_uncommitted),
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
  bool fb_read_tx_uncommitted_;
  int64_t frozen_version_;
  share::SCN fb_snapshot_;
  int64_t timeout_ts_;
  int64_t tx_lock_timeout_;
  ObSQLMode sql_mode_;
  ObQueryFlag scan_flag_;
  int32_t pd_storage_flag_;
  common::ObWrapperAllocatorWithAttr stmt_allocator_;
  common::ObWrapperAllocatorWithAttr scan_allocator_;
  const common::SampleInfo *sample_info_; //Block(Row)SampleScan, only support local das scan
  bool is_for_foreign_check_;
private:
  union {
    storage::ObRow2ExprsProjector row2exprs_projector_;
  };
  union {
    ObPushdownOperator pd_expr_op_;
  };
};

struct ObDASObsoletedObj
{
  OB_UNIS_VERSION(1);
public:
  ObDASObsoletedObj() : flag_(false) {}
  TO_STRING_KV(K_(flag));
  bool flag_;
};

class ObDASScanOp : public ObIDASTaskOp
{
  friend class DASOpResultIter;
  friend class ObDASMergeIter;
  OB_UNIS_VERSION(1);
public:
  ObDASScanOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASScanOp();

  virtual int open_op() override;
  virtual int release_op() override;
  storage::ObTableScanParam &get_scan_param() { return scan_param_; }
  const storage::ObTableScanParam &get_scan_param() const { return scan_param_; }

  int init_related_tablet_ids(ObDASRelatedTabletID &related_tablet_ids);

  virtual int decode_task_result(ObIDASTaskResult *task_result) override;
  virtual int fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit) override;
  virtual int fill_extra_result() override;
  virtual int init_task_info(uint32_t row_extend_size) override;
  virtual int swizzling_remote_task(ObDASRemoteInfo *remote_info) override;
  virtual const ObDASBaseCtDef *get_ctdef() const override { return scan_ctdef_; }
  virtual ObDASBaseRtDef *get_rtdef() override { return scan_rtdef_; }
  bool need_check_output_datum() const { return scan_rtdef_->need_check_output_datum_; }
  virtual const ExprFixedArray &get_result_outputs() const;
  void set_scan_ctdef(const ObDASScanCtDef *scan_ctdef) { scan_ctdef_ = scan_ctdef; }
  void set_scan_rtdef(ObDASScanRtDef *scan_rtdef) { scan_rtdef_ = scan_rtdef; }
  int reserve_related_buffer(const int64_t related_scan_cnt);
  int set_related_task_info(const ObDASBaseCtDef *attach_ctdef,
                            ObDASBaseRtDef *attach_rtdef,
                            const common::ObTabletID &tablet_id);
  //only used in local index lookup, it it nullptr when scan data table or scan index table
  const ObDASScanCtDef *get_lookup_ctdef() const;
  ObDASScanRtDef *get_lookup_rtdef();
  int get_aux_lookup_tablet_id(common::ObTabletID &tablet_id) const;
  int get_table_lookup_tablet_id(common::ObTabletID &tablet_id) const;
  int init_scan_param();
  int rescan();
  int reuse_iter();
  void reset_access_datums_ptr(int64_t capacity = 0);
  ObLocalIndexLookupOp *get_lookup_op();
  bool is_contain_trans_info() {return NULL != scan_ctdef_->trans_info_expr_; }
  int do_table_scan();
  int do_domain_index_lookup();
  int get_text_ir_tablet_ids(
      common::ObTabletID &inv_idx_tablet_id,
      common::ObTabletID &fwd_idx_tablet_id,
      common::ObTabletID &doc_id_idx_tablet_id);
  bool enable_rich_format() const { return scan_rtdef_->enable_rich_format(); }
  INHERIT_TO_STRING_KV("parent", ObIDASTaskOp,
                       KPC_(scan_ctdef),
                       KPC_(scan_rtdef),
                       "scan_range", scan_param_.key_ranges_,
                       KPC_(result),
                       "scan_flag", scan_param_.scan_flag_);
protected:
  common::ObITabletScan &get_tsc_service();
  int do_local_index_lookup();
  common::ObNewRowIterator *get_storage_scan_iter();
  common::ObNewRowIterator *get_output_result_iter() { return result_; }
  ObDASIterTreeType get_iter_tree_type() const;
public:
  ObSEArray<ObDatum *, 4> trans_info_array_;
protected:
  void init_retry_alloc()
  {
    if (nullptr == retry_alloc_) {
      ObMemAttr attr;
      attr.tenant_id_ = MTL_ID();
      attr.label_ = "RetryDASCtx";
      retry_alloc_ = new(&retry_alloc_buf_) common::ObArenaAllocator();
      retry_alloc_->set_attr(attr);
    }
  }
  //对于DASScanOp，本质上是对PartitionService的table_scan()接口的封装，
  //参数为scan_param,结果为result iterator
  storage::ObTableScanParam scan_param_;
  const ObDASScanCtDef *scan_ctdef_;
  ObDASScanRtDef *scan_rtdef_;
  // result_ is actually a ObDASIter during execution
  common::ObNewRowIterator *result_;
  //Indicates the number of remaining rows currently that need to be sent through DTL
  int64_t remain_row_cnt_;
  // only can be used in runner server
  ObDASRelatedTabletID tablet_ids_;

  common::ObArenaAllocator *retry_alloc_;
  union {
    common::ObArenaAllocator retry_alloc_buf_;
  };
  ObDASObsoletedObj ir_param_;   // FARM COMPAT WHITELIST: obsoleted attribute, please gc me at next barrier version
};

class ObDASScanResult : public ObIDASTaskResult, public common::ObNewRowIterator
{
  OB_UNIS_VERSION(1);
public:
  ObDASScanResult();
  virtual ~ObDASScanResult();
  virtual int init(const ObIDASTaskOp &op, common::ObIAllocator &alloc) override;
  virtual int reuse() override;
  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset() override;
  virtual int link_extra_result(ObDASExtraData &extra_result) override;
  int init_result_iter(const ExprFixedArray *output_exprs, ObEvalCtx *eval_ctx);
  ObChunkDatumStore &get_datum_store() { return datum_store_; }
  ObTempRowStore &get_vec_row_store() { return vec_row_store_; }
  INHERIT_TO_STRING_KV("ObIDASTaskResult", ObIDASTaskResult,
                       K_(datum_store),
                       KPC_(output_exprs),
                       K_(enable_rich_format),
                       K_(vec_row_store),
                       K_(io_read_bytes),
                       K_(ssstore_read_bytes),
                       K_(ssstore_read_row_cnt),
                       K_(memstore_read_row_cnt));
private:
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator result_iter_;
  ObTempRowStore vec_row_store_;
  ObTempRowStore::Iterator vec_result_iter_;
  const ExprFixedArray *output_exprs_;
  ObEvalCtx *eval_ctx_;
  ObDASExtraData *extra_result_;
  bool need_check_output_datum_;
  bool enable_rich_format_;
  int64_t io_read_bytes_;
  int64_t ssstore_read_bytes_;
  int64_t ssstore_read_row_cnt_;
  int64_t memstore_read_row_cnt_;
};

class ObLocalIndexLookupOp : public common::ObNewRowIterator, public ObIndexLookupOpImpl
{
public:
  ObLocalIndexLookupOp()
    : ObNewRowIterator(ObNewRowIterator::IterType::ObLocalIndexLookupIterator),
      ObIndexLookupOpImpl(LOCAL_INDEX, 1000 /*default_batch_row_count */),
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

  virtual void do_clear_evaluated_flag() override {index_rtdef_->p_pd_expr_op_->clear_evaluated_flag();}
  virtual int reset_lookup_state() override;
  virtual int get_next_row_from_index_table() override;
  virtual int get_next_rows_from_index_table(int64_t &count, int64_t capacity) override;
  virtual int process_data_table_rowkey() override;
  virtual int process_data_table_rowkeys(const int64_t size, const ObBitVector *skip) override;
  virtual int do_index_lookup() override;
  virtual int get_next_row_from_data_table() override;
  virtual int get_next_rows_from_data_table(int64_t &count, int64_t capacity) override;
  virtual int check_lookup_row_cnt() override;

  virtual ObEvalCtx & get_eval_ctx() override {return *(lookup_rtdef_->eval_ctx_);}
  virtual const ExprFixedArray & get_output_expr() override {return  lookup_ctdef_->pd_expr_spec_.access_exprs_; }
  ObNewRowIterator *&get_lookup_storage_iter() { return lookup_iter_; }
  ObNewRowIterator *get_lookup_iter() { return lookup_iter_; }
  void set_is_group_scan(bool v) { is_group_scan_ = v; }
  bool is_group_scan() const { return is_group_scan_; }
  void set_tablet_id(const common::ObTabletID &tablet_id) { tablet_id_ = tablet_id; }
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  void set_rowkey_iter(common::ObNewRowIterator *rowkey_iter) {rowkey_iter_ = rowkey_iter;}
  common::ObNewRowIterator *get_rowkey_iter() { return rowkey_iter_; }
  int reuse_iter();
  virtual int revert_iter();
  VIRTUAL_TO_STRING_KV(KPC_(lookup_ctdef),
                       KPC_(lookup_rtdef),
                       KPC_(tx_desc),
                       KPC_(snapshot),
                       K_(tablet_id),
                       K_(ls_id),
                       K_(state),
                       K_(index_end));
  common::ObITabletScan &get_tsc_service();
protected:
  virtual int init_scan_param();
protected:
  void print_trans_info_and_key_range_();
protected:
  const ObDASScanCtDef *lookup_ctdef_; //lookup ctdef
  ObDASScanRtDef *lookup_rtdef_; //lookup rtdef
  const ObDASScanCtDef *index_ctdef_;
  ObDASScanRtDef *index_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  // Local index lookup is executed within a DAS task, whether executed locally or remotely,
  // both index scan and lookup are completed on the same machine.
  common::ObNewRowIterator *rowkey_iter_;
  common::ObNewRowIterator *lookup_iter_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  storage::ObTableScanParam scan_param_;

  ObSEArray<ObDatum *, 4> trans_info_array_;
  lib::MemoryContext lookup_memctx_;
  union {
    uint32_t status_;
    struct {
      uint32_t is_group_scan_     : 1;
      //add status here
    };
  };
};

// NOTE: ObDASGroupScanOp defined here is For cross-version compatibility， and it will be removed in future barrier-version;
// For das remote execution in upgrade stage,
//   1. ctrl(4.2.1) -> executor(4.2.3):
//        the executor will execute group scan task as the logic of das scan op, and return the result to ctr;
//   2. ctrl(4.2.3) -> executor(4.2.1):
//        the ctrl will send group scan task to executor to ensure exectuor will execute succeed;
class ObDASGroupScanOp : public ObDASScanOp
{
  OB_UNIS_VERSION(1);
public:
  ObDASGroupScanOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASGroupScanOp();
  void init_group_range(int64_t cur_group_idx, int64_t group_size);
private:
  ObGroupScanIter iter_;
  int64_t cur_group_idx_;
  int64_t group_size_;
};


}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_SCAN_OP_H_ */
