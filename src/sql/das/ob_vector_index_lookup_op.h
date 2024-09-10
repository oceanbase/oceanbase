/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OBDEV_SRC_SQL_DAS_OB_VECTOR_INDEX_LOOKUP_OP_H_
#define OBDEV_SRC_SQL_DAS_OB_VECTOR_INDEX_LOOKUP_OP_H_
#include "sql/das/ob_domain_index_lookup_op.h"
#include "src/share/vector_index/ob_plugin_vector_index_service.h"
namespace oceanbase
{
namespace sql
{

enum ObVidAdaLookupStatus
{
  STATES_INIT,
  QUERY_INDEX_ID_TBL,
  QUERY_SNAPSHOT_TBL,
  QUERY_ROWKEY_VEC,
  STATES_END,
  STATES_ERROR
};

class ObVectorIndexLookupOp : public ObDomainIndexLookupOp
{
public:
  ObVectorIndexLookupOp(ObIAllocator &allocator)
  : ObDomainIndexLookupOp(allocator),
    aux_lookup_iter_(nullptr),
    adaptor_vid_iter_(nullptr),
    search_vec_(nullptr),
    delta_buf_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    index_id_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    snapshot_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    delta_buf_scan_param_(),
    index_id_scan_param_(),
    snapshot_scan_param_(),
    com_aux_vec_scan_param_(),
    delta_buf_iter_(nullptr),
    index_id_iter_(nullptr),
    snapshot_iter_(nullptr),
    com_aux_vec_iter_(nullptr),
    delta_buf_ctdef_(nullptr),
    delta_buf_rtdef_(nullptr),
    index_id_ctdef_(nullptr),
    index_id_rtdef_(nullptr),
    snapshot_ctdef_(nullptr),
    snapshot_rtdef_(nullptr),
    com_aux_vec_ctdef_(nullptr),
    com_aux_vec_rtdef_(nullptr),
    vec_eval_ctx_(nullptr),
    limit_param_(),
    sort_ctdef_(nullptr),
    sort_rtdef_(nullptr),
    is_inited_(false),
    vec_index_param_(),
    dim_(0) {}
  virtual ~ObVectorIndexLookupOp() {};
  int init(const ObDASBaseCtDef *table_lookup_ctdef,
            ObDASBaseRtDef *table_lookup_rtdef,
            transaction::ObTxDesc *tx_desc,
            transaction::ObTxReadSnapshot *snapshot,
            storage::ObTableScanParam &scan_param); // init for OP
  virtual int reset_lookup_state() override;
  virtual int do_aux_table_lookup();
  virtual int revert_iter() override;
  virtual void do_clear_evaluated_flag() override;
  int reuse_scan_iter(bool need_switch_param);
  void set_aux_lookup_iter(common::ObNewRowIterator *aux_lookup_iter)
  {
    aux_lookup_iter_ = aux_lookup_iter;
  }
  void set_aux_table_id(ObTabletID delta_buf_tablet_id,
                        ObTabletID index_id_tablet_id,
                        ObTabletID snapshot_tablet_id,
                        ObTabletID com_aux_vec_tablet_id)
  {
    delta_buf_tablet_id_ = delta_buf_tablet_id;
    index_id_tablet_id_ = index_id_tablet_id;
    snapshot_tablet_id_ = snapshot_tablet_id;
    com_aux_vec_tablet_id_ = com_aux_vec_tablet_id;
  }
  void set_sort_ctdef(const ObDASSortCtDef *sort_ctdef) { sort_ctdef_ = sort_ctdef;}
  void set_sort_rtdef(ObDASSortRtDef *sort_rtdef) { sort_rtdef_ = sort_rtdef;}
  int init_sort(const ObDASVecAuxScanCtDef *ir_ctdef,
                  ObDASVecAuxScanRtDef *ir_rtdef);
  int init_limit(const ObDASVecAuxScanCtDef *ir_ctdef,
                ObDASVecAuxScanRtDef *ir_rtdef);
  int revert_iter_for_complete_data();
  int prepare_state(const ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext &ada_ctx);
  int vector_do_index_lookup();
  int get_cmpt_aux_table_rowkey();
  int do_vid_rowkey_table_scan();
  int set_com_main_table_lookup_key();
  void set_dim(int64_t dim) {dim_ = dim;}
  int set_vec_index_param(ObString vec_index_param) { return ob_write_string(*allocator_, vec_index_param, vec_index_param_); }
protected:
  virtual int fetch_index_table_rowkey() override;
  virtual int fetch_index_table_rowkeys(int64_t &count, const int64_t capacity) override;
  virtual int get_aux_table_rowkey() override;
  virtual int get_aux_table_rowkeys(const int64_t lookup_row_cnt) override;
private:
  int init_delta_buffer_scan_param();
  int init_index_id_scan_param();
  int init_snapshot_scan_param();
  int init_vid_rowkey_scan_param();
  int init_com_aux_vec_scan_param();
  int process_adaptor_state();
  int prepare_state(const ObVidAdaLookupStatus& cur_state);
  int call_pva_interface(const ObVidAdaLookupStatus& cur_state,
                        ObVectorQueryAdaptorResultContext& ada_ctx,
                        ObPluginVectorIndexAdaptor &adaptor);
  int next_state(ObVidAdaLookupStatus& cur_states,
                  ObVectorQueryAdaptorResultContext& ada_ctx,
                  bool& is_continue);
  int set_lookup_vid_key();
  int set_lookup_vid_key(ObRowkey& doc_id_rowkey);
  int set_lookup_vid_keys(ObNewRow *row, int64_t count);
  int set_main_table_lookup_key();
  static int init_base_idx_scan_param(const share::ObLSID &ls_id,
                                      const common::ObTabletID &tablet_id,
                                      const sql::ObDASScanCtDef *ctdef,
                                      sql::ObDASScanRtDef *rtdef,
                                      transaction::ObTxDesc *tx_desc,
                                      transaction::ObTxReadSnapshot *snapshot,
                                      ObTableScanParam &scan_param,
                                      bool reverse_order = false);
  int gen_scan_range(const int64_t obj_cnt, common::ObTableID table_id, ObNewRange &scan_range);
  int set_vector_query_condition(ObVectorQueryConditions &query_cond);
private:
  static const int64_t DELTA_BUF_PRI_KEY_CNT = 2;
  static const int64_t INDEX_ID_PRI_KEY_CNT = 3;
  static const int64_t SNAPSHOT_PRI_KEY_CNT = 1;
  static const uint64_t MAX_VSAG_QUERY_RES_SIZE = 16384;
private:
  common::ObNewRowIterator *aux_lookup_iter_;
  ObVectorQueryVidIterator* adaptor_vid_iter_;
  ObExpr* search_vec_;
  ObTabletID delta_buf_tablet_id_;
  ObTabletID index_id_tablet_id_;
  ObTabletID snapshot_tablet_id_;
  ObTabletID com_aux_vec_tablet_id_;
  // delte buffer table scan
  ObTableScanParam delta_buf_scan_param_;
  // index id table scan
  ObTableScanParam index_id_scan_param_;
  // snapshot table scan
  ObTableScanParam snapshot_scan_param_;
  // aux vector table scan
  ObTableScanParam com_aux_vec_scan_param_;
  common::ObNewRowIterator *delta_buf_iter_;
  common::ObNewRowIterator *index_id_iter_;
  common::ObNewRowIterator *snapshot_iter_;
  common::ObNewRowIterator *com_aux_vec_iter_;
  const ObDASScanCtDef *delta_buf_ctdef_;
  ObDASScanRtDef *delta_buf_rtdef_;
  const ObDASScanCtDef *index_id_ctdef_;
  ObDASScanRtDef *index_id_rtdef_;
  const ObDASScanCtDef *snapshot_ctdef_;
  ObDASScanRtDef *snapshot_rtdef_;
  const ObDASScanCtDef *com_aux_vec_ctdef_;
  ObDASScanRtDef *com_aux_vec_rtdef_;
  ObEvalCtx *vec_eval_ctx_;
  common::ObLimitParam limit_param_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;
  // init
  bool is_inited_;
  ObString vec_index_param_;
  int64_t dim_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_VECTOR_INDEX_LOOKUP_OP_H_ */
