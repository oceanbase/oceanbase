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

#ifndef OCEANBASE_ENGINE_PX_OB_PX_SCHUDULER_H_
#define OCEANBASE_ENGINE_PX_OB_PX_SCHUDULER_H_

#include "sql/engine/px/exchange/ob_receive_op.h"
#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/datahub/ob_dh_msg_ctx.h"
#include "sql/engine/px/datahub/components/ob_dh_rollup_key.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/components/ob_dh_range_dist_wf.h"
#include "sql/engine/px/datahub/components/ob_dh_second_stage_reporting_wf.h"
#include "sql/engine/px/datahub/components/ob_dh_opt_stats_gather.h"

namespace oceanbase
{
namespace sql
{

class ObPxCoordOp;
class ObPxObDfoMgr;
class ObPxRootDfoAction
{
public:
  virtual int receive_channel_root_dfo(
    ObExecContext &ctx, ObDfo &parent, ObPxTaskChSets &parent_ch_sets) = 0;
  virtual int receive_channel_root_dfo(
      ObExecContext &ctx, ObDfo &parent, dtl::ObDtlChTotalInfo &ch_info) = 0;
  virtual int notify_peers_mock_eof(
      ObDfo *dfo, int64_t timeout_ts, common::ObAddr addr) const = 0;

};

enum class TableAccessType {
  NO_TABLE,
  PURE_VIRTUAL_TABLE,
  HAS_USER_TABLE
};

// for runtime filter, jf create op must scheduled earlier than jf use op
struct RuntimeFilterDependencyInfo
{
public:
  RuntimeFilterDependencyInfo() : rf_create_ops_(), rf_use_ops_() {}
  ~RuntimeFilterDependencyInfo() = default;
  void destroy()
  {
    rf_create_ops_.reset();
    rf_use_ops_.reset();
  }
  inline bool is_empty() const {
    return rf_create_ops_.empty() && rf_use_ops_.empty();
  }
  int describe_dependency(ObDfo *root_dfo);
public:
  ObTMArray<const ObOpSpec *> rf_create_ops_;
  ObTMArray<const ObOpSpec *> rf_use_ops_;
};

struct ObP2PDfoMapNode
{
  ObP2PDfoMapNode() : target_dfo_id_(OB_INVALID_ID),  addrs_() {}
  ~ObP2PDfoMapNode() { addrs_.reset(); }
  int assign(const ObP2PDfoMapNode &other) {
    target_dfo_id_ = other.target_dfo_id_;
    return addrs_.assign(other.addrs_);
  }
  void reset() {
    target_dfo_id_ = OB_INVALID_ID;
    addrs_.reset();
  }
  int64_t target_dfo_id_;
  common::ObSArray<ObAddr>addrs_;
  TO_STRING_KV(K(target_dfo_id_), K(addrs_));
};
struct ObTempTableP2PInfo
{
  ObTempTableP2PInfo() : temp_access_ops_(),  dfos_() {}
  ~ObTempTableP2PInfo() { reset(); }
  void reset() {
    temp_access_ops_.reset();
    dfos_.reset();
  }
  ObSEArray<const ObOpSpec *, 4> temp_access_ops_;
  ObSEArray<ObDfo *, 4> dfos_;
  TO_STRING_KV(K(temp_access_ops_), K(dfos_));
};
// 这些信息是调度时候需要用的变量，暂时统一叫做CoordInfo
class ObPxCoordInfo
{
public:
  ObPxCoordInfo(ObPxCoordOp &coord,
                ObIAllocator &allocator,
                dtl::ObDtlChannelLoop &msg_loop,
                ObInterruptibleTaskID &interrupt_id)
  : dfo_mgr_(allocator),
    rpc_proxy_(),
    all_threads_finish_(false),
    first_error_code_(common::OB_SUCCESS),
    msg_loop_(msg_loop),
    interrupt_id_(interrupt_id),
    coord_(coord),
    batch_rescan_ctl_(NULL),
    pruning_table_location_(NULL),
    table_access_type_(TableAccessType::NO_TABLE),
    qc_detectable_id_(),
    p2p_dfo_map_(),
    p2p_temp_table_info_(),
    rf_dpd_info_()
  {}
  virtual ~ObPxCoordInfo() {}
  virtual void destroy()
  {
    dfo_mgr_.destroy();
    piece_msg_ctx_mgr_.reset();
    p2p_dfo_map_.destroy();
    p2p_temp_table_info_.reset();
    rf_dpd_info_.destroy();
  }
  void reset_for_rescan()
  {
    all_threads_finish_ = false;
    dfo_mgr_.destroy();
    piece_msg_ctx_mgr_.reset();
    batch_rescan_ctl_ = NULL;
    p2p_dfo_map_.reuse();
    p2p_temp_table_info_.reset();
  }
  int init();
  bool enable_px_batch_rescan() { return get_rescan_param_count() > 0; }
  int64_t get_rescan_param_count()
  {
    return NULL == batch_rescan_ctl_ ? 0 : batch_rescan_ctl_->params_.get_count();
  }
  int64_t get_batch_id() const
  {
    return NULL == batch_rescan_ctl_ ? 0 : batch_rescan_ctl_->cur_idx_;
  }
  // if there is no physical op visits user table and at least one physical op visits virtual table, ignore error
  OB_INLINE bool should_ignore_vtable_error()
  {
    return TableAccessType::PURE_VIRTUAL_TABLE == table_access_type_;
  }
public:
  ObDfoMgr dfo_mgr_;
  ObPieceMsgCtxMgr piece_msg_ctx_mgr_;
  obrpc::ObPxRpcProxy rpc_proxy_;
  bool all_threads_finish_; // QC 已经明确知道所有 task 都已经执行完成并释放了资源
  int first_error_code_;
  dtl::ObDtlChannelLoop &msg_loop_;
  ObInterruptibleTaskID &interrupt_id_;
  ObPxCoordOp &coord_;
  ObBatchRescanCtl *batch_rescan_ctl_;
  const common::ObIArray<ObTableLocation> *pruning_table_location_;
  TableAccessType table_access_type_;
  ObDetectableId qc_detectable_id_;
  // key = p2p_dh_id value = dfo_id + target_addrs
  hash::ObHashMap<int64_t, ObP2PDfoMapNode, hash::NoPthreadDefendMode> p2p_dfo_map_;
  ObTempTableP2PInfo p2p_temp_table_info_;
  RuntimeFilterDependencyInfo rf_dpd_info_;
};

class ObDfoSchedulerBasic;

class ObPxTerminateMsgProc : public ObIPxCoordMsgProc
{
public:
  ObPxTerminateMsgProc(
    ObPxCoordInfo &coord_info,
    ObIPxCoordEventListener &listener)
  : coord_info_(coord_info), listener_(listener) {}
  // msg processor callback
  int on_sqc_init_msg(ObExecContext &ctx, const ObPxInitSqcResultMsg &pkt);
  int on_sqc_finish_msg(ObExecContext &ctx, const ObPxFinishSqcResultMsg &pkt);
  int on_eof_row(ObExecContext &ctx);
  int on_sqc_init_fail(ObDfo &dfo, ObPxSqcMeta &sqc);
  int on_interrupted(ObExecContext &ctx, const common::ObInterruptCode &pkt);
  int startup_msg_loop(ObExecContext &ctx);
  // Begin Datahub processing
  // Don't need to process datahub message in terminate message processor
  int on_piece_msg(ObExecContext &ctx, const ObBarrierPieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  int on_piece_msg(ObExecContext &ctx, const ObWinbufPieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  int on_piece_msg(ObExecContext &ctx, const ObDynamicSamplePieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  int on_piece_msg(ObExecContext &ctx, const ObRollupKeyPieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  int on_piece_msg(ObExecContext &ctx, const ObRDWFPieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  int on_piece_msg(ObExecContext &ctx, const ObInitChannelPieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  int on_piece_msg(ObExecContext &ctx, const ObReportingWFPieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  int on_piece_msg(ObExecContext &ctx, const ObOptStatsGatherPieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  int on_piece_msg(ObExecContext &ctx, const SPWinFuncPXPieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  int on_piece_msg(ObExecContext &ctx, const RDWinFuncPXPieceMsg &pkt) { UNUSED(ctx); UNUSED(pkt); return common::OB_NOT_SUPPORTED; }
  // End Datahub processing
  ObPxCoordInfo &coord_info_;
  ObIPxCoordEventListener &listener_;
};
class ObPxMsgProc : public ObIPxCoordMsgProc
{
public:
  ObPxMsgProc(
    ObPxCoordInfo &coord_info,
    ObIPxCoordEventListener &listener,
    ObPxRootDfoAction &root_dfo_action)
  : coord_info_(coord_info), listener_(listener),
    root_dfo_action_(root_dfo_action), scheduler_(NULL){}
  // msg processor callback
  int on_sqc_init_msg(ObExecContext &ctx, const ObPxInitSqcResultMsg &pkt);
  int on_sqc_finish_msg(ObExecContext &ctx, const ObPxFinishSqcResultMsg &pkt);
  int on_eof_row(ObExecContext &ctx);
  int on_sqc_init_fail(ObDfo &dfo, ObPxSqcMeta &sqc);
  int on_interrupted(ObExecContext &ctx, const common::ObInterruptCode &pkt);
  int startup_msg_loop(ObExecContext &ctx);
  int on_process_end(ObExecContext &ctx);

  void set_scheduler(ObDfoSchedulerBasic *scheduler) { scheduler_ = scheduler; }

  // root dfo 的调度特殊路径
  int on_dfo_pair_thread_inited(ObExecContext &ctx, ObDfo &child, ObDfo &parent);
  static int mark_rpc_filter(ObExecContext &ctx,
                             ObJoinFilterDataCtx &bf_ctx,
                             int64_t &each_group_size);
  // begin DATAHUB msg processing
  int on_piece_msg(ObExecContext &ctx, const ObBarrierPieceMsg &pkt);
  int on_piece_msg(ObExecContext &ctx, const ObWinbufPieceMsg &pkt);
  int on_piece_msg(ObExecContext &ctx, const ObDynamicSamplePieceMsg &pkt);
  int on_piece_msg(ObExecContext &ctx, const ObRollupKeyPieceMsg &pkt);
  int on_piece_msg(ObExecContext &ctx, const ObRDWFPieceMsg &pkt);
  int on_piece_msg(ObExecContext &ctx, const ObInitChannelPieceMsg &pkt);
  int on_piece_msg(ObExecContext &ctx, const ObReportingWFPieceMsg &pkt);
  int on_piece_msg(ObExecContext &ctx, const ObOptStatsGatherPieceMsg &pkt);
  int on_piece_msg(ObExecContext &ctx, const SPWinFuncPXPieceMsg &pkt);
  int on_piece_msg(ObExecContext &ctx, const RDWinFuncPXPieceMsg &pkt);
  void clean_dtl_interm_result(ObExecContext &ctx);
  // end DATAHUB msg processing
  void log_warn_sqc_fail(int ret, const ObPxFinishSqcResultMsg &pkt, ObPxSqcMeta *sqc);
private:
  int do_cleanup_dfo(ObDfo &dfo);
  int fast_dispatch_sqc(ObExecContext &exec_ctx,
                        ObDfo &dfo,
                        ObArray<ObPxSqcMeta *> &sqcs);
  int wait_for_dfo_finish(ObDfoMgr &dfo_mgr);

  int process_sqc_finish_msg_once(ObExecContext &ctx, const ObPxFinishSqcResultMsg &pkt,
                             ObPxSqcMeta *sqc, ObDfo *edge);

private:
  ObPxCoordInfo &coord_info_;
  ObIPxCoordEventListener &listener_;
  ObPxRootDfoAction &root_dfo_action_;
  ObDfoSchedulerBasic *scheduler_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_COORD_OP_H_
