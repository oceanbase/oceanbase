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

#ifndef _OB_SQL_PX_SUB_CORRD_H_
#define _OB_SQL_PX_SUB_CORRD_H_

#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_granule_pump.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/ob_sqc_ctx.h"
#include "sql/engine/px/ob_px_worker.h"
#include "sql/engine/px/ob_sub_trans_ctrl.h"
#include "sql/engine/ob_engine_op_traits.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_local_first_buffer_manager.h"
#include "sql/ob_sql_trans_control.h"
#include "lib/allocator/ob_safe_arena.h"

namespace oceanbase {
namespace observer {
class ObGlobalContext;
}

namespace sql {
class ObPxSQCHandler;

#define ENG_OP typename ObEngineOpTraits<NEW_ENG>
class ObPxSubCoord {
public:
  explicit ObPxSubCoord(const observer::ObGlobalContext& gctx, ObPxRpcInitSqcArgs& arg)
      : gctx_(gctx),
        sqc_arg_(arg),
        sqc_ctx_(arg),
        allocator_(common::ObModIds::OB_SQL_PX),
        local_worker_factory_(gctx, allocator_),
        thread_worker_factory_(gctx, allocator_),
        first_buffer_cache_(allocator_)
  {}
  virtual ~ObPxSubCoord() = default;
  int pre_process();
  int try_start_tasks(int64_t& dispatch_worker_count, bool is_fast_sqc = false);
  void notify_dispatched_task_exit(int64_t dispatched_work);
  int end_process();
  int init_exec_env(ObExecContext& exec_ctx);
  ObPxSQCProxy& get_sqc_proxy()
  {
    return sqc_ctx_.sqc_proxy_;
  }
  ObSqcCtx& get_sqc_ctx()
  {
    return sqc_ctx_;
  }
  int start_participants(ObPxRpcInitSqcArgs& args)
  {
    return trans_ctrl_.start_participants(*args.exec_ctx_, args.sqc_);
  };
  int end_participants(ObPxRpcInitSqcArgs& args, bool need_rollback)
  {
    int ret = OB_SUCCESS;
    if (NULL != args.exec_ctx_) {
      ret = trans_ctrl_.end_participants(*args.exec_ctx_, need_rollback);
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  int set_partitions_info(ObIArray<ObPxPartitionInfo>& partitions_info)
  {
    return sqc_ctx_.partitions_info_.assign(partitions_info);
  }
  int report_sqc_finish(int end_ret)
  {
    return sqc_ctx_.sqc_proxy_.report(end_ret);
  }
  int init_first_buffer_cache(bool is_rpc_worker, int64_t dop);
  void destroy_first_buffer_cache();

private:
  int setup_loop_proc(ObSqcCtx& sqc_ctx) const;
  int setup_op_input(ObExecContext& ctx, ObPhyOperator& root, ObSqcCtx& sqc_ctx,
      ObPartitionReplicaLocationIArray& tsc_locations, int64_t& tsc_locations_idx,
      ObIArray<const ObTableScan*>& all_scan_ops, int64_t& dml_op_count);
  int setup_op_input(ObExecContext& ctx, ObOpSpec& root, ObSqcCtx& sqc_ctx,
      ObPartitionReplicaLocationIArray& tsc_locations, int64_t& tsc_locations_idx,
      ObIArray<const ObTableScanSpec*>& all_scan_ops, int64_t& dml_op_count);
  template <bool NEW_ENG>
  int get_tsc_or_dml_op_partition_key(ENG_OP::Root& root, ObPartitionReplicaLocationIArray& tsc_locations,
      int64_t& tsc_locations_idx, common::ObIArray<const ENG_OP::TSC*>& scan_ops,
      common::ObIArray<common::ObPartitionArray>& partition_keys_array, int64_t& td_op_count, int64_t part_count);
  int link_sqc_qc_channel(ObPxRpcInitSqcArgs& sqc_arg);
  int dispatch_tasks(
      ObPxRpcInitSqcArgs& sqc_arg, ObSqcCtx& sqc_ctx, int64_t& dispatch_worker_count, bool is_fast_sqc = false);
  int link_sqc_task_channel(ObSqcCtx& sqc_ctx);
  int unlink_sqc_task_channel(ObSqcCtx& sqc_ctx);
  int unlink_sqc_qc_channel(ObPxRpcInitSqcArgs& sqc_arg);
  int create_tasks(ObPxRpcInitSqcArgs& sqc_arg, ObSqcCtx& sqc_ctx);
  int try_cleanup_tasks();

  int dispatch_task_to_thread_pool(ObPxRpcInitSqcArgs& sqc_arg, ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc, int64_t task_idx);
  int dispatch_task_to_local_thread(ObPxRpcInitSqcArgs& sqc_arg, ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc);

  int try_prealloc_data_channel(ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc);
  int try_prealloc_transmit_channel(ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc);
  int try_prealloc_receive_channel(ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc);

  dtl::ObDtlLocalFirstBufferCache* get_first_buffer_cache()
  {
    return &first_buffer_cache_;
  }

private:
  const observer::ObGlobalContext& gctx_;
  ObPxRpcInitSqcArgs& sqc_arg_;
  ObSqcCtx sqc_ctx_;
  ObSubTransCtrl trans_ctrl_;
  common::ObSafeArena allocator_;
  ObPxLocalWorkerFactory local_worker_factory_;
  ObPxThreadWorkerFactory thread_worker_factory_;
  int64_t reserved_thread_count_;
  dtl::ObDtlLocalFirstBufferCache first_buffer_cache_;
  DISALLOW_COPY_AND_ASSIGN(ObPxSubCoord);
};
}  // namespace sql
}  // namespace oceanbase
#undef ENG_OP
#endif /* _OB_SQL_PX_SUB_CORRD_H_ */
//// end of header file
