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
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/ob_sql_trans_control.h"
#include "lib/allocator/ob_safe_arena.h"


namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}

namespace sql
{
class ObPxSQCHandler;
class ObPxSubCoord
{
public:
  explicit ObPxSubCoord(const observer::ObGlobalContext &gctx,
                        ObPxRpcInitSqcArgs &arg)
      : gctx_(gctx),
        sqc_arg_(arg),
        sqc_ctx_(arg),
        allocator_(common::ObModIds::OB_SQL_PX),
        local_worker_factory_(gctx, allocator_),
        thread_worker_factory_(gctx, allocator_),
        is_single_tsc_leaf_dfo_(false),
        all_shared_rf_msgs_()
  {}
  virtual ~ObPxSubCoord() = default;
  int pre_process();
  int try_start_tasks(int64_t &dispatch_worker_count, bool is_fast_sqc = false);
  void notify_dispatched_task_exit(int64_t dispatched_work);
  int end_process();
  int init_exec_env(ObExecContext &exec_ctx);
  ObPxSQCProxy &get_sqc_proxy() { return sqc_ctx_.sqc_proxy_; }
  ObSqcCtx &get_sqc_ctx() { return sqc_ctx_; }
  int64_t get_ddl_context_id() const { return ddl_ctrl_.context_id_; }
  int set_partitions_info(ObIArray<ObPxTabletInfo> &partitions_info) {
    return sqc_ctx_.partitions_info_.assign(partitions_info);
  }
  int report_sqc_finish(int end_ret) {
    return sqc_ctx_.sqc_proxy_.report(end_ret);
  }

  // for ddl insert sstable
  // using start and end pair function to control the life cycle of ddl context
  int check_need_start_ddl(bool &need_start_ddl);
  int start_ddl();
  int end_ddl(const bool need_commit);

  int pre_setup_op_input(ObExecContext &ctx,
      ObOpSpec &root,
      ObSqcCtx &sqc_ctx,
      const DASTabletLocIArray &tsc_locations,
      const ObIArray<ObSqcTableLocationKey> &tsc_location_keys);
  int rebuild_sqc_access_table_locations();
  void set_is_single_tsc_leaf_dfo(bool flag) { is_single_tsc_leaf_dfo_ = flag; }
  int get_participants(ObPxSqcMeta &sqc,
                       const int64_t table_id,
                       ObIArray<std::pair<share::ObLSID, ObTabletID>> &ls_tablet_ids) const;
  void destroy_shared_rf_msgs();
private:
  int setup_loop_proc(ObSqcCtx &sqc_ctx) const;
  int setup_op_input(ObExecContext &ctx,
                     ObOpSpec &root,
                     ObSqcCtx &sqc_ctx,
                     const DASTabletLocIArray &tsc_locations,
                     const ObIArray<ObSqcTableLocationKey> &tsc_location_keys);
  int setup_gi_op_input(ObExecContext &ctx,
                        ObOpSpec &root,
                        ObSqcCtx &sqc_ctx,
                        const DASTabletLocIArray &tsc_locations,
                        const ObIArray<ObSqcTableLocationKey> &tsc_location_keys);
  int get_tsc_or_dml_op_tablets(ObOpSpec &root,
                                const DASTabletLocIArray &tsc_locations,
                                const common::ObIArray<ObSqcTableLocationKey> &tsc_location_keys,
                                common::ObIArray<const ObTableScanSpec*> &scan_ops,
                                common::ObIArray<DASTabletLocArray> &tablets_array);
  int link_sqc_qc_channel(ObPxRpcInitSqcArgs &sqc_arg);
  int dispatch_tasks(ObPxRpcInitSqcArgs &sqc_arg,
                     ObSqcCtx &sqc_ctx,
                     int64_t &dispatch_worker_count,
                     bool is_fast_sqc = false);
  int link_sqc_task_channel(ObSqcCtx &sqc_ctx);
  int unlink_sqc_task_channel(ObSqcCtx &sqc_ctx);
  int unlink_sqc_qc_channel(ObPxRpcInitSqcArgs &sqc_arg);
  int create_tasks(ObPxRpcInitSqcArgs &sqc_arg, ObSqcCtx &sqc_ctx, bool is_fast_sqc = false);
  int try_cleanup_tasks();

  int dispatch_task_to_thread_pool(ObPxRpcInitSqcArgs &sqc_arg,
                                   ObSqcCtx &sqc_ctx,
                                   ObPxSqcMeta &sqc,
                                   int64_t task_idx);
  int dispatch_task_to_local_thread(ObPxRpcInitSqcArgs &sqc_arg,
                                    ObSqcCtx &sqc_ctx,
                                    ObPxSqcMeta &sqc);

  int try_prealloc_data_channel(ObSqcCtx &sqc_ctx, ObPxSqcMeta &sqc);
  int try_prealloc_transmit_channel(ObSqcCtx &sqc_ctx, ObPxSqcMeta &sqc);
  int try_prealloc_receive_channel(ObSqcCtx &sqc_ctx, ObPxSqcMeta &sqc);
  void try_get_dml_op(ObOpSpec &root, ObTableModifySpec *&dml_op);
  int construct_p2p_dh_map() {
    return sqc_ctx_.sqc_proxy_.construct_p2p_dh_map(
           sqc_arg_.sqc_.get_p2p_dh_map_info());
  }
private:
  const observer::ObGlobalContext &gctx_;
  ObPxRpcInitSqcArgs &sqc_arg_;
  ObSqcCtx sqc_ctx_;
  ObSubTransCtrl trans_ctrl_;
  ObDDLCtrl ddl_ctrl_; // for ddl insert sstable
  common::ObSafeArena allocator_;
  ObPxLocalWorkerFactory local_worker_factory_; // 当仅有1个task时，使用 local 构造 worker
  ObPxThreadWorkerFactory thread_worker_factory_; // 超过1个task的部分，使用thread 构造 worker
  int64_t reserved_thread_count_;
  bool is_single_tsc_leaf_dfo_;
  ObArray<int64_t> all_shared_rf_msgs_; // for clear
  DISALLOW_COPY_AND_ASSIGN(ObPxSubCoord);
};
}
}
#endif /* _OB_SQL_PX_SUB_CORRD_H_ */
//// end of header file
