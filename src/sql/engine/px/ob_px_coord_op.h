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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_COORD_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_COORD_OP_H_

#include "sql/engine/px/exchange/ob_px_receive_op.h"
#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_local_first_buffer_manager.h"
#include "sql/dtl/ob_dtl_task.h"

namespace oceanbase {
namespace sql {

class ObPxCoordSpec : public ObPxReceiveSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxCoordSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObPxReceiveSpec(alloc, type), px_expected_worker_count_(0), qc_id_(common::OB_INVALID_ID)
  {}
  ~ObPxCoordSpec()
  {}

  inline void set_expected_worker_count(int64_t c)
  {
    px_expected_worker_count_ = c;
  }
  inline int64_t get_expected_worker_count() const
  {
    return px_expected_worker_count_;
  }
  int64_t px_expected_worker_count_;  // max thread count for current px.
  int64_t qc_id_;
};

class ObPxCoordOp : public ObPxReceiveOp, public ObPxRootDfoAction {
public:
  ObPxCoordOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObPxCoordOp()
  {}

public:
  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_close() override;

  virtual void destroy() override
  {
    // don't change the order
    // no need to reset rpc_proxy_
    // no need to reset root_receive_ch_provider_
    coord_info_.destroy();
    row_allocator_.reset();
    allocator_.reset();
    ObPxReceiveOp::destroy();
  }
  void reset_for_rescan()
  {
    coord_info_.reset_for_rescan();
    root_dfo_ = nullptr;
    root_receive_ch_provider_.reset();
    first_row_fetched_ = false;
    first_row_sent_ = false;
    all_rows_finish_ = false;
    // time_recorder_ = 0;
    // don't change the order
    row_allocator_.reset();
    allocator_.reset();
    ObPxReceiveOp::reset_for_rescan();
  }

  virtual int receive_channel_root_dfo(ObExecContext& ctx, ObDfo& parent, ObPxTaskChSets& parent_ch_sets) override;
  virtual int receive_channel_root_dfo(ObExecContext& ctx, ObDfo& parent, dtl::ObDtlChTotalInfo& ch_info) override;

protected:
  virtual int free_allocator()
  {
    return common::OB_SUCCESS;
  }
  /* destroy all channel */
  int destroy_all_channel();
  /* setup input for every op with in dfo */
  int setup_op_input(ObDfo& root);
  /* px functions */
  int init_dfo_mgr(const ObDfoInterruptIdGen& dfo_id_gen, ObDfoMgr& dfo_mgr);
  /* interrupt */
  int terminate_running_dfos(ObDfoMgr& dfo_mgr);
  int post_init_op_ctx();

  /* for debug only */
  void debug_print(ObDfo& root);
  void debug_print_dfo_tree(int level, ObDfo& dfo);
  int handle_monitor_info();
  int write_op_metric_info(ObPxTaskMonitorInfo& task_monitor_info, char* metric_info, int64_t len, int64_t& pos);

  int try_link_channel() override;

  virtual int wait_all_running_dfos_exit();

  virtual int setup_loop_proc();

  int register_first_buffer_cache(ObDfo* root_dfo);
  void unregister_first_buffer_cache();

  int check_all_sqc(common::ObIArray<ObDfo*>& active_dfos, bool& all_dfo_terminate);

  int calc_allocated_worker_count(
      int64_t px_expected, int64_t query_expected, int64_t query_allocated, int64_t& allocated_worker_count);

  int register_interrupt();
  void clear_interrupt();

  virtual int init_dfc(ObDfo& dfo, dtl::ObDtlChTotalInfo* ch_info);
  virtual ObIPxCoordEventListener& get_listenner() = 0;

  dtl::ObDtlLocalFirstBufferCache* get_first_buffer_cache()
  {
    return &first_buffer_cache_;
  }

protected:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator row_allocator_;
  ObPxCoordInfo coord_info_;
  ObDfo* root_dfo_;  // pointer to QC
  ObPxRootReceiveChProvider root_receive_ch_provider_;
  bool first_row_fetched_;
  bool first_row_sent_;
  bool all_rows_finish_;  // QC has return all rows from all child dfos to upper operator.
  uint64_t qc_id_;
  dtl::ObDtlLocalFirstBufferCache first_buffer_cache_;
  bool register_interrupted_;
  // px_sequence_id_ is unique in single server, so the value pair {server_id, px_sequence_id_}
  // is unique in cluster.
  uint64_t px_sequence_id_;
  ObInterruptibleTaskID interrupt_id_;
  int px_dop_;
  int64_t time_recorder_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_COORD_OP_H_
