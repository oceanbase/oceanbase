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

#ifndef _OB_SQL_PX_COORD_H_
#define _OB_SQL_PX_COORD_H_

#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/executor/ob_receive.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_local_first_buffer_manager.h"
#include "sql/dtl/ob_dtl_task.h"

namespace oceanbase {
namespace sql {
class ObPxReceive;

class ObPxCoord;
class ObPxMergeSortCoord;

class ObPxCoord : public ObPxReceive {
  OB_UNIS_VERSION_V(1);

public:
  class ObPxCoordCtx : public ObPxReceiveCtx, public ObPxRootDfoAction {
    static const int64_t QC_INTP_CONTEXT_HASH_BUCKET = 32;

  public:
    explicit ObPxCoordCtx(ObExecContext& ctx);
    virtual ~ObPxCoordCtx();
    virtual void destroy()
    {
      // don't change the order
      // no need to reset rpc_proxy_
      // no need to reset root_receive_ch_provider_
      coord_info_.destroy();
      row_allocator_.reset();
      allocator_.reset();
      ObPxReceiveCtx::destroy();
    }
    void reset_for_rescan()
    {
      root_dfo_ = nullptr;
      root_receive_ch_provider_.reset();
      first_row_fetched_ = false;
      first_row_sent_ = false;
      all_rows_finish_ = false;
      time_recorder_ = 0;
      // don't change the order
      coord_info_.reset_for_rescan();
      row_allocator_.reset();
      allocator_.reset();
      ObPxReceiveCtx::reset_for_rescan();
    }
    virtual int init_dfc(ObExecContext& ctx, ObDfo& dfo);
    virtual ObIPxCoordEventListener& get_listenner() = 0;

    dtl::ObDtlLocalFirstBufferCache* get_first_buffer_cache()
    {
      return &first_buffer_cache_;
    }

    virtual int receive_channel_root_dfo(ObExecContext& ctx, ObDfo& parent, ObPxTaskChSets& parent_ch_sets);
    virtual int receive_channel_root_dfo(ObExecContext& ctx, ObDfo& parent, dtl::ObDtlChTotalInfo& ch_info);

  protected:
    common::ObArenaAllocator allocator_;
    common::ObArenaAllocator row_allocator_;
    ObPxCoordInfo coord_info_;
    ObDfo* root_dfo_;  // point to QC
    ObPxRootReceiveChProvider root_receive_ch_provider_;
    bool first_row_fetched_;
    bool first_row_sent_;
    // QC already has received all Child DFO's data and give it to it's upper operator
    bool all_rows_finish_;
    uint64_t qc_id_;
    dtl::ObDtlLocalFirstBufferCache first_buffer_cache_;
    int64_t time_recorder_;
    bool register_interrupted_;
    // unique id for cluster
    uint64_t px_sequence_id_;
    ObInterruptibleTaskID interrupt_id_;
    int px_dop_;
    friend class ObPxCoord;
  };

public:
  explicit ObPxCoord(common::ObIAllocator& alloc);
  virtual ~ObPxCoord();
  inline void set_expected_worker_count(int64_t c)
  {
    px_expected_worker_count_ = c;
  }
  inline int64_t get_expected_worker_count() const
  {
    return px_expected_worker_count_;
  }
  // int open();
  // for debug purpose, should remove later
  // inline void set_dfo_tree(ObDfo &root) { root_ = &root;}
protected:
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int rescan(ObExecContext& ctx) const;
  /* save some misc data for px  */
  virtual int create_operator_input(ObExecContext& ctx) const;
  virtual int free_allocator(ObExecContext& ctx) const
  {
    UNUSED(ctx);
    return common::OB_SUCCESS;
  }

protected:
  /* destroy all channel */
  int destroy_all_channel(ObPxCoordCtx& px_ctx) const;
  /* setup input for every op with in dfo */
  int setup_op_input(ObExecContext& ctx, ObDfo& root) const;
  /* px functions */
  int init_dfo_mgr(const ObDfoInterruptIdGen& dfo_id_gen, ObExecContext& exec_ctx, ObDfoMgr& dfo_mgr) const;
  /* interrupt */
  int terminate_running_dfos(ObExecContext& ctx, ObDfoMgr& dfo_mgr) const;
  int create_op_ctx(ObExecContext& ctx) const;
  int post_init_op_ctx(ObExecContext& ctx, ObPxCoordCtx& op_ctx) const;

  /* for debug only */
  void debug_print(ObDfo& root) const;
  void debug_print_dfo_tree(int level, ObDfo& dfo) const;
  int handle_monitor_info(ObPxCoordCtx& px_ctx, ObExecContext& ctx) const;
  int write_op_metric_info(ObPxTaskMonitorInfo& task_monitor_info, char* metric_info, int64_t len, int64_t& pos) const;

  int try_link_channel(ObExecContext& ctx) const override;

  virtual int wait_all_running_dfos_exit(ObExecContext& ctx) const;

  virtual int setup_loop_proc(ObExecContext& ctx, ObPxCoordCtx& px_ctx) const;

  int register_first_buffer_cache(ObExecContext& ctx, ObPxCoordCtx& px_ctx, ObDfo* root_dfo) const;
  void unregister_first_buffer_cache(ObExecContext& ctx, ObPxCoordCtx& px_ctx) const;

  int check_all_sqc(common::ObIArray<ObDfo*>& active_dfos, bool& all_dfo_terminate) const;

  int calc_allocated_worker_count(
      int64_t px_expected, int64_t query_expected, int64_t query_allocated, int64_t& allocated_worker_count) const;

  int register_interrupt(ObPxCoordCtx* px_ctx) const;
  void clear_interrupt(ObPxCoordCtx* px_ctx) const;

private:
  // for multi-px concurrent limiting
  int64_t px_expected_worker_count_;
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPxCoord);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_PX_COORD_H_ */
//// end of header file
