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

namespace oceanbase
{
namespace sql
{

class ObPxCoordOp : public ObPxReceiveOp, public ObPxRootDfoAction
{
public:
  ObPxCoordOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObPxCoordOp() {}
public:
  struct ObPxBatchOpInfo
  {
    OB_UNIS_VERSION_V(1);
    public:
    ObPxBatchOpInfo() : op_type_(PHY_INVALID), op_id_(OB_INVALID_ID) {}
    bool is_inited() const { return PHY_INVALID != op_type_;  }
    ObPhyOperatorType op_type_;
    int64_t op_id_;
  };
public:
  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_rescan() override;
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
  virtual int inner_drain_exch() override;
  void reset_for_rescan()
  {
    coord_info_.reset_for_rescan();
    root_dfo_ = nullptr;
    root_receive_ch_provider_.reset();
    first_row_fetched_ = false;
    first_row_sent_ = false;
    // time_recorder_ = 0;
    // don't change the order
    row_allocator_.reset();
    allocator_.reset();
    ObPxReceiveOp::reset_for_rescan();
  }
  int64_t get_batch_id() { return coord_info_.get_batch_id(); }
  bool enable_px_batch_rescan() { return coord_info_.enable_px_batch_rescan(); }
  int64_t get_rescan_param_count()
  {  return coord_info_.get_rescan_param_count(); }
  const common::ObIArray<ObTableLocation> *get_pruning_table_locations()
  {  return coord_info_.pruning_table_location_; }
  void set_pruning_table_locations(const common::ObIArray<ObTableLocation> *pruning_table_locations)
  {
    coord_info_.pruning_table_location_ = pruning_table_locations;
  }
  virtual int receive_channel_root_dfo(ObExecContext &ctx, ObDfo &parent, ObPxTaskChSets &parent_ch_sets) override;
  virtual int receive_channel_root_dfo(
      ObExecContext &ctx, ObDfo &parent, dtl::ObDtlChTotalInfo &ch_info) override;
  virtual int notify_peers_mock_eof(
      ObDfo *dfo, int64_t timeout_ts, common::ObAddr addr) const;
protected:
  virtual int free_allocator() { return common::OB_SUCCESS; }
  /* destroy all channel */
  int destroy_all_channel();
  /* setup input for every op with in dfo */
  int setup_op_input(ObDfo &root);
  /* px functions */
  int init_dfo_mgr(const ObDfoInterruptIdGen &dfo_id_gen,
                   ObDfoMgr &dfo_mgr);
  /* interrupt */
  int terminate_running_dfos(ObDfoMgr &dfo_mgr);
  int post_init_op_ctx();

  /* for debug only */
  void debug_print(ObDfo &root);
  void debug_print_dfo_tree(int level, ObDfo &dfo);
  int try_link_channel() override;

  virtual int wait_all_running_dfos_exit();

  virtual int setup_loop_proc();

  int register_first_buffer_cache(ObDfo *root_dfo);
  void unregister_first_buffer_cache();

  int check_all_sqc(common::ObIArray<ObDfo *> &active_dfos,
      int64_t &time_offset,
      bool &all_dfo_terminate,
      int64_t &cur_timestamp);

  int register_interrupt();
  void clear_interrupt();

  virtual int init_dfc(ObDfo &dfo, dtl::ObDtlChTotalInfo *ch_info);
  virtual ObIPxCoordEventListener &get_listenner() = 0;
  dtl::ObDtlLocalFirstBufferCache *get_first_buffer_cache() { return &first_buffer_cache_; }

  int init_batch_info();
  int batch_rescan();
  int erase_dtl_interm_result();
  // send rpc to clean dtl interm result of not scheduled dfos.
  virtual void clean_dfos_dtl_interm_result() = 0;
  int try_clear_p2p_dh_info();
protected:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator row_allocator_;
  ObPxCoordInfo coord_info_;
  ObDfo *root_dfo_; // 指向 QC
  ObPxRootReceiveChProvider root_receive_ch_provider_;
  bool first_row_fetched_;
  bool first_row_sent_;
  uint64_t qc_id_;
  dtl::ObDtlLocalFirstBufferCache first_buffer_cache_;
  bool register_interrupted_;
  /*
    *   px_sequnce_id  explaination
    *   在中断功能和dtl buffer中均对key的唯一性有要求
    *   执行嵌套px的计划按照此前的设计将存在key的id不唯一的缺陷.
    *   引入px_sequence_id, 该id可以保证在该server下的每一个px都是唯一递增的.
    *   配合server_id使用即可全集群唯一.
    * */
  uint64_t px_sequence_id_;
  ObInterruptibleTaskID interrupt_id_;
  bool register_detectable_id_;
  ObDetectableId detectable_id_;
  int px_dop_;
  int64_t time_recorder_;
  int64_t batch_rescan_param_version_;
  ObExtraServerAliveCheck server_alive_checker_;
  int64_t last_px_batch_rescan_size_;
};

class ObPxCoordSpec : public ObPxReceiveSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxCoordSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObPxReceiveSpec(alloc, type),
    px_expected_worker_count_(0),
    qc_id_(common::OB_INVALID_ID),
    batch_op_info_(),
    table_locations_(alloc),
    sort_exprs_(alloc),
    sort_collations_(alloc),
    sort_cmp_funs_(alloc)
  {}
  ~ObPxCoordSpec() {}

  inline void set_expected_worker_count(int64_t c)
  {
    px_expected_worker_count_ = c;
  }
  inline int64_t get_expected_worker_count() const
  {
    return px_expected_worker_count_;
  }
  inline void set_px_batch_op_info(int64_t id, ObPhyOperatorType type)
  {
    batch_op_info_.op_id_ = id;
    batch_op_info_.op_type_ = type;
  }
  TableLocationFixedArray &get_table_locations()
  { return table_locations_; }
  int64_t px_expected_worker_count_; // 当前 px 可以分到的线程数上限，用于multi-px 限流场景
  int64_t qc_id_;
  // px在支持分布式batch rescan时需要感知做rescan的算子id以及算子类型
  // 是1对1的对应关系
  ObPxCoordOp::ObPxBatchOpInfo batch_op_info_;
  // 对于有条件下推的table_location, 需要序列化
  TableLocationFixedArray table_locations_;
  // dynamic sample related
  ExprFixedArray sort_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funs_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_COORD_OP_H_
