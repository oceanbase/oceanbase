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

#ifndef __OCEANBASE_SQL_ENGINE_PX_DFO_H__
#define __OCEANBASE_SQL_ENGINE_PX_DFO_H__

#include "share/interrupt/ob_global_interrupt_call.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/px/ob_px_interruption.h"
#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase {
namespace sql {

enum class ObDfoState { WAIT, BLOCK, RUNNING, FINISH, FAIL };

#define SQC_TASK_START (1 << 0)
#define SQC_TASK_EXIT (1 << 1)

class ObPhyOperator;
class ObPhysicalPlan;
class ObSqcTaskMgr;
class ObPxSqcHandler;

class ObPxTaskMeta {
public:
  ObPxTaskMeta()
      : exec_addr_(),
        sqc_id_(common::OB_INVALID_INDEX),
        task_id_(common::OB_INVALID_INDEX),
        sm_group_id_(common::OB_INVALID_INDEX)
  {}
  ~ObPxTaskMeta() = default;
  void set_exec_addr(const common::ObAddr& addr)
  {
    exec_addr_ = addr;
  }
  const common::ObAddr& get_exec_addr() const
  {
    return exec_addr_;
  }
  void set_task_id(int64_t task_id)
  {
    task_id_ = task_id;
  }
  int64_t get_task_id() const
  {
    return task_id_;
  }
  void set_sqc_id(int64_t sqc_id)
  {
    sqc_id_ = sqc_id;
  }
  int64_t get_sqc_id() const
  {
    return sqc_id_;
  }
  void set_sm_group_id(int64_t sm_group_id)
  {
    sm_group_id_ = sm_group_id;
  }
  int64_t get_sm_group_id() const
  {
    return sm_group_id_;
  }
  TO_STRING_KV(K_(exec_addr), K_(sqc_id), K_(task_id));

private:
  common::ObAddr exec_addr_;
  int64_t sqc_id_;
  int64_t task_id_;
  // slave map group id
  int64_t sm_group_id_;
};

class ObPxSqcMeta {
  OB_UNIS_VERSION(1);

public:
  struct PartitionIdValue {
    OB_UNIS_VERSION(1);

  public:
    PartitionIdValue() : partition_id_(0), location_idx_(0), value_begin_idx_(0), value_count_(0)
    {}
    int64_t partition_id_;
    int64_t location_idx_;
    int64_t value_begin_idx_;
    int64_t value_count_;
    TO_STRING_KV(K_(partition_id), K_(location_idx), K_(value_begin_idx), K_(value_count));
  };

public:
  ObPxSqcMeta()
      : execution_id_(common::OB_INVALID_ID),
        qc_id_(common::OB_INVALID_ID),
        sqc_id_(common::OB_INVALID_ID),
        dfo_id_(common::OB_INVALID_ID),
        locations_(),
        access_table_locations_(),
        qc_ch_info_(),
        sqc_ch_info_(),
        qc_channel_(NULL),
        sqc_channel_(NULL),
        exec_addr_(),
        qc_addr_(),
        task_count_(0),
        max_task_count_(3),
        min_task_count_(1),
        total_task_count_(1),
        total_part_count_(1),
        thread_inited_(false),
        thread_finish_(false),
        px_int_id_(),
        task_monitor_info_array_(),
        is_fulltree_(false),
        is_rpc_worker_(false),
        need_report_(false),
        qc_server_id_(common::OB_INVALID_ID),
        parent_dfo_id_(common::OB_INVALID_ID),
        px_sequence_id_(common::OB_INVALID_ID),
        transmit_use_interm_result_(false),
        recieve_use_interm_result_(false),
        serial_receive_channels_(),
        interm_result_ids_(),
        partition_id_values_()
  {}
  ~ObPxSqcMeta() = default;
  int assign(const ObPxSqcMeta& other);

  dtl::ObDtlChannelInfo& get_qc_channel_info()
  {
    return qc_ch_info_;
  }
  const dtl::ObDtlChannelInfo& get_qc_channel_info_const() const
  {
    return qc_ch_info_;
  }
  dtl::ObDtlChannelInfo& get_sqc_channel_info()
  {
    return sqc_ch_info_;
  }
  const dtl::ObDtlChannelInfo& get_sqc_channel_info_const() const
  {
    return sqc_ch_info_;
  }
  ObPartitionReplicaLocationIArray& get_locations()
  {
    return locations_;
  }
  ObPartitionReplicaLocationIArray& get_access_table_locations()
  {
    return access_table_locations_;
  }
  const ObPartitionReplicaLocationIArray& get_locations() const
  {
    return locations_;
  }
  void set_execution_id(uint64_t execution_id)
  {
    execution_id_ = execution_id;
  }
  void set_qc_id(uint64_t qc_id)
  {
    qc_id_ = qc_id;
  }
  void set_sqc_id(int64_t sqc_id)
  {
    sqc_id_ = sqc_id;
  }
  void set_dfo_id(int64_t dfo_id)
  {
    dfo_id_ = dfo_id;
  }
  inline ObPxInterruptID get_interrupt_id()
  {
    return px_int_id_;
  }
  inline void set_interrupt_id(const ObPxInterruptID& px_int_id)
  {
    px_int_id_ = px_int_id;
  }
  void set_exec_addr(const common::ObAddr& addr)
  {
    exec_addr_ = addr;
  }
  void set_qc_addr(const common::ObAddr& addr)
  {
    qc_addr_ = addr;
  }
  void set_qc_channel(dtl::ObDtlChannel* ch)
  {
    qc_channel_ = ch;
  }
  void set_sqc_channel(dtl::ObDtlChannel* ch)
  {
    sqc_channel_ = ch;
  }
  void set_task_count(int64_t task_count)
  {
    task_count_ = task_count;
  }
  void set_max_task_count(int64_t max_task_count)
  {
    max_task_count_ = max_task_count;
  }
  void set_min_task_count(int64_t min_task_count)
  {
    min_task_count_ = min_task_count;
  }
  void set_total_task_count(int64_t total_task_count)
  {
    total_task_count_ = total_task_count;
  }
  void set_total_part_count(int64_t total_part_count)
  {
    total_part_count_ = total_part_count;
  }

  uint64_t get_execution_id() const
  {
    return execution_id_;
  }
  uint64_t get_qc_id() const
  {
    return qc_id_;
  }
  int64_t get_sqc_id() const
  {
    return sqc_id_;
  }
  int64_t get_dfo_id() const
  {
    return dfo_id_;
  }
  common::ObIArray<ObPxPartitionInfo>& get_partitions_info()
  {
    return partitions_info_;
  }
  common::ObIArray<uint64_t>& get_interm_results()
  {
    return interm_result_ids_;
  }

  const common::ObAddr& get_exec_addr() const
  {
    return exec_addr_;
  }
  const common::ObAddr& get_sqc_addr() const
  {
    return exec_addr_;
  }  // get_exec_addr alias
  const common::ObAddr& get_qc_addr() const
  {
    return qc_addr_;
  }
  dtl::ObDtlChannel* get_qc_channel()
  {
    return qc_channel_;
  }
  dtl::ObDtlChannel* get_sqc_channel()
  {
    return sqc_channel_;
  }
  dtl::ObDtlChannel* get_sqc_channel() const
  {
    return sqc_channel_;
  }
  int64_t get_task_count() const
  {
    return task_count_;
  }
  int64_t get_max_task_count() const
  {
    return max_task_count_;
  }
  int64_t get_min_task_count() const
  {
    return min_task_count_;
  }
  int64_t get_total_task_count() const
  {
    return total_task_count_;
  }
  int64_t get_total_part_count() const
  {
    return total_part_count_;
  }
  void set_fulltree(bool v)
  {
    is_fulltree_ = v;
  }
  bool is_fulltree() const
  {
    return is_fulltree_;
  }
  void set_rpc_worker(bool v)
  {
    is_rpc_worker_ = v;
  }
  bool is_rpc_worker() const
  {
    return is_rpc_worker_;
  }
  void set_thread_inited(bool v)
  {
    thread_inited_ = v;
  }
  bool is_thread_inited() const
  {
    return thread_inited_;
  }
  void set_thread_finish(bool v)
  {
    thread_finish_ = v;
  }
  bool is_thread_finish() const
  {
    return thread_finish_;
  }
  void set_need_report(bool v)
  {
    need_report_ = v;
  }
  bool need_report()
  {
    return need_report_;
  }
  void set_qc_server_id(int64_t qc_server_id)
  {
    qc_server_id_ = qc_server_id;
  }
  int64_t get_qc_server_id()
  {
    return qc_server_id_;
  }
  void set_parent_dfo_id(int64_t parent_dfo_id)
  {
    parent_dfo_id_ = parent_dfo_id;
  }
  int64_t get_parent_dfo_id()
  {
    return parent_dfo_id_;
  }
  void set_px_sequence_id(uint64_t px_sequence_id)
  {
    px_sequence_id_ = px_sequence_id;
  }
  int64_t get_px_sequence_id()
  {
    return px_sequence_id_;
  }
  ObPxTransmitDataChannelMsg& get_transmit_channel_msg()
  {
    return transmit_channel_;
  }
  ObPxReceiveDataChannelMsg& get_receive_channel_msg()
  {
    return receive_channel_;
  }
  common::ObIArray<ObPxReceiveDataChannelMsg>& get_serial_receive_channels()
  {
    return serial_receive_channels_;
  }
  void reset()
  {
    locations_.reset();
    access_table_locations_.reset();
    transmit_channel_.reset();
    receive_channel_.reset();
    serial_receive_channels_.reset();
  }
  bool is_prealloc_transmit_channel() const
  {
    return transmit_channel_.has_filled_channel();
  }
  bool is_prealloc_receive_channel() const
  {
    return receive_channel_.has_filled_channel();
  }
  ObPxTaskMonitorInfoArray& get_task_monitor_info_array()
  {
    return task_monitor_info_array_;
  };
  int set_task_monitor_info_array(const ObPxTaskMonitorInfoArray& other)
  {
    return task_monitor_info_array_.assign(other);
  }
  void set_transmit_use_interm_result(bool flag)
  {
    transmit_use_interm_result_ = flag;
  }
  void set_recieve_use_interm_result(bool flag)
  {
    recieve_use_interm_result_ = flag;
  }
  bool transmit_use_interm_result()
  {
    return transmit_use_interm_result_;
  }
  bool recieve_use_interm_result()
  {
    return recieve_use_interm_result_;
  }
  int add_serial_recieve_channel(const ObPxReceiveDataChannelMsg& channel);
  int add_partition_id_values(int64_t partition_id, int64_t value_begin_idx, int64_t location_idx, int64_t value_count);
  common::ObIArray<PartitionIdValue>& get_partition_id_values()
  {
    return partition_id_values_;
  }
  int split_values(ObExecContext& ctx);
  TO_STRING_KV(K_(execution_id), K_(qc_id), K_(sqc_id), K_(dfo_id), K_(exec_addr), K_(qc_addr), K_(qc_ch_info),
      K_(sqc_ch_info), K_(task_count), K_(max_task_count), K_(min_task_count), K_(thread_inited), K_(thread_finish),
      K_(px_int_id), K_(is_fulltree), K_(is_rpc_worker), K_(transmit_use_interm_result), K_(recieve_use_interm_result),
      K(interm_result_ids_));

private:
  uint64_t execution_id_;
  uint64_t qc_id_;
  int64_t sqc_id_;
  int64_t dfo_id_;
  ObPartitionReplicaLocationSEArray locations_;
  ObPartitionReplicaLocationSEArray access_table_locations_;
  ObPxTransmitDataChannelMsg transmit_channel_;
  ObPxReceiveDataChannelMsg receive_channel_;
  dtl::ObDtlChannelInfo qc_ch_info_;
  dtl::ObDtlChannelInfo sqc_ch_info_;
  dtl::ObDtlChannel* qc_channel_;
  dtl::ObDtlChannel* sqc_channel_;
  common::ObAddr exec_addr_;
  common::ObAddr qc_addr_;
  int64_t task_count_;
  int64_t max_task_count_;
  int64_t min_task_count_;
  int64_t total_task_count_;
  int64_t total_part_count_;
  bool thread_inited_;
  bool thread_finish_;
  ObPxInterruptID px_int_id_;
  ObPxTaskMonitorInfoArray task_monitor_info_array_;
  bool is_fulltree_;
  bool is_rpc_worker_;
  // No need to serialize
  ObSEArray<ObPxPartitionInfo, 8> partitions_info_;
  bool need_report_;
  uint64_t qc_server_id_;
  int64_t parent_dfo_id_;
  uint64_t px_sequence_id_;
  bool transmit_use_interm_result_;
  bool recieve_use_interm_result_;
  common::ObSEArray<ObPxReceiveDataChannelMsg, 1> serial_receive_channels_;
  ObSEArray<uint64_t, 8> interm_result_ids_;
  ObSArray<PartitionIdValue> partition_id_values_;
};

class ObDfo {
  friend class ObDfoTreeNormalizer;
  friend class ObDfoMgr;
  using TaskFilterFunc = std::function<bool(const ObPxTaskChSet&)>;

public:
  static const int64_t MAX_DFO_ID = INT32_MAX;

public:
  ObDfo(common::ObIAllocator& allocator)
      : allocator_(allocator),
        execution_id_(common::OB_INVALID_ID),
        qc_id_(common::OB_INVALID_ID),
        dfo_id_(common::OB_INVALID_ID),
        px_int_id_(),
        dop_(0),
        assigned_worker_cnt_(0),
        used_worker_cnt_(0),
        is_single_(false),
        is_root_dfo_(false),
        prealloced_receive_channel_(false),
        prealloced_transmit_channel_(false),
        phy_plan_(NULL),
        root_op_(NULL),
        root_op_spec_(nullptr),
        child_dfos_(),
        has_scan_(false),
        has_dml_op_(false),
        has_temp_insert_(false),
        has_temp_scan_(false),
        is_active_(false),
        is_scheduled_(false),
        thread_inited_(false),
        thread_finish_(false),
        depend_sibling_(NULL),
        parent_(NULL),
        receive_ch_sets_map_(),
        dfo_ch_infos_(),
        is_fulltree_(false),
        is_rpc_worker_(false),
        qc_server_id_(common::OB_INVALID_ID),
        parent_dfo_id_(common::OB_INVALID_ID),
        px_sequence_id_(common::OB_INVALID_ID),
        temp_table_id_(0),
        slave_mapping_type_(SlaveMappingType::SM_NONE),
        part_ch_map_(),
        total_task_cnt_(0),
        has_expr_values_(false)
  {}

  virtual ~ObDfo() = default;
  inline void set_has_expr_values(bool flag)
  {
    has_expr_values_ = flag;
  }
  inline bool has_expr_values()
  {
    return has_expr_values_;
  }
  inline void set_execution_id(uint64_t execution_id)
  {
    execution_id_ = execution_id;
  }
  inline uint64_t get_execution_id()
  {
    return execution_id_;
  }
  inline void set_qc_id(uint64_t qc_id)
  {
    qc_id_ = qc_id;
  }
  inline uint64_t get_qc_id()
  {
    return qc_id_;
  }
  inline ObPxInterruptID& get_interrupt_id()
  {
    return px_int_id_;
  }
  inline void set_interrupt_id(const ObPxInterruptID& px_int_id)
  {
    px_int_id_ = px_int_id;
  }
  inline void set_dop(const int64_t dop)
  {
    dop_ = dop;
  }
  inline int64_t get_dop() const
  {
    return dop_;
  }
  void set_assigned_worker_count(int64_t cnt)
  {
    assigned_worker_cnt_ = cnt;
  }
  int64_t get_assigned_worker_count() const
  {
    return assigned_worker_cnt_;
  }
  void set_used_worker_count(int64_t cnt)
  {
    used_worker_cnt_ = cnt;
  }
  int64_t get_used_worker_count() const
  {
    return used_worker_cnt_;
  }
  inline void set_single(const bool single)
  {
    is_single_ = single;
  }
  inline bool is_single() const
  {
    return is_single_;
  }
  inline void set_root_dfo(bool is_root)
  {
    is_root_dfo_ = is_root;
  }
  inline bool is_root_dfo() const
  {
    return is_root_dfo_;
  }
  inline void set_prealloc_receive_channel(const bool v)
  {
    prealloced_receive_channel_ = v;
  }
  inline bool is_prealloc_receive_channel() const
  {
    return prealloced_receive_channel_;
  }
  inline void set_prealloc_transmit_channel(const bool v)
  {
    prealloced_transmit_channel_ = v;
  }
  inline bool is_prealloc_transmit_channel() const
  {
    return prealloced_transmit_channel_;
  }
  inline void set_phy_plan(const ObPhysicalPlan* phy_plan)
  {
    phy_plan_ = phy_plan;
  }
  inline const ObPhysicalPlan* get_phy_plan() const
  {
    return phy_plan_;
  }
  inline void set_root_op(const ObPhyOperator* op)
  {
    root_op_ = op;
  }
  inline const ObPhyOperator* get_root_op()
  {
    return root_op_;
  }
  inline void set_root_op_spec(const ObOpSpec* op_spec)
  {
    root_op_spec_ = op_spec;
  }
  inline const ObOpSpec* get_root_op_spec()
  {
    return root_op_spec_;
  }
  inline void get_root(const ObPhyOperator*& root) const
  {
    root = root_op_;
  }
  inline void get_root(const ObOpSpec*& root) const
  {
    root = root_op_spec_;
  }
  inline void set_scan(bool has_scan)
  {
    has_scan_ = has_scan;
  }
  inline bool has_scan_op() const
  {
    return has_scan_;
  }
  inline void set_dml_op(bool has_dml_op)
  {
    has_dml_op_ = has_dml_op;
  }
  inline bool has_dml_op()
  {
    return has_dml_op_;
  }
  inline void set_temp_table_insert(bool has_insert)
  {
    has_temp_insert_ = has_insert;
  }
  inline bool has_temp_table_insert() const
  {
    return has_temp_insert_;
  }
  inline void set_temp_table_scan(bool has_scan)
  {
    has_temp_scan_ = has_scan;
  }
  inline bool has_temp_table_scan() const
  {
    return has_temp_scan_;
  }
  inline void set_rpc_worker(bool v)
  {
    is_rpc_worker_ = v;
  }
  inline bool is_rpc_worker() const
  {
    return is_rpc_worker_;
  }
  inline bool is_fast_dfo() const
  {
    return is_prealloc_receive_channel() || is_prealloc_transmit_channel();
  }
  inline void set_slave_mapping_type(SlaveMappingType v)
  {
    slave_mapping_type_ = v;
  }
  inline SlaveMappingType get_slave_mapping_type()
  {
    return slave_mapping_type_;
  }
  inline bool is_slave_mapping()
  {
    return SlaveMappingType::SM_NONE != slave_mapping_type_;
  }

  ObPxPartChMapArray& get_part_ch_map()
  {
    return part_ch_map_;
  }

  int add_sqc(const ObPxSqcMeta& sqc);
  int get_addrs(common::ObIArray<common::ObAddr>& addrs) const;
  int get_sqcs(common::ObIArray<ObPxSqcMeta*>& sqcs);
  int get_sqcs(common::ObIArray<const ObPxSqcMeta*>& sqcs) const;
  int get_sqc(int64_t idx, ObPxSqcMeta*& sqc);
  common::ObIArray<ObPxSqcMeta>& get_sqcs()
  {
    return sqcs_;
  }
  int64_t get_sqcs_count()
  {
    return sqcs_.count();
  }
  int build_tasks();
  int alloc_data_xchg_ch();
  int get_qc_channels(common::ObIArray<dtl::ObDtlChannel*>& sqc_chs);

  inline int append_child_dfo(ObDfo* dfo)
  {
    return child_dfos_.push_back(dfo);
  }
  inline int get_child_dfo(int64_t idx, ObDfo*& dfo) const
  {
    return child_dfos_.at(idx, dfo);
  }
  inline int64_t get_child_count() const
  {
    return child_dfos_.count();
  }
  ObIArray<ObDfo*>& get_child_dfos()
  {
    return child_dfos_;
  }
  inline bool has_child_dfo() const
  {
    return get_child_count() > 0;
  }
  void set_depend_sibling(ObDfo* sibling)
  {
    depend_sibling_ = sibling;
  }
  bool has_depend_sibling() const
  {
    return NULL != depend_sibling_;
  }
  ObDfo* depend_sibling() const
  {
    return depend_sibling_;
  }
  void set_parent(ObDfo* parent)
  {
    parent_ = parent;
  }
  bool has_parent() const
  {
    return NULL != parent_;
  }
  ObDfo* parent() const
  {
    return parent_;
  }

  void set_dfo_id(int64_t dfo_id)
  {
    dfo_id_ = dfo_id;
  }
  int64_t get_dfo_id() const
  {
    return dfo_id_;
  }

  void set_qc_server_id(int64_t qc_server_id)
  {
    qc_server_id_ = qc_server_id;
  }
  int64_t get_qc_server_id() const
  {
    return qc_server_id_;
  }
  void set_parent_dfo_id(int64_t parent_dfo_id)
  {
    parent_dfo_id_ = parent_dfo_id;
  }
  int64_t get_parent_dfo_id() const
  {
    return parent_dfo_id_;
  }

  void set_px_sequence_id(uint64_t px_sequence_id)
  {
    px_sequence_id_ = px_sequence_id;
  }
  int64_t get_px_sequence_id() const
  {
    return px_sequence_id_;
  }

  void set_temp_table_id(int64_t temp_table_id)
  {
    temp_table_id_ = temp_table_id;
  }
  int64_t get_temp_table_id() const
  {
    return temp_table_id_;
  }

  void set_active()
  {
    is_active_ = true;
  }
  bool is_active() const
  {
    return is_active_;
  }
  void set_scheduled()
  {
    is_scheduled_ = true;
  }
  bool is_scheduled() const
  {
    return is_scheduled_;
  }
  void set_thread_inited(bool v)
  {
    thread_inited_ = v;
  }
  bool is_thread_inited() const
  {
    return thread_inited_;
  }
  void set_thread_finish(bool v)
  {
    thread_finish_ = v;
  }
  bool is_thread_finish() const
  {
    return thread_finish_;
  }
  const common::ObArray<ObPxTaskMeta>& get_tasks() const
  {
    return tasks_;
  }
  int get_task_receive_chs_for_update(int64_t child_dfo_id, common::ObIArray<ObPxTaskChSet*>& ch_sets);
  int get_task_transmit_chs_for_update(common::ObIArray<ObPxTaskChSet*>& ch_sets);
  int get_task_receive_chs(int64_t child_dfo_id, ObPxTaskChSets& ch_sets) const;
  int get_task_receive_chs(int64_t child_dfo_id, ObPxTaskChSets& ch_sets, TaskFilterFunc filter) const;
  int get_task_transmit_chs(ObPxTaskChSets& ch_sets, TaskFilterFunc filter) const;

  static void reset_resource(ObDfo* dfo);
  void set_fulltree(bool v)
  {
    is_fulltree_ = v;
  }
  bool is_fulltree() const
  {
    return is_fulltree_;
  }
  bool check_root_valid();
  const ObPhysicalPlan* get_plan_by_root();

  void set_dist_method(ObPQDistributeMethod::Type dist_method)
  {
    dist_method_ = dist_method;
  }
  ObPQDistributeMethod::Type get_dist_method()
  {
    return dist_method_;
  }

  ObPxChTotalInfos& get_dfo_ch_total_infos()
  {
    return dfo_ch_infos_;
  }
  int64_t get_total_task_count()
  {
    return total_task_cnt_;
  }
  int get_dfo_ch_info(int64_t sqc_idx, dtl::ObDtlChTotalInfo*& ch_info);
  int prepare_channel_info();
  static int check_dfo_pair(ObDfo& parent, ObDfo& child, int64_t& child_dfo_idx);
  static int fill_channel_info_by_sqc(dtl::ObDtlExecServer& ch_servers, common::ObIArray<ObPxSqcMeta>& sqcs);
  static int fill_channel_info_by_sqc(dtl::ObDtlExecServer& ch_servers, ObPxSqcMeta& sqc);
  static bool is_valid_dfo_id(int64_t dfo_id)
  {
    return (dfo_id >= 0 && dfo_id <= MAX_DFO_ID) || (dfo_id == MAX_DFO_ID);
  }
  TO_STRING_KV(K_(execution_id), K_(dfo_id), K_(is_active), K_(is_scheduled), K_(thread_inited), K_(thread_finish),
      K_(dop), K_(assigned_worker_cnt), K_(used_worker_cnt), K_(is_single), K_(is_root_dfo), K_(is_fulltree),
      K_(has_scan), K_(sqcs), KP_(depend_sibling), KP_(parent), "child", get_child_count(), K_(slave_mapping_type),
      K_(dist_method));

private:
  DISALLOW_COPY_AND_ASSIGN(ObDfo);

private:
  int calc_total_task_count();

private:
  common::ObIAllocator& allocator_;
  uint64_t execution_id_;
  uint64_t qc_id_;
  int64_t dfo_id_;
  ObPxInterruptID px_int_id_;
  int64_t dop_;
  int64_t assigned_worker_cnt_;
  int64_t used_worker_cnt_;
  bool is_single_;
  bool is_root_dfo_;
  bool prealloced_receive_channel_;
  bool prealloced_transmit_channel_;
  const ObPhysicalPlan* phy_plan_;
  const ObPhyOperator* root_op_;
  const ObOpSpec* root_op_spec_;
  common::ObSEArray<ObDfo*, 4> child_dfos_;
  bool has_scan_;
  bool has_dml_op_;
  bool has_temp_insert_;
  bool has_temp_scan_;
  bool is_active_;
  bool is_scheduled_;
  bool thread_inited_;
  bool thread_finish_;
  ObDfo* depend_sibling_;
  ObDfo* parent_;
  ObPxTaskChSets transmit_ch_sets_;
  common::ObArray<ObPxTaskChSets*> receive_ch_sets_map_;
  ObPxChTotalInfos dfo_ch_infos_;
  common::ObArray<ObPxSqcMeta> sqcs_;
  common::ObArray<ObPxTaskMeta> tasks_;
  bool is_fulltree_;
  bool is_rpc_worker_;
  uint64_t qc_server_id_;
  int64_t parent_dfo_id_;
  uint64_t px_sequence_id_;
  uint64_t temp_table_id_;
  SlaveMappingType slave_mapping_type_;
  ObPxPartChMapArray part_ch_map_;
  ObPQDistributeMethod::Type dist_method_;
  int64_t total_task_cnt_;  // the task total count of dfo start worker
  bool has_expr_values_;
};

struct ObPxRpcInitSqcArgs {
  OB_UNIS_VERSION(1);

public:
  ObPxRpcInitSqcArgs()
      : sqc_(),
        exec_ctx_(NULL),
        ser_phy_plan_(NULL),
        des_phy_plan_(NULL),
        op_root_(NULL),
        op_spec_root_(nullptr),
        static_engine_root_(nullptr),
        des_allocator_(NULL),
        sqc_handler_(NULL),
        scan_ops_(),
        scan_spec_ops_()
  {}
  ~ObPxRpcInitSqcArgs() = default;

  void set_serialize_param(ObExecContext& exec_ctx, ObPhyOperator& op_root, const ObPhysicalPlan& ser_phy_plan);
  void set_serialize_param(ObExecContext& exec_ctx, ObOpSpec& op_spec_root, const ObPhysicalPlan& ser_phy_plan);
  void set_deserialize_param(ObExecContext& exec_ctx, ObPhysicalPlan& des_phy_plan, ObIAllocator* des_allocator);
  int assign(ObPxRpcInitSqcArgs& other);
  int do_deserialize(int64_t& pos, const char* buf, int64_t data_len);

  void set_static_engine_root(ObOperator& root)
  {
    static_engine_root_ = &root;
  }
  TO_STRING_KV(K_(sqc));

public:
  ObPxSqcMeta sqc_;
  ObExecContext* exec_ctx_;
  const ObPhysicalPlan* ser_phy_plan_;
  ObPhysicalPlan* des_phy_plan_;
  ObPhyOperator* op_root_;
  ObOpSpec* op_spec_root_;
  ObOperator* static_engine_root_;
  ObIAllocator* des_allocator_;
  // no need to serialize
  ObPxSqcHandler* sqc_handler_;
  ObSEArray<const ObTableScan*, 8> scan_ops_;
  ObSEArray<const ObTableScanSpec*, 8> scan_spec_ops_;
};

class ObPxTask {
  OB_UNIS_VERSION(1);

public:
  ObPxTask()
      : qc_id_(common::OB_INVALID_ID),
        dfo_id_(0),
        sqc_id_(0),
        task_id_(-1),
        execution_id_(0),
        task_channel_(NULL),
        sqc_channel_(NULL),
        rc_(TASK_DEFAULT_RET_VALUE),  // rc is set if less than 0
        state_(0),
        task_co_id_(0),
        px_int_id_(),
        task_monitor_info_(),
        is_fulltree_(false),
        affected_rows_(0),
        dml_row_info_()
  {}
  ~ObPxTask() = default;
  ObPxTask& operator=(const ObPxTask& other)
  {
    qc_id_ = other.qc_id_;
    dfo_id_ = other.dfo_id_;
    sqc_id_ = other.sqc_id_;
    task_id_ = other.task_id_;
    execution_id_ = other.execution_id_;
    sqc_ch_info_ = other.sqc_ch_info_;
    task_ch_info_ = other.task_ch_info_;
    sqc_addr_ = other.sqc_addr_;
    exec_addr_ = other.exec_addr_;
    qc_addr_ = other.qc_addr_;
    rc_ = other.rc_;
    state_ = other.state_;
    task_co_id_ = other.task_co_id_;
    px_int_id_ = other.px_int_id_;
    task_monitor_info_ = other.task_monitor_info_;
    is_fulltree_ = other.is_fulltree_;
    affected_rows_ = other.affected_rows_;
    dml_row_info_ = other.dml_row_info_;
    return *this;
  }

public:
  TO_STRING_KV(K_(qc_id), K_(dfo_id), K_(sqc_id), K_(task_id), K_(execution_id), K_(sqc_ch_info), K_(task_ch_info),
      K_(sqc_addr), K_(exec_addr), K_(qc_addr), K_(rc), K_(task_co_id), K_(px_int_id), K_(is_fulltree),
      K_(affected_rows), K_(dml_row_info));
  dtl::ObDtlChannelInfo& get_sqc_channel_info()
  {
    return sqc_ch_info_;
  }
  dtl::ObDtlChannelInfo& get_task_channel_info()
  {
    return task_ch_info_;
  }
  void set_task_channel(dtl::ObDtlChannel* ch)
  {
    task_channel_ = ch;
  }
  dtl::ObDtlChannel* get_task_channel()
  {
    return task_channel_;
  }
  void set_sqc_channel(dtl::ObDtlChannel* ch)
  {
    sqc_channel_ = ch;
  }
  dtl::ObDtlChannel* get_sqc_channel()
  {
    return sqc_channel_;
  }
  inline void set_task_state(int32_t flag)
  {
    state_ |= flag;
  }
  inline bool is_task_state_set(int32_t flag) const
  {
    return 0 != (state_ & flag);
  }
  inline void set_task_id(int64_t task_id)
  {
    task_id_ = task_id;
  }
  inline int64_t get_task_id() const
  {
    return task_id_;
  }
  inline void set_qc_id(uint64_t qc_id)
  {
    qc_id_ = qc_id;
  }
  inline int64_t get_qc_id() const
  {
    return qc_id_;
  }
  inline void set_sqc_id(int64_t sqc_id)
  {
    sqc_id_ = sqc_id;
  }
  inline int64_t get_sqc_id() const
  {
    return sqc_id_;
  }
  inline void set_dfo_id(int64_t dfo_id)
  {
    dfo_id_ = dfo_id;
  }
  inline ObPxInterruptID get_interrupt_id()
  {
    return px_int_id_;
  }
  inline void set_interrupt_id(const ObPxInterruptID& px_int_id)
  {
    px_int_id_ = px_int_id;
  }
  inline int64_t get_dfo_id() const
  {
    return dfo_id_;
  }
  inline void set_execution_id(int64_t execution_id)
  {
    execution_id_ = execution_id;
  }
  inline int64_t get_execution_id() const
  {
    return execution_id_;
  }
  inline void set_result(int rc)
  {
    rc_ = rc;
  }
  inline bool has_result() const
  {
    return rc_ <= 0;
  }
  inline int get_result() const
  {
    return rc_;
  }
  void set_exec_addr(const common::ObAddr& addr)
  {
    exec_addr_ = addr;
  }
  void set_sqc_addr(const common::ObAddr& addr)
  {
    sqc_addr_ = addr;
  }
  void set_qc_addr(const common::ObAddr& addr)
  {
    qc_addr_ = addr;
  }
  const common::ObAddr& get_exec_addr() const
  {
    return exec_addr_;
  }
  const common::ObAddr& get_sqc_addr() const
  {
    return sqc_addr_;
  }
  const common::ObAddr& get_qc_addr() const
  {
    return qc_addr_;
  }
  inline void set_task_co_id(const uint64_t& id)
  {
    task_co_id_ = id;
  }
  inline uint64_t get_task_co_id() const
  {
    return task_co_id_;
  }
  inline void set_task_monitor_info(const ObPxTaskMonitorInfo& other)
  {
    task_monitor_info_ = other;
  }
  inline ObPxTaskMonitorInfo get_task_monitor_info() const
  {
    return task_monitor_info_;
  }
  inline ObPxTaskMonitorInfo& get_task_monitor_info()
  {
    return task_monitor_info_;
  }
  void set_fulltree(bool v)
  {
    is_fulltree_ = v;
  }
  bool is_fulltree() const
  {
    return is_fulltree_;
  }
  inline void set_affected_rows(int64_t v)
  {
    affected_rows_ = v;
  }
  int64_t get_affected_rows()
  {
    return affected_rows_;
  }

public:
  // if less than 0, rc is set. task default ret is 1
  static const int64_t TASK_DEFAULT_RET_VALUE = 1;

public:
  uint64_t qc_id_;
  int64_t dfo_id_;
  int64_t sqc_id_;
  int64_t task_id_;
  int64_t execution_id_;
  dtl::ObDtlChannelInfo sqc_ch_info_;
  dtl::ObDtlChannelInfo task_ch_info_;
  dtl::ObDtlChannel* task_channel_;
  dtl::ObDtlChannel* sqc_channel_;
  common::ObAddr sqc_addr_;
  common::ObAddr exec_addr_;
  common::ObAddr qc_addr_;
  int rc_;
  volatile int32_t state_;
  volatile uint64_t task_co_id_;
  ObPxInterruptID px_int_id_;
  ObPxTaskMonitorInfo task_monitor_info_;
  bool is_fulltree_;
  int64_t affected_rows_;
  ObPxDmlRowInfo dml_row_info_;
};

class ObPxRpcInitTaskArgs {
  OB_UNIS_VERSION(1);

public:
  ObPxRpcInitTaskArgs()
      : task_(),
        exec_ctx_(NULL),
        ser_phy_plan_(NULL),
        des_phy_plan_(NULL),
        op_root_(NULL),
        op_spec_root_(nullptr),
        static_engine_root_(nullptr),
        sqc_task_ptr_(NULL),
        des_allocator_(NULL),
        sqc_handler_(nullptr)
  {}

  void set_serialize_param(ObExecContext& exec_ctx, ObPhyOperator& op_root, const ObPhysicalPlan& ser_phy_plan);
  void set_serialize_param(ObExecContext& exec_ctx, ObOpSpec& op_spec_root, const ObPhysicalPlan& ser_phy_plan);
  void set_deserialize_param(ObExecContext& exec_ctx, ObPhysicalPlan& des_phy_plan, ObIAllocator* des_allocator);
  int init_deserialize_param(lib::MemoryContext& mem_context, const observer::ObGlobalContext& gctx);
  int deep_copy_assign(ObPxRpcInitTaskArgs& src, common::ObIAllocator& alloc);

  void destroy()
  {
    if (nullptr != des_phy_plan_) {
      des_phy_plan_->~ObPhysicalPlan();
      des_phy_plan_ = NULL;
    }
    if (nullptr != exec_ctx_) {
      exec_ctx_->~ObExecContext();
      exec_ctx_ = NULL;
    }
    op_root_ = NULL;
    op_spec_root_ = NULL;
    ser_phy_plan_ = NULL;
    sqc_task_ptr_ = NULL;
    des_allocator_ = NULL;
    sqc_handler_ = NULL;
  }

  ObPxSqcHandler* get_sqc_handler()
  {
    return sqc_handler_;
  }

  ObPxRpcInitTaskArgs& operator=(const ObPxRpcInitTaskArgs& other)
  {
    if (&other != this) {
      task_ = other.task_;
      exec_ctx_ = other.exec_ctx_;
      ser_phy_plan_ = other.ser_phy_plan_;
      des_phy_plan_ = other.des_phy_plan_;
      op_root_ = other.op_root_;
      op_spec_root_ = other.op_spec_root_;
      sqc_task_ptr_ = other.sqc_task_ptr_;
      des_allocator_ = other.des_allocator_;
      sqc_handler_ = other.sqc_handler_;
    }
    return *this;
  }
  bool is_invalid()
  {
    return OB_ISNULL(des_phy_plan_) || (OB_ISNULL(op_root_) && OB_ISNULL(op_spec_root_));
  }
  TO_STRING_KV(K_(task));

public:
  ObPxTask task_;
  ObExecContext* exec_ctx_;
  const ObPhysicalPlan* ser_phy_plan_;
  ObPhysicalPlan* des_phy_plan_;
  ObPhyOperator* op_root_;
  ObOpSpec* op_spec_root_;
  ObOperator* static_engine_root_;
  ObPxTask* sqc_task_ptr_;
  ObIAllocator* des_allocator_;
  ObPxSqcHandler* sqc_handler_;
};

struct ObPxRpcInitTaskResponse {
  OB_UNIS_VERSION(1);

public:
  ObPxRpcInitTaskResponse() : task_co_id_(0)
  {}
  TO_STRING_KV(K_(task_co_id));

public:
  uint64_t task_co_id_;
};

struct ObPxRpcInitSqcResponse {
  OB_UNIS_VERSION(1);

public:
  ObPxRpcInitSqcResponse() : rc_(common::OB_NOT_INIT), reserved_thread_count_(0), partitions_info_()
  {}
  TO_STRING_KV(K_(rc), K_(reserved_thread_count));

public:
  int rc_;
  int64_t reserved_thread_count_;
  ObSEArray<ObPxPartitionInfo, 8> partitions_info_;
};

class ObPxWorkerEnvArgs {
public:
  ObPxWorkerEnvArgs()
      : trace_id_(nullptr),
        log_level_(OB_LOG_LEVEL_NONE),
        is_oracle_mode_(false),
        enqueue_timestamp_(-1),
        gctx_(nullptr),
        group_id_(0)
  {}

  virtual ~ObPxWorkerEnvArgs()
  {}

  ObPxWorkerEnvArgs& operator=(const ObPxWorkerEnvArgs& other)
  {
    trace_id_ = other.trace_id_;
    log_level_ = other.log_level_;
    is_oracle_mode_ = other.is_oracle_mode_;
    enqueue_timestamp_ = other.enqueue_timestamp_;
    gctx_ = other.gctx_;
    group_id_ = other.group_id_;
    return *this;
  }

  void set_trace_id(const uint64_t* trace_id)
  {
    trace_id_ = trace_id;
  }
  const uint64_t* get_trace_id() const
  {
    return trace_id_;
  }
  int8_t get_log_level() const
  {
    return log_level_;
  }
  void set_log_level(const int8_t log_level)
  {
    log_level_ = log_level;
  }
  void set_is_oracle_mode(bool oracle_mode)
  {
    is_oracle_mode_ = oracle_mode;
  }
  bool is_oracle_mode() const
  {
    return is_oracle_mode_;
  }
  void set_enqueue_timestamp(int64_t v)
  {
    enqueue_timestamp_ = v;
  }
  int64_t get_enqueue_timestamp() const
  {
    return enqueue_timestamp_;
  }
  void set_gctx(const observer::ObGlobalContext* ctx)
  {
    gctx_ = ctx;
  }
  const observer::ObGlobalContext* get_gctx()
  {
    return gctx_;
  }
  void set_group_id(int32_t v)
  {
    group_id_ = v;
  }
  int32_t get_group_id() const
  {
    return group_id_;
  }

private:
  const uint64_t* trace_id_;
  uint8_t log_level_;
  bool is_oracle_mode_;
  int64_t enqueue_timestamp_;
  const observer::ObGlobalContext* gctx_;
  int32_t group_id_;
};

class ObExecCtxDfoRootOpGuard {
public:
  ObExecCtxDfoRootOpGuard(ObExecContext* exec_ctx, const ObPhyOperator* op)
  {
    exec_ctx_ = NULL;
    op_ = NULL;
    if (OB_NOT_NULL(exec_ctx) && OB_NOT_NULL(op)) {
      op_ = exec_ctx->get_root_op();
      exec_ctx->set_root_op(op);
      exec_ctx_ = exec_ctx;
    }
  }
  ~ObExecCtxDfoRootOpGuard()
  {
    if (OB_NOT_NULL(exec_ctx_)) {
      exec_ctx_->set_root_op(op_);
    }
  }

private:
  ObExecContext* exec_ctx_;
  const ObPhyOperator* op_;
  DISALLOW_COPY_AND_ASSIGN(ObExecCtxDfoRootOpGuard);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OCEANBASE_SQL_ENGINE_PX_DFO_H__ */
//// end of header file
