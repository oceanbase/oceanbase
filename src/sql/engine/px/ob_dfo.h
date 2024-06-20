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
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/px/ob_px_interruption.h"
#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "sql/engine/ob_exec_feedback_info.h"
#include "sql/das/ob_das_define.h"
#include "lib/string/ob_strings.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/detect/ob_detect_callback.h"
namespace oceanbase
{

namespace common
{
class ObIDetectCallback;
}
namespace sql
{

enum class ObDfoState
{
  WAIT,
  BLOCK,
  RUNNING,
  FINISH,
  FAIL
};

#define SQC_TASK_START (1 << 0)
#define SQC_TASK_EXIT  (1 << 1)

class ObPhysicalPlan;
class ObSqcTaskMgr;
class ObPxSqcHandler;
class ObJoinFilter;
class ObPxCoordInfo;

// 在 PX 端描述每个 SQC 的 task
// 通过 exec_addr 区分 SQC
class ObPxTaskMeta
{
public:
  ObPxTaskMeta() : exec_addr_(), sqc_id_(common::OB_INVALID_INDEX),
  task_id_(common::OB_INVALID_INDEX), sm_group_id_(common::OB_INVALID_INDEX) {}
  ~ObPxTaskMeta() = default;
  void set_exec_addr(const common::ObAddr &addr) { exec_addr_ = addr; }
  const common::ObAddr &get_exec_addr() const {   return exec_addr_; }
  void set_task_id(int64_t task_id) { task_id_ = task_id; }
  int64_t get_task_id() const { return task_id_; }
  void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  int64_t get_sqc_id() const { return sqc_id_; }
  void set_sm_group_id(int64_t sm_group_id) { sm_group_id_ = sm_group_id; }
  int64_t get_sm_group_id() const { return sm_group_id_; }
  TO_STRING_KV(K_(exec_addr), K_(sqc_id), K_(task_id));
private:
  common::ObAddr exec_addr_;
  // 考虑到容错重试，同一个 addr 上可能有多个 sqc 存在，通过 sqc_id_ 来区分 task 归属为哪个 sqc
  int64_t sqc_id_;
  // 记录 Task 是 SQC 中的第几个任务，用于 partial partition wise join 场景
  // ref:
  int64_t task_id_;
  // slave map group id
  int64_t sm_group_id_;
};
struct ObSqcTableLocationKey
{
 OB_UNIS_VERSION(1);
public:
  ObSqcTableLocationKey() : table_location_key_(0),
      ref_table_id_(OB_INVALID_ID), tablet_id_(),
      is_dml_(false), is_loc_uncertain_(false) {}
  ObSqcTableLocationKey(int64_t key, int64_t ref_table_id,
      common::ObTabletID &tablet_id, bool is_dml, bool is_loc_uncertain) :
      table_location_key_(key),
      ref_table_id_(ref_table_id),
      tablet_id_(tablet_id),
      is_dml_(is_dml),
      is_loc_uncertain_(is_loc_uncertain) {}
  int64_t table_location_key_;
  int64_t ref_table_id_;
  common::ObTabletID tablet_id_;
  bool is_dml_;
  bool is_loc_uncertain_; // pdml location
  TO_STRING_KV(K_(table_location_key), K_(ref_table_id), K_(tablet_id), K_(is_dml), K_(is_loc_uncertain));
};

struct ObSqcTableLocationIndex
{
public:
  ObSqcTableLocationIndex() : table_location_key_(0),
    location_start_pos_(0), location_end_pos_(0) {}
  ObSqcTableLocationIndex(int64_t key,
      int64_t start_pos,
      int64_t end_pos) :
      table_location_key_(key),
      location_start_pos_(start_pos),
      location_end_pos_(end_pos) {}
  int64_t table_location_key_;
  int64_t location_start_pos_;
  int64_t location_end_pos_;
  TO_STRING_KV(K_(table_location_key), K_(location_start_pos), K_(location_end_pos));
};

struct ObPxDetectableIds
{
  OB_UNIS_VERSION(1);
public:
  ObPxDetectableIds() : qc_detectable_id_(), sqc_detectable_id_() {}
  ObPxDetectableIds(const common::ObDetectableId &qc_detectable_id, const common::ObDetectableId &sqc_detectable_id)
      : qc_detectable_id_(qc_detectable_id), sqc_detectable_id_(sqc_detectable_id) {}
  void operator=(const ObPxDetectableIds &other)
  {
    qc_detectable_id_ = other.qc_detectable_id_;
    sqc_detectable_id_ = other.sqc_detectable_id_;
  }
  bool operator==(const ObPxDetectableIds &other) const
  {
    return qc_detectable_id_ == other.qc_detectable_id_ &&
           sqc_detectable_id_ == other.sqc_detectable_id_;
  }
  TO_STRING_KV(K_(qc_detectable_id), K_(sqc_detectable_id));
  common::ObDetectableId qc_detectable_id_;
  common::ObDetectableId sqc_detectable_id_;
};


struct ObP2PDhMapInfo
{
  OB_UNIS_VERSION(1);
public:
  ObP2PDhMapInfo() : p2p_sequence_ids_(),
                     target_addrs_() {}
  ~ObP2PDhMapInfo() {
    p2p_sequence_ids_.reset();
    target_addrs_.reset();
  }
  void destroy() {
    p2p_sequence_ids_.reset();
    target_addrs_.reset();
  }
  int assign(const ObP2PDhMapInfo &other) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(p2p_sequence_ids_.assign(other.p2p_sequence_ids_))) {
      OB_LOG(WARN, "fail to assign other p2p seq id", K(ret));
    } else if (OB_FAIL(target_addrs_.assign(other.target_addrs_))) {
      OB_LOG(WARN, "fail to assign other target_addrs_", K(ret));
    }
    return ret;
  }
  bool is_empty() { return p2p_sequence_ids_.empty(); }
public:
  common::ObSArray<int64_t> p2p_sequence_ids_;
  common::ObSArray<ObSArray<ObAddr>> target_addrs_;
  TO_STRING_KV(K_(p2p_sequence_ids), K_(target_addrs));
};

struct ObQCMonitoringInfo {
  OB_UNIS_VERSION(1);
public:
  int init(const ObExecContext &exec_ctx);
  int assign(const ObQCMonitoringInfo &other);
  void reset();
public:
  common::ObString cur_sql_;
  // in nested px situation, it is the current px coordinator's thread id
  int64_t qc_tid_;
  TO_STRING_KV(K_(cur_sql), K_(qc_tid));
};

// PX 端描述每个 SQC 的数据结构
class ObPxSqcMeta
{
  OB_UNIS_VERSION(1);
public:
  ObPxSqcMeta() :
              execution_id_(common::OB_INVALID_ID),
              qc_id_(common::OB_INVALID_ID),
              sqc_id_(common::OB_INVALID_ID),
              dfo_id_(common::OB_INVALID_ID),
              branch_id_base_(0),
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
              is_fulltree_(false),
              is_rpc_worker_(false),
              need_report_(false),
              qc_server_id_(common::OB_INVALID_ID),
              parent_dfo_id_(common::OB_INVALID_ID),
              px_sequence_id_(common::OB_INVALID_ID),
              transmit_use_interm_result_(false),
              recieve_use_interm_result_(false),
              serial_receive_channels_(),
              rescan_batch_params_(),
              partition_pruning_table_locations_(),
              temp_table_ctx_(),
              ignore_vtable_error_(false),
              access_table_location_keys_(),
              access_table_location_indexes_(),
              server_not_alive_(false),
              adjoining_root_dfo_(false),
              is_single_tsc_leaf_dfo_(false),
              allocator_("PxSqcMetaInner"),
              access_external_table_files_(),
              px_detectable_ids_(),
              interrupt_by_dm_(false),
              p2p_dh_map_info_(),
              sqc_order_gi_tasks_(false)
  {}
  ~ObPxSqcMeta() = default;
  int assign(const ObPxSqcMeta &other);

  // 输入、输出函数
  /* 获取 qc 端的通信端口，由 qc 端读取并 link */
  dtl::ObDtlChannelInfo &get_qc_channel_info() { return qc_ch_info_; }
  const dtl::ObDtlChannelInfo &get_qc_channel_info_const() const { return qc_ch_info_; }
  /* 获取 sqc 端的通信端口，由 sqc 端读取并 link */
  dtl::ObDtlChannelInfo &get_sqc_channel_info() { return sqc_ch_info_; }
  const dtl::ObDtlChannelInfo &get_sqc_channel_info_const() const { return sqc_ch_info_; }
  ObIArray<ObSqcTableLocationKey> &get_access_table_location_keys() { return access_table_location_keys_; }
  ObIArray<ObSqcTableLocationIndex> &get_access_table_location_indexes() { return access_table_location_indexes_; }
  ObIArray<share::ObExternalFileInfo> &get_access_external_table_files() { return access_external_table_files_; }
  DASTabletLocIArray &get_access_table_locations_for_update() { return access_table_locations_; }
  const DASTabletLocIArray &get_access_table_locations() const { return access_table_locations_; }
  void set_execution_id(uint64_t execution_id) { execution_id_ = execution_id; }
  void set_qc_id(uint64_t qc_id) { qc_id_ = qc_id; }
  void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  void set_dfo_id(int64_t dfo_id) { dfo_id_ = dfo_id; }
  inline ObPxInterruptID get_interrupt_id() { return px_int_id_; }
  inline void set_interrupt_id(const ObPxInterruptID &px_int_id)
                              { px_int_id_ = px_int_id; }
  void set_px_detectable_ids(const ObPxDetectableIds &px_detectable_ids) { px_detectable_ids_ = px_detectable_ids; }
  const ObPxDetectableIds &get_px_detectable_ids() { return px_detectable_ids_; }
  void set_interrupt_by_dm(bool val) { interrupt_by_dm_ = val; }
  bool is_interrupt_by_dm() { return interrupt_by_dm_; }
  void set_exec_addr(const common::ObAddr &addr) {   exec_addr_ = addr; }
  void set_qc_addr(const common::ObAddr &addr) {   qc_addr_ = addr; }
  void set_qc_channel(dtl::ObDtlChannel *ch) {   qc_channel_ = ch; }
  void set_sqc_channel(dtl::ObDtlChannel *ch) { sqc_channel_ = ch; }
  void set_task_count(int64_t task_count) {   task_count_ =  task_count; }
  void set_max_task_count(int64_t max_task_count) {   max_task_count_ =  max_task_count; }
  void set_min_task_count(int64_t min_task_count) {   min_task_count_ =  min_task_count; }
  void set_total_task_count(int64_t total_task_count) {   total_task_count_ =  total_task_count; }
  void set_total_part_count(int64_t total_part_count) {   total_part_count_ =  total_part_count; }

  uint64_t get_execution_id() const { return execution_id_; }
  uint64_t get_qc_id() const { return qc_id_; }
  int64_t get_sqc_id() const { return sqc_id_; }
  int64_t get_dfo_id() const { return dfo_id_; }
  common::ObIArray<ObPxTabletInfo> &get_partitions_info() { return partitions_info_; }
  common::ObIArray<ObSqlTempTableCtx> &get_temp_table_ctx() { return temp_table_ctx_; }

  const common::ObAddr &get_exec_addr() const {   return exec_addr_; }
  const common::ObAddr &get_sqc_addr() const {   return exec_addr_; } // get_exec_addr alias
  const common::ObAddr &get_qc_addr() const {   return qc_addr_; }
  dtl::ObDtlChannel *get_qc_channel() {   return qc_channel_; }
  dtl::ObDtlChannel *get_sqc_channel() { return sqc_channel_; }
  dtl::ObDtlChannel *get_sqc_channel() const { return sqc_channel_; }
  int64_t get_task_count() const {   return task_count_; }
  int64_t get_max_task_count() const {   return max_task_count_; }
  int64_t get_min_task_count() const {   return min_task_count_; }
  int64_t get_total_task_count() const { return total_task_count_; }
  int64_t get_total_part_count() const { return total_part_count_; }
  void set_fulltree(bool v) { is_fulltree_ = v; }
  bool is_fulltree() const { return is_fulltree_; }
  void set_thread_inited(bool v) { thread_inited_ = v; }
  bool is_thread_inited() const { return thread_inited_; }
  void set_thread_finish(bool v) { thread_finish_ = v; }
  bool is_thread_finish() const { return thread_finish_; }
  void set_need_report(bool v) { need_report_ = v; }
  bool need_report() { return need_report_; }
  void set_qc_server_id(int64_t qc_server_id) { qc_server_id_ = qc_server_id; }
  int64_t get_qc_server_id() { return qc_server_id_; }
  void set_parent_dfo_id(int64_t parent_dfo_id) { parent_dfo_id_ = parent_dfo_id; }
  int64_t get_parent_dfo_id() { return parent_dfo_id_; }
  void set_px_sequence_id(uint64_t px_sequence_id) { px_sequence_id_ = px_sequence_id; }
  int64_t get_px_sequence_id() { return px_sequence_id_; }
  void set_ignore_vtable_error(bool flag) { ignore_vtable_error_ = flag; }
  bool is_ignore_vtable_error() { return ignore_vtable_error_; }
  void set_server_not_alive(bool not_alive) { server_not_alive_ = not_alive; }
  bool is_server_not_alive() { return server_not_alive_; }
  ObPxTransmitDataChannelMsg &get_transmit_channel_msg() { return transmit_channel_; }
  ObPxReceiveDataChannelMsg &get_receive_channel_msg() { return receive_channel_; }
  common::ObIArray<ObPxReceiveDataChannelMsg>& get_serial_receive_channels()
  { return serial_receive_channels_; }
  void reset()
  {
    access_table_locations_.reset();
    transmit_channel_.reset();
    receive_channel_.reset();
    serial_receive_channels_.reset();
    rescan_batch_params_.reset();
    partition_pruning_table_locations_.reset();
    access_external_table_files_.reset();
    allocator_.reset();
    monitoring_info_.reset();
  }
  // SQC 端收到 InitSQC 消息后通过 data_channel 信息是否为空
  // 来判断 data channel 是否已经预分配好，是否要走轻量调度
  bool is_prealloc_transmit_channel() const
  {
    return transmit_channel_.has_filled_channel();
  }
  bool is_prealloc_receive_channel() const
  {
    return receive_channel_.has_filled_channel();
  }
  void set_transmit_use_interm_result(bool flag)
  { transmit_use_interm_result_ = flag; }
  void set_recieve_use_interm_result(bool flag)
  { recieve_use_interm_result_ = flag; }
  bool transmit_use_interm_result() const { return transmit_use_interm_result_; }
  bool recieve_use_interm_result() const { return recieve_use_interm_result_; }
  int add_serial_recieve_channel(const ObPxReceiveDataChannelMsg &channel);
  int set_rescan_batch_params(ObBatchRescanParams &params) { return rescan_batch_params_.assign(params); }
  ObBatchRescanParams &get_rescan_batch_params() { return rescan_batch_params_; }
  common::ObIArray<ObTableLocation> &get_pruning_table_locations()
  { return partition_pruning_table_locations_; }
  void set_adjoining_root_dfo(bool flag)
  { adjoining_root_dfo_ = flag; }
  bool adjoining_root_dfo() const { return adjoining_root_dfo_; }
  void set_single_tsc_leaf_dfo(bool flag) { is_single_tsc_leaf_dfo_ = flag; }
  bool is_single_tsc_leaf_dfo() { return is_single_tsc_leaf_dfo_; }
  ObP2PDhMapInfo &get_p2p_dh_map_info() { return p2p_dh_map_info_;};
  void set_sqc_count(int64_t sqc_cnt) { sqc_count_ = sqc_cnt; }
  int64_t get_sqc_count() const { return sqc_count_;}
  void set_sqc_order_gi_tasks(bool v) { sqc_order_gi_tasks_ = v; }
  bool sqc_order_gi_tasks() const { return sqc_order_gi_tasks_; }
  ObQCMonitoringInfo &get_monitoring_info() { return monitoring_info_; }
  const ObQCMonitoringInfo &get_monitoring_info() const { return monitoring_info_; }
  void set_branch_id_base(const int16_t branch_id_base) { branch_id_base_ = branch_id_base; }
  int16_t get_branch_id_base() const { return branch_id_base_; }
  TO_STRING_KV(K_(need_report), K_(execution_id), K_(qc_id), K_(sqc_id), K_(dfo_id), K_(exec_addr), K_(qc_addr),
               K_(branch_id_base), K_(qc_ch_info), K_(sqc_ch_info),
               K_(task_count), K_(max_task_count), K_(min_task_count),
               K_(thread_inited), K_(thread_finish), K_(px_int_id),
               K_(transmit_use_interm_result),
               K_(recieve_use_interm_result), K_(serial_receive_channels),
               K_(sqc_order_gi_tasks));
private:
  uint64_t execution_id_;
  uint64_t qc_id_;
  int64_t sqc_id_;
  int64_t dfo_id_;
  // branch id is used to distinguish datas written concurrently by px-workers
  // for replace and insert update operator, they need branch_id to rollback writes by one px-worker
  int16_t branch_id_base_;
  ObQCMonitoringInfo monitoring_info_;
  // The partition location information of the all table_scan op and dml op
  // used for px worker execution
  // no need serialize
  DASTabletLocSEArray access_table_locations_;

  ObPxTransmitDataChannelMsg transmit_channel_; // 用于快速建立 QC-Task 通道模式
  ObPxReceiveDataChannelMsg receive_channel_; // 用于快速建立 QC-Task 通道模式
  dtl::ObDtlChannelInfo qc_ch_info_;
  dtl::ObDtlChannelInfo sqc_ch_info_;
  dtl::ObDtlChannel *qc_channel_; /* 用于 qc 给 sqc 发送数据 */
  dtl::ObDtlChannel *sqc_channel_; /* 用于 sqc 给 qc 发送数据 */
  common::ObAddr exec_addr_; /* SQC 的运行地址 */
  common::ObAddr qc_addr_; /* 记录 QC 的地址，用于 SQC-QC 通信 */
  int64_t task_count_; /* 每个 DFO 会被拆分成 task-count 个 SQC 并发执行 */
  int64_t max_task_count_;
  int64_t min_task_count_;
  int64_t total_task_count_;
  int64_t total_part_count_;
  bool thread_inited_;
  bool thread_finish_;
  ObPxInterruptID px_int_id_;
  bool is_fulltree_;
  bool is_rpc_worker_;
  // No need to serialize
  ObSEArray<ObPxTabletInfo, 8> partitions_info_;
  bool need_report_;
  uint64_t qc_server_id_;
  int64_t parent_dfo_id_;
  uint64_t px_sequence_id_;
  // 以下两个变量在单层调度dfo场景中会使用
  // 标记sqc在执行transmit/recieve算子时是否从中间结收/发数据
  bool transmit_use_interm_result_;
  bool recieve_use_interm_result_;
  // 新增receive_channels, sqc meta中捎带channel
  // 在串行调度依赖此数组获取receive_channels.
  common::ObSEArray<ObPxReceiveDataChannelMsg, 1>serial_receive_channels_;
  // 在NLJ分布式batch rescan场景下使用
  // 用于存放NLJ条件下推的参数
  ObBatchRescanParams rescan_batch_params_;
  // for partition pruning
  common::ObSEArray<ObTableLocation, 2> partition_pruning_table_locations_;
  // sqc中需要中间结果文件写的数据块id的数组
  ObSEArray<ObSqlTempTableCtx, 2> temp_table_ctx_;
  // all child is virtual table, need ignore error.
  bool ignore_vtable_error_;
  ObSEArray<ObSqcTableLocationKey, 2> access_table_location_keys_;
  ObSEArray<ObSqcTableLocationIndex, 2> access_table_location_indexes_;
  bool server_not_alive_;
  /*used for init channel msg, indicate a transmit
  op is adjoining coodinator, that need not wait that msg*/
  bool adjoining_root_dfo_;
  //for auto scale
  bool is_single_tsc_leaf_dfo_;
  ObArenaAllocator allocator_;
  ObSEArray<share::ObExternalFileInfo, 8> access_external_table_files_;

  ObPxDetectableIds px_detectable_ids_;
  bool interrupt_by_dm_;
  // for p2p dh msg
  ObP2PDhMapInfo p2p_dh_map_info_;
  int64_t sqc_count_;
  bool sqc_order_gi_tasks_;
};

class ObDfo
{
  template <class T>
  friend class DfoTreeNormalizer;
  friend class ObDfoMgr;
  using TaskFilterFunc = std::function<bool(const ObPxTaskChSet &)>;
public:
  static const int64_t MAX_DFO_ID = INT_MAX32;
public:
  ObDfo(common::ObIAllocator &allocator) :
    allocator_(allocator),
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
    root_op_spec_(nullptr),
    child_dfos_(),
    has_scan_(false),
    has_dml_op_(false),
    has_need_branch_id_op_(false),
    has_temp_scan_(false),
    is_active_(false),
    is_scheduled_(false),
    thread_inited_(false),
    thread_finish_(false),
    depend_sibling_(NULL),
    has_depend_sibling_(false),
    parent_(NULL),
    receive_ch_sets_map_(),
    dfo_ch_infos_(),
    is_fulltree_(false),
    is_rpc_worker_(false),
    earlier_sched_(false),
    qc_server_id_(common::OB_INVALID_ID),
    parent_dfo_id_(common::OB_INVALID_ID),
    px_sequence_id_(common::OB_INVALID_ID),
    temp_table_id_(0),
    slave_mapping_type_(SlaveMappingType::SM_NONE),
    part_ch_map_(),
    total_task_cnt_(0),
    pkey_table_loc_id_(0),
    tsc_op_cnt_(0),
    external_table_files_(),
    px_detectable_ids_(),
    detect_cb_(nullptr),
    node_sequence_id_(0),
    p2p_dh_ids_(),
    p2p_dh_addrs_(),
    p2p_dh_loc_(nullptr),
    need_p2p_info_(false),
    p2p_dh_map_info_(),
    coord_info_ptr_(nullptr),
    force_bushy_(false)
  {
  }

  virtual ~ObDfo() = default;
  inline void set_execution_id(uint64_t execution_id) { execution_id_ = execution_id; }
  inline uint64_t get_execution_id() { return execution_id_; }
  inline void set_qc_id(uint64_t qc_id) { qc_id_ = qc_id; }
  inline uint64_t get_qc_id() { return qc_id_; }
  inline ObPxInterruptID &get_interrupt_id() { return px_int_id_; }
  inline void set_interrupt_id(const ObPxInterruptID &px_int_id)
                              { px_int_id_ = px_int_id; }
  inline void set_dop(const int64_t dop) { dop_ = dop; }
  inline int64_t get_dop() const { return dop_; }
  void set_assigned_worker_count(int64_t cnt) { assigned_worker_cnt_ = cnt; }
  int64_t get_assigned_worker_count() const { return assigned_worker_cnt_; }
  void set_used_worker_count(int64_t cnt) { used_worker_cnt_ = cnt; }
  int64_t get_used_worker_count() const { return used_worker_cnt_; }
  inline void set_single(const bool single) { is_single_ = single; }
  inline bool is_single() const { return is_single_; }
  inline void set_root_dfo(bool is_root) {is_root_dfo_ = is_root;}
  inline bool is_root_dfo() const {return is_root_dfo_;}
  // 注意：一个 dfo 可能有多个 receive 算子，和多个 child dfo 通信
  // 这里仅用于标记和第一个 child dfo 通信的 receive 算子通道分配状态
  inline void set_prealloc_receive_channel(const bool v) {prealloced_receive_channel_ = v;}
  inline bool is_prealloc_receive_channel() const {return prealloced_receive_channel_;}
  inline void set_prealloc_transmit_channel(const bool v) {prealloced_transmit_channel_ = v;}
  inline bool is_prealloc_transmit_channel() const {return prealloced_transmit_channel_;}
  inline void set_phy_plan(const ObPhysicalPlan *phy_plan) { phy_plan_ = phy_plan; }
  inline const ObPhysicalPlan *get_phy_plan() const { return phy_plan_; }
  inline void set_root_op_spec(const ObOpSpec *op_spec) {root_op_spec_ = op_spec;}
  inline const ObOpSpec *get_root_op_spec() { return root_op_spec_; }
  inline void get_root(const ObOpSpec *&root) const { root = root_op_spec_; }
  inline void set_scan(bool has_scan) { has_scan_ = has_scan; }
  inline bool has_scan_op() const { return has_scan_; }
  inline void set_dml_op(bool has_dml_op) { has_dml_op_ = has_dml_op; }
  inline bool has_dml_op() { return has_dml_op_; }
  inline void set_need_branch_id_op(bool has_need_branch_id_op) { has_need_branch_id_op_ = has_need_branch_id_op; }
  inline bool has_need_branch_id_op() const { return has_need_branch_id_op_; }
  inline void set_temp_table_scan(bool has_scan) { has_temp_scan_ = has_scan; }
  inline bool has_temp_table_scan() const { return has_temp_scan_; }
  inline bool is_fast_dfo() const { return is_prealloc_receive_channel() || is_prealloc_transmit_channel(); }
  inline void set_slave_mapping_type(SlaveMappingType v) { slave_mapping_type_ = v; }
  inline SlaveMappingType get_slave_mapping_type() { return slave_mapping_type_; }
  inline bool is_slave_mapping() { return SlaveMappingType::SM_NONE != slave_mapping_type_; }

  ObPxPartChMapArray &get_part_ch_map() { return part_ch_map_; }

  // DFO 分布，DFO 在各个 server 上的任务状态
  int add_sqc(const ObPxSqcMeta &sqc);
  int get_addrs(common::ObIArray<common::ObAddr> &addrs) const;
  int get_sqcs(common::ObIArray<ObPxSqcMeta *> &sqcs);
  int get_sqcs(common::ObIArray<const ObPxSqcMeta *> &sqcs) const;
  int get_sqc(int64_t idx, ObPxSqcMeta *&sqc);
  common::ObIArray<ObPxSqcMeta>  &get_sqcs() { return sqcs_; }
  int64_t get_sqcs_count() { return sqcs_.count(); }
  int build_tasks();
  int alloc_data_xchg_ch();
  /* 获取 qc 端的 channel 端口 */
  int get_qc_channels(common::ObIArray<dtl::ObDtlChannel *> &sqc_chs);


  /* DFO 关系图

   假设先调度 dfo3，则 dfo2 称作 dfo3 的 depend sibling，如下图：

                                  dfo1
                                /      \
        dfo1's 1st child  ---> dfo2    dfo3 <--- dfo1's 2nd child
                                ^
                                |
   dfo3's depend sibling  ------+
   */
  inline int append_child_dfo(ObDfo *dfo) { return child_dfos_.push_back(dfo); }
  inline int get_child_dfo(int64_t idx, ObDfo *&dfo) const { return child_dfos_.at(idx, dfo); }
  inline int64_t get_child_count() const { return child_dfos_.count(); }
  ObIArray<ObDfo*> &get_child_dfos() { return child_dfos_; }
  inline bool has_child_dfo() const { return get_child_count() > 0; }
  // 本 DFO 即使优先调度，但可能依赖于另外的 DFO 调度才能推进本 DFO
  // 使用 depend_sibling 记录依赖的 DFO
  void set_depend_sibling(ObDfo *sibling) { depend_sibling_ = sibling; }
  void set_has_depend_sibling(bool has_depend_sibling) { has_depend_sibling_ = has_depend_sibling; }
  bool has_depend_sibling() const { return has_depend_sibling_; }
  ObDfo *depend_sibling() const { return depend_sibling_; }
  void set_parent(ObDfo *parent) { parent_ = parent; }
  bool has_parent() const { return NULL != parent_; }
  ObDfo *parent() const { return parent_; }
  // 下面两个函数用于3层DFO 调度，使得HASH JOIN上面无需接MATERIAL算子，
  // 流式输出结果集，被标记为 earlier_sched_ 的 dfo 被提前调度，直接消费JOIN结果
  void set_earlier_sched(bool earlier) { earlier_sched_ = earlier; }
  bool is_earlier_sched() const { return earlier_sched_; }
  void set_dfo_id(int64_t dfo_id) { dfo_id_ = dfo_id; }
  int64_t get_dfo_id() const { return dfo_id_; }

  void set_qc_server_id(int64_t qc_server_id) { qc_server_id_ = qc_server_id; }
  int64_t get_qc_server_id() const { return qc_server_id_; }
  void set_parent_dfo_id(int64_t parent_dfo_id) { parent_dfo_id_ = parent_dfo_id; }
  int64_t get_parent_dfo_id() const { return parent_dfo_id_; }

  void set_px_sequence_id(uint64_t px_sequence_id) { px_sequence_id_ = px_sequence_id; }
  int64_t get_px_sequence_id() const { return px_sequence_id_; }

  void set_px_detectable_ids(const ObPxDetectableIds &px_detectable_ids) { px_detectable_ids_ = px_detectable_ids; }
  const ObPxDetectableIds &get_px_detectable_ids() { return px_detectable_ids_; }
  common::ObIDetectCallback *get_detect_cb() { return detect_cb_; }
  void set_detect_cb(common::ObIDetectCallback *cb) { detect_cb_ = cb; }
  void set_node_sequence_id(uint64_t node_sequence_id) { node_sequence_id_ = node_sequence_id; }
  uint64_t get_node_sequence_id() { return node_sequence_id_; }

  void set_temp_table_id(uint64_t temp_table_id) { temp_table_id_ = temp_table_id; }
  uint64_t get_temp_table_id() const { return temp_table_id_; }

  // TODO: 以下四个状态需要重新梳理，太乱了
  void set_active() { is_active_ = true; }
  bool is_active() const { return is_active_; }
  void set_scheduled() { is_scheduled_ = true; }
  bool is_scheduled() const { return is_scheduled_; }
  void set_thread_inited(bool v) { thread_inited_ = v; }
  bool is_thread_inited() const { return thread_inited_; }
  void set_thread_finish(bool v) { thread_finish_ = v; }
  bool is_thread_finish() const { return thread_finish_; }
  const common::ObArray<ObPxTaskMeta> &get_tasks() const { return tasks_; }
  int get_task_receive_chs_for_update(int64_t child_dfo_id,
                           common::ObIArray<ObPxTaskChSet *> &ch_sets);
  int get_task_transmit_chs_for_update(common::ObIArray<ObPxTaskChSet *> &ch_sets);
  int get_task_receive_chs(int64_t child_dfo_id,
                           ObPxTaskChSets &ch_sets) const;
  int get_task_receive_chs(int64_t child_dfo_id,
                           ObPxTaskChSets &ch_sets,
                           TaskFilterFunc filter) const;
  int get_task_transmit_chs(ObPxTaskChSets &ch_sets,
                            TaskFilterFunc filter) const;

  static void reset_resource(ObDfo *dfo);
  void set_fulltree(bool v) { is_fulltree_ = v; }
  bool is_fulltree() const { return is_fulltree_; }
  bool check_root_valid();
  const ObPhysicalPlan* get_plan_by_root();

  void set_dist_method(ObPQDistributeMethod::Type dist_method) { dist_method_ = dist_method; }
  ObPQDistributeMethod::Type get_dist_method() { return dist_method_; }
  ObPxChTotalInfos &get_dfo_ch_total_infos() { return dfo_ch_infos_; }
  int64_t get_total_task_count() { return total_task_cnt_; }
  int get_dfo_ch_info(int64_t sqc_idx, dtl::ObDtlChTotalInfo *&ch_info);
  int prepare_channel_info();
  static int check_dfo_pair(ObDfo &parent, ObDfo &child, int64_t &child_dfo_idx);
  static int fill_channel_info_by_sqc(
              dtl::ObDtlExecServer &ch_servers,
              common::ObIArray<ObPxSqcMeta> &sqcs);
  static int fill_channel_info_by_sqc(
              dtl::ObDtlExecServer &ch_servers,
              ObPxSqcMeta &sqc);
  static bool is_valid_dfo_id(int64_t dfo_id)
  {
    return (dfo_id >= 0 && dfo_id <= MAX_DFO_ID) ||
           (dfo_id == MAX_DFO_ID);
  }
  void set_pkey_table_loc_id(int64_t id) { pkey_table_loc_id_ = id; }
  int64_t get_pkey_table_loc_id() { return pkey_table_loc_id_; };
  void inc_tsc_op_cnt() { tsc_op_cnt_++; }
  bool is_leaf_dfo() { return child_dfos_.empty(); }
  bool is_single_tsc_leaf_dfo() { return is_leaf_dfo() && 1 == tsc_op_cnt_; }
  common::ObIArray<share::ObExternalFileInfo> &get_external_table_files() { return external_table_files_; }
  int add_p2p_dh_ids(int64_t id) { return p2p_dh_ids_.push_back(id); }
  common::ObIArray<int64_t> &get_p2p_dh_ids() { return p2p_dh_ids_; }
  void set_p2p_dh_loc(ObDASTableLoc *p2p_dh_loc) { p2p_dh_loc_ = p2p_dh_loc; }
  ObDASTableLoc *get_p2p_dh_loc() { return p2p_dh_loc_; }
  common::ObIArray<ObAddr> &get_p2p_dh_addrs() { return p2p_dh_addrs_; }
  void set_need_p2p_info(bool flag) { need_p2p_info_ = flag; }
  bool need_p2p_info() { return need_p2p_info_; }
  void set_coord_info_ptr(ObPxCoordInfo *ptr) { coord_info_ptr_ = ptr; }
  ObPxCoordInfo *get_coord_info_ptr() { return coord_info_ptr_; }
  ObP2PDhMapInfo &get_p2p_dh_map_info() { return p2p_dh_map_info_;};
  bool force_bushy() { return force_bushy_; }
  void set_force_bushy(bool flag) { force_bushy_ = flag; }
  TO_STRING_KV(K_(execution_id),
               K_(dfo_id),
               K_(is_active),
               K_(earlier_sched),
               K_(is_scheduled),
               K_(thread_inited),
               K_(thread_finish),
               K_(dop),
               K_(assigned_worker_cnt),
               K_(used_worker_cnt),
               K_(is_single),
               K_(is_root_dfo),
               K_(is_fulltree),
               K_(has_scan),
               K_(sqcs),
               KP_(depend_sibling),
               KP_(parent),
               "child", get_child_count(),
               K_(slave_mapping_type),
               K_(dist_method),
               K_(pkey_table_loc_id),
               K_(tsc_op_cnt),
               K_(transmit_ch_sets),
               K_(receive_ch_sets_map),
               K_(p2p_dh_ids),
               K_(p2p_dh_addrs),
               K_(need_p2p_info));

private:
  DISALLOW_COPY_AND_ASSIGN(ObDfo);
private:
  int calc_total_task_count();
private:
  common::ObIAllocator &allocator_;
  uint64_t execution_id_;
  uint64_t qc_id_;
  int64_t dfo_id_;
  ObPxInterruptID px_int_id_;
  // dop 是用户/优化器建议的并发度，为所有 server 上预期分配的 worker 数之和
  // used_worker_cnt 用于统计当前 dfo 一共消耗了多少个 px worker 才完成
  // 三者的关系：
  // 1. 当 dop 较小，且访问的 partition 分布在较多 server 上时，
  // 为了保证每个 server 至少有一个 px worker 去服务，
  // used_worker_cnt 比实际 dop 大
  // 2. 当 server 上线程不足，实际创建的 worker 数可能比预期少（DOP 降级）
  // userd_worker_cnt 可能比 dop 小
  // 3. assigned_worker_cnt 是根据 admission 许可的线程数计算出的每个 dfo 可以取得的线程数
  int64_t dop_;
  int64_t assigned_worker_cnt_;
  int64_t used_worker_cnt_;
  // is_single 用于标记此dfo用一个线程去调度, 既可能在本地, 也可能在远程.
  // 如果此dfo包含table scan算子, 调度时将跟随table scan location调度
  // 如果此dfo不包含table scan算子, 调度时理论上可以在任意server去调度,
  // 但在当前的实现上, 不包含table scan算子时, 会在本地调度
  bool is_single_;
  bool is_root_dfo_;
  /* 数据通道不等 sqc 返回建立了多少个 worker 就预分配好了 */
  bool prealloced_receive_channel_; // 第一个 receive 算子的通道已经分配好
  bool prealloced_transmit_channel_;
  const ObPhysicalPlan *phy_plan_;
  const ObOpSpec *root_op_spec_;
  common::ObSEArray<ObDfo *, 4> child_dfos_;
  bool has_scan_; // DFO 中包含至少一个 scan 算子，或者仅仅包含一个dml
  bool has_dml_op_; // DFO中可能包含一个dml
  bool has_need_branch_id_op_; // DFO 中有算子需要分配branch_id
  bool has_temp_scan_;
  bool is_active_;
  bool is_scheduled_;
  bool thread_inited_;
  bool thread_finish_;
  ObDfo *depend_sibling_; // 依赖其它边的调度才能执行完成，目前只支持记录一个依赖
  bool has_depend_sibling_;
  ObDfo *parent_; // 依赖其它边的调度才能执行完成，目前只支持记录一个依赖
  ObPxTaskChSets transmit_ch_sets_;
  common::ObArray<ObPxTaskChSets *>receive_ch_sets_map_;
  ObPxChTotalInfos dfo_ch_infos_;
  common::ObArray<ObPxSqcMeta> sqcs_; // 所有 server 都分配好后初始化
  common::ObArray<ObPxTaskMeta> tasks_; // 所有 SQC 都 setup 完成后根据 SQC 记录的实际分配线程数初始化
  bool is_fulltree_;
  bool is_rpc_worker_;
  bool earlier_sched_; // 标记本 dfo 是否是因为 3 DFO 调度策略而被提前调度起来了
  uint64_t qc_server_id_;
  int64_t parent_dfo_id_;
  uint64_t px_sequence_id_;
  uint64_t temp_table_id_;
  SlaveMappingType slave_mapping_type_;
  ObPxPartChMapArray part_ch_map_;
  ObPQDistributeMethod::Type dist_method_;
  int64_t total_task_cnt_;      // the task total count of dfo start worker
  int64_t pkey_table_loc_id_; // record pkey table loc id for child dfo
  int64_t tsc_op_cnt_;
  common::ObArray<share::ObExternalFileInfo> external_table_files_;
  // for dm
  ObPxDetectableIds px_detectable_ids_;
  common::ObIDetectCallback *detect_cb_;
  uint64_t node_sequence_id_;
  // ---------------
  // for p2p dh mgr
  common::ObArray<int64_t>p2p_dh_ids_; //for dh create
  common::ObArray<ObAddr>p2p_dh_addrs_; //for dh use
  ObDASTableLoc *p2p_dh_loc_;
  bool need_p2p_info_;
  ObP2PDhMapInfo p2p_dh_map_info_;
  // ---------------
  ObPxCoordInfo *coord_info_ptr_;
  bool force_bushy_;
};


class ObSqcSerializeCache
{
public:
  ObSqcSerializeCache() :
      cache_serialized_(false),enable_serialize_cache_(false),
      len1_(0),len2_(0),buf1_(nullptr),buf2_(nullptr),slen_(0) {}
  common::ObArenaAllocator allocator_;
  // after serialized, enable serialize cache to avoid unnecessary serialization
  bool cache_serialized_;
  bool enable_serialize_cache_; // disable for 1 sqc per dfo, otherwise enable
  int64_t len1_;
  int64_t len2_;
  void *buf1_;
  void *buf2_;
  int64_t slen_; // serialize length cache
};

class ObPxRpcInitSqcArgs {
  OB_UNIS_VERSION(1);
public:
  ObPxRpcInitSqcArgs()
      : sqc_(),
        exec_ctx_(NULL),
        ser_phy_plan_(NULL),
        des_phy_plan_(NULL),
        op_spec_root_(nullptr),
        static_engine_root_(nullptr),
        des_allocator_(NULL),
        sqc_handler_(NULL),
        scan_spec_ops_(),
        qc_order_gi_tasks_(true)
  {}
  ~ObPxRpcInitSqcArgs() = default;

  void set_serialize_param(ObExecContext &exec_ctx,
                           ObOpSpec &op_spec_root,
                           const ObPhysicalPlan &ser_phy_plan);
  void set_deserialize_param(ObExecContext &exec_ctx,
                             ObPhysicalPlan &des_phy_plan,
                             ObIAllocator *des_allocator);
  int assign(ObPxRpcInitSqcArgs &other);
  int do_deserialize(int64_t &pos, const char *buf, int64_t data_len);

  void set_static_engine_root(ObOperator &root)
  { static_engine_root_ = &root; }
  void enable_serialize_cache()
  {
    ser_cache_.enable_serialize_cache_ = true;
  }
  TO_STRING_KV(K_(sqc));

private:
  int serialize_common_parts_1(char *buf, const int64_t buf_len, int64_t &pos) const;
  int serialize_common_parts_2(char *buf, const int64_t buf_len, int64_t &pos) const;
public:
  ObPxSqcMeta sqc_;
  ObExecContext *exec_ctx_;
  const ObPhysicalPlan *ser_phy_plan_;
  ObPhysicalPlan *des_phy_plan_;
  ObOpSpec *op_spec_root_;
  ObOperator *static_engine_root_;
  ObIAllocator *des_allocator_;
  // no need to serialize
  ObPxSqcHandler *sqc_handler_;
  ObSEArray<const ObTableScanSpec*, 8> scan_spec_ops_;
  ObSqcSerializeCache ser_cache_;
  // whether qc support order gi tasks. default value is true and set false before deserialize.
  bool qc_order_gi_tasks_;
};

struct ObPxCleanDtlIntermResInfo
{
  OB_UNIS_VERSION(1);
public:
  ObPxCleanDtlIntermResInfo() : ch_total_info_(), sqc_id_(common::OB_INVALID_ID), task_count_(0) {}
  ObPxCleanDtlIntermResInfo(dtl::ObDtlChTotalInfo &ch_info, int64_t sqc_id, int64_t task_count) :
    ch_total_info_(ch_info), sqc_id_(sqc_id), task_count_(task_count)
  {}

  ~ObPxCleanDtlIntermResInfo() { }
  void reset()
  {
    ch_total_info_.reset();
    sqc_id_ = common::OB_INVALID_ID;
    task_count_ = 0;
  }

  TO_STRING_KV(K_(ch_total_info), K_(sqc_id), K_(task_count));
public:
  dtl::ObDtlChTotalInfo ch_total_info_;
  int64_t sqc_id_;
  int64_t task_count_;
};

class ObPxCleanDtlIntermResArgs
{
  OB_UNIS_VERSION(1);
public:
  ObPxCleanDtlIntermResArgs() : info_(), batch_size_(0) {}
  ~ObPxCleanDtlIntermResArgs() { reset(); }
  void reset()
  {
    info_.reset();
    batch_size_ = 0;
  }

  TO_STRING_KV(K_(info), K_(batch_size));
public:
  ObSEArray<ObPxCleanDtlIntermResInfo, 8> info_;
  uint64_t batch_size_;
};

class ObPxTask
{
  OB_UNIS_VERSION(1);
public:
  ObPxTask()
    : qc_id_(common::OB_INVALID_ID),
      dfo_id_(0),
      sqc_id_(0),
      task_id_(-1),
      branch_id_(0),
      execution_id_(0),
      task_channel_(NULL),
      sqc_channel_(NULL),
      rc_(TASK_DEFAULT_RET_VALUE), // 小于等于 0 表示设置了 rc 值
      das_retry_rc_(common::OB_SUCCESS),
      state_(0),
      task_co_id_(0),
      px_int_id_(),
      is_fulltree_(false),
      affected_rows_(0),
      dml_row_info_(),
      temp_table_id_(common::OB_INVALID_ID),
      interm_result_ids_(),
      tx_desc_(NULL),
      is_use_local_thread_(false),
      fb_info_(),
      err_msg_(),
      memstore_read_row_count_(0),
      ssstore_read_row_count_(0)
  {

  }
  ~ObPxTask() = default;
  ObPxTask &operator=(const ObPxTask &other)
  {
    qc_id_ = other.qc_id_;
    dfo_id_ = other.dfo_id_;
    sqc_id_ = other.sqc_id_;
    task_id_ = other.task_id_;
    branch_id_ = other.branch_id_;
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
    is_fulltree_ = other.is_fulltree_;
    affected_rows_ = other.affected_rows_;
    dml_row_info_ = other.dml_row_info_;
    temp_table_id_ = other.temp_table_id_;
    interm_result_ids_.assign(other.interm_result_ids_);
    tx_desc_ = other.tx_desc_;
    is_use_local_thread_ = other.is_use_local_thread_;
    fb_info_.assign(other.fb_info_);
    memstore_read_row_count_ = other.memstore_read_row_count_;
    ssstore_read_row_count_ = other.ssstore_read_row_count_;
    return *this;
  }
public:
  TO_STRING_KV(K_(qc_id),
               K_(dfo_id),
               K_(sqc_id),
               K_(task_id),
               K_(branch_id),
               K_(execution_id),
               K_(sqc_ch_info),
               K_(task_ch_info),
               K_(sqc_addr),
               K_(exec_addr),
               K_(qc_addr),
               K_(rc),
               K_(das_retry_rc),
               K_(task_co_id),
               K_(px_int_id),
               K_(is_fulltree),
               K_(affected_rows),
               K_(dml_row_info),
               K_(temp_table_id),
               K_(interm_result_ids),
               K_(tx_desc),
               K_(is_use_local_thread),
               K_(fb_info),
               K_(memstore_read_row_count),
               K_(ssstore_read_row_count));
  dtl::ObDtlChannelInfo &get_sqc_channel_info() { return sqc_ch_info_; }
  dtl::ObDtlChannelInfo &get_task_channel_info() { return task_ch_info_; }
  void set_task_channel(dtl::ObDtlChannel *ch) { task_channel_ = ch; }
  dtl::ObDtlChannel *get_task_channel() { return task_channel_; }
  void set_sqc_channel(dtl::ObDtlChannel *ch) { sqc_channel_ = ch; }
  dtl::ObDtlChannel *get_sqc_channel() { return sqc_channel_; }
  inline void set_task_state(int32_t flag) { state_ |= flag; }
  inline bool is_task_state_set(int32_t flag) const { return 0 != (state_ & flag); }
  inline void set_task_id(int64_t task_id) { task_id_ = task_id; }
  inline int64_t get_task_id() const { return task_id_; }
  inline void set_branch_id(int16_t branch_id) { branch_id_ = branch_id; }
  inline int16_t get_branch_id() const { return branch_id_; }
  inline void set_qc_id(uint64_t qc_id) { qc_id_ = qc_id; }
  inline int64_t get_qc_id() const { return qc_id_; }
  inline void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  inline int64_t get_sqc_id() const { return sqc_id_; }
  inline void set_dfo_id(int64_t dfo_id) { dfo_id_ = dfo_id; }
  inline ObPxInterruptID get_interrupt_id() { return px_int_id_; }
  inline void set_interrupt_id(const ObPxInterruptID &px_int_id)
                              { px_int_id_ = px_int_id; }
  inline int64_t get_dfo_id() const { return dfo_id_; }
  inline void set_execution_id(int64_t execution_id) { execution_id_ = execution_id; }
  inline int64_t get_execution_id() const { return execution_id_; }
  inline void set_result(int rc) { rc_ = rc; }
  inline bool has_result() const { return rc_ <= 0; }
  inline int get_result() const { return rc_; }
  void set_das_retry_rc(int das_retry_rc)
  { das_retry_rc_ = (das_retry_rc_ == common::OB_SUCCESS ? das_retry_rc : das_retry_rc_); }
  int get_das_retry_rc() const { return das_retry_rc_; }
  void set_exec_addr(const common::ObAddr &addr) {   exec_addr_ = addr; }
  void set_sqc_addr(const common::ObAddr &addr) {   sqc_addr_ = addr; }
  void set_qc_addr(const common::ObAddr &addr) {   qc_addr_ = addr; }
  const common::ObAddr &get_exec_addr() const {   return exec_addr_; }
  const common::ObAddr &get_sqc_addr() const {   return sqc_addr_; }
  const common::ObAddr &get_qc_addr() const {   return qc_addr_; }
  inline void set_task_co_id(const uint64_t &id) { task_co_id_ = id; }
  inline uint64_t get_task_co_id() const { return task_co_id_; }
  void set_fulltree(bool v) { is_fulltree_ = v; }
  bool is_fulltree() const { return is_fulltree_; }
  inline void set_affected_rows(int64_t v) { affected_rows_ = v; }
  int64_t get_affected_rows() { return affected_rows_; }
  transaction::ObTxDesc *&get_tx_desc() { return tx_desc_; }
  void set_use_local_thread(bool flag) { is_use_local_thread_ = flag; }
  bool is_use_local_thread() { return is_use_local_thread_; }
  ObExecFeedbackInfo &get_feedback_info() { return fb_info_; };
  const ObPxUserErrorMsg &get_err_msg() const { return err_msg_; }
  ObPxUserErrorMsg &get_err_msg() { return err_msg_; }
  void set_memstore_read_row_count(int64_t v) { memstore_read_row_count_ = v; }
  void set_ssstore_read_row_count(int64_t v) { ssstore_read_row_count_ = v; }
  int64_t get_memstore_read_row_count() const { return memstore_read_row_count_; }
  int64_t get_ssstore_read_row_count() const { return ssstore_read_row_count_; }
public:
  // 小于等于0表示设置了rc 值, task default ret值为1
  static const int64_t TASK_DEFAULT_RET_VALUE = 1;
public:
  uint64_t qc_id_;
  int64_t dfo_id_;
  int64_t sqc_id_;
  int64_t task_id_;
  int16_t branch_id_;
  int64_t execution_id_;
  dtl::ObDtlChannelInfo sqc_ch_info_;
  dtl::ObDtlChannelInfo task_ch_info_;
  dtl::ObDtlChannel *task_channel_;
  dtl::ObDtlChannel *sqc_channel_;
  common::ObAddr sqc_addr_; /* 记录 SQC 的地址，用于 SQC-Task 通信 */
  common::ObAddr exec_addr_; /* Task 的运行地址 */
  common::ObAddr qc_addr_;  /*记录 QC 的地址，用于中断*/
  int rc_;
  int das_retry_rc_;
  volatile int32_t state_; // 被 task 线程设置
  volatile uint64_t task_co_id_; /* task 的协程 id */
  ObPxInterruptID px_int_id_;
  bool is_fulltree_; // 标记序列化时这个 task 的 op tree 是否包含会被ex 算子截断
  int64_t affected_rows_; // pdml情况下，每个task涉及到的行
  ObPxDmlRowInfo dml_row_info_; // DML情况下, 需要统计行相关信息
  uint64_t temp_table_id_;
  common::ObSEArray<uint64_t, 8> interm_result_ids_;  //返回每个task生成的结果集
  transaction::ObTxDesc *tx_desc_; // transcation information
  bool is_use_local_thread_;
  ObExecFeedbackInfo fb_info_; //for feedback info
  ObPxUserErrorMsg err_msg_; // for error msg & warning msg
  int64_t memstore_read_row_count_; // the count of row from mem
  int64_t ssstore_read_row_count_; // the count of row from disk
};

class ObPxRpcInitTaskArgs
{
  OB_UNIS_VERSION(1);
public:
  ObPxRpcInitTaskArgs()
      : task_(),
        exec_ctx_(NULL),
        ser_phy_plan_(NULL),
        des_phy_plan_(NULL),
        op_spec_root_(nullptr),
        static_engine_root_(nullptr),
        sqc_task_ptr_(NULL),
        des_allocator_(NULL),
        sqc_handler_(nullptr)
  {}

  void set_serialize_param(ObExecContext &exec_ctx,
                           ObOpSpec &op_spec_root,
                           const ObPhysicalPlan &ser_phy_plan);
  void set_deserialize_param(ObExecContext &exec_ctx,
                             ObPhysicalPlan &des_phy_plan,
                             ObIAllocator *des_allocator);
  int init_deserialize_param(lib::MemoryContext &mem_context,
                           const observer::ObGlobalContext &gctx);
  int deep_copy_assign(ObPxRpcInitTaskArgs &src,
                      common::ObIAllocator &alloc);

  void destroy() {
    // worker 执行完成后，立即释放当前 worker 持有的 physical plan、 exec ctx 等资源
    // 内含各种算子的上下文内存等
    if (nullptr != des_phy_plan_) {
      des_phy_plan_->~ObPhysicalPlan();
      des_phy_plan_ = NULL;
    }
    if (nullptr != exec_ctx_) {
      exec_ctx_->~ObExecContext();
      exec_ctx_ = NULL;
    }
    op_spec_root_ = NULL;
    ser_phy_plan_ = NULL;
    sqc_task_ptr_ = NULL;
    des_allocator_ = NULL;
    sqc_handler_ = NULL;
  }

  ObPxSqcHandler *get_sqc_handler() {
    return sqc_handler_;
  }

  ObPxRpcInitTaskArgs &operator = (const ObPxRpcInitTaskArgs &other) {
    if (&other != this) {
      task_ = other.task_;
      exec_ctx_ = other.exec_ctx_;
      ser_phy_plan_ = other.ser_phy_plan_;
      des_phy_plan_ = other.des_phy_plan_;
      op_spec_root_ = other.op_spec_root_;
      sqc_task_ptr_ = other.sqc_task_ptr_;
      des_allocator_ = other.des_allocator_;
      sqc_handler_ = other.sqc_handler_;
    }
    return *this;
  }
  bool is_invalid()
  {
    return OB_ISNULL(des_phy_plan_) || OB_ISNULL(op_spec_root_);
  }
  TO_STRING_KV(K_(task));
public:
  ObPxTask task_;
  ObExecContext *exec_ctx_;
  const ObPhysicalPlan *ser_phy_plan_;
  ObPhysicalPlan *des_phy_plan_;
  ObOpSpec *op_spec_root_;
  ObOperator *static_engine_root_;
  ObPxTask *sqc_task_ptr_; // 指针指向 SQC Ctx task 数组中对应的 task
  ObIAllocator *des_allocator_;
  ObPxSqcHandler *sqc_handler_; // 指向 SQC Handler 内存
};

struct ObPxRpcInitTaskResponse
{
  OB_UNIS_VERSION(1);
public:
  ObPxRpcInitTaskResponse()
      : task_co_id_(0)
  {}
  TO_STRING_KV(K_(task_co_id));
public:
  uint64_t task_co_id_;
};

struct ObPxRpcInitSqcResponse
{
  OB_UNIS_VERSION(1);
public:
  ObPxRpcInitSqcResponse()
      : rc_(common::OB_NOT_INIT),
        reserved_thread_count_(0),
        partitions_info_(),
        sqc_order_gi_tasks_(false)
  {}
  TO_STRING_KV(K_(rc), K_(reserved_thread_count));
public:
  int rc_;
  int64_t reserved_thread_count_;
  ObSEArray<ObPxTabletInfo, 8> partitions_info_;
  bool sqc_order_gi_tasks_;
};

class ObPxWorkerEnvArgs
{
public :
  typedef common::ObCurTraceId::TraceId TraceId;
  ObPxWorkerEnvArgs () : trace_id_(), log_level_(OB_LOG_LEVEL_NONE),
  is_oracle_mode_(false), enqueue_timestamp_(-1), gctx_(nullptr),
  group_id_(0) { }

  virtual ~ObPxWorkerEnvArgs() { }

  ObPxWorkerEnvArgs &operator = (const ObPxWorkerEnvArgs &other) {
    trace_id_ = other.trace_id_;
    log_level_ = other.log_level_;
    is_oracle_mode_ = other.is_oracle_mode_;
    enqueue_timestamp_ = other.enqueue_timestamp_;
    gctx_ = other.gctx_;
    group_id_ = other.group_id_;
    return *this;
  }

  void set_trace_id(TraceId& trace_id) { trace_id_ = trace_id; }
  const TraceId& get_trace_id() const { return trace_id_; }
  int8_t get_log_level() const { return log_level_; }
  void set_log_level(const int8_t log_level) { log_level_ = log_level; }
  void set_is_oracle_mode(bool oracle_mode) { is_oracle_mode_ = oracle_mode; }
  bool is_oracle_mode() const { return is_oracle_mode_; }
  void set_enqueue_timestamp(int64_t v) { enqueue_timestamp_ = v; }
  int64_t get_enqueue_timestamp() const { return enqueue_timestamp_; }
  void set_gctx(const observer::ObGlobalContext *ctx) { gctx_ = ctx; }
  const observer::ObGlobalContext *get_gctx() { return gctx_; }
  void set_group_id(int32_t v) { group_id_ = v; }
  int32_t get_group_id() const { return group_id_; }

private:
  TraceId trace_id_;
  uint8_t log_level_;
  bool is_oracle_mode_;
  int64_t enqueue_timestamp_;
  const observer::ObGlobalContext *gctx_;
  int32_t group_id_;
};

}
}
#endif /* __OCEANBASE_SQL_ENGINE_PX_DFO_H__ */
//// end of header file
