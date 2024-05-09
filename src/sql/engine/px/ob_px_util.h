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

#ifndef __OCEANBASE_SQL_ENGINE_PX_UTIL_H__
#define __OCEANBASE_SQL_ENGINE_PX_UTIL_H__

#include "share/unit/ob_unit_info.h"
#include "lib/container/ob_array.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/ob_phy_table_location.h"
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "sql/engine/px/ob_px_op_size_factor.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/ob_engine_op_traits.h"

namespace oceanbase
{
namespace sql
{
const int64_t PX_RESCAN_BATCH_ROW_COUNT = 8192;
class ObIExtraStatusCheck;
enum ObBcastOptimization {
  BC_TO_WORKER,
  BC_TO_SERVER,
};

// 监听各类事件，如 root dfo 调度事件，etc
class ObIPxCoordEventListener
{
public:
  virtual int on_root_data_channel_setup() = 0;
};


struct ObExprExtraSerializeInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprExtraSerializeInfo() :
    current_time_(nullptr),
    last_trace_id_(nullptr),
    mview_ids_(nullptr),
    last_refresh_scns_(nullptr)
    { }
  common::ObObj *current_time_;
  common::ObCurTraceId::TraceId *last_trace_id_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> *mview_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> *last_refresh_scns_;
};

class ObPxSqcUtil
{
public:
  static double get_sqc_partition_ratio(ObExecContext *exec_ctx);
  static double get_sqc_est_worker_ratio(ObExecContext *exec_ctx);

  static int64_t get_est_total_worker_count(ObExecContext *exec_ctx);
  static int64_t get_est_sqc_worker_count(ObExecContext *exec_ctx);

  static int64_t get_total_partition_count(ObExecContext *exec_ctx);
  static int64_t get_sqc_total_partition_count(ObExecContext *exec_ctx);

  static int64_t get_actual_total_worker_count(ObExecContext *exec_ctx);
  static int64_t get_actual_worker_count(ObExecContext *exec_ctx);

  static uint64_t get_plan_id(ObExecContext *exec_ctx);
  static const char* get_sql_id(ObExecContext *exec_ctx);
  static uint64_t get_exec_id(ObExecContext *exec_ctx);
  static uint64_t get_session_id(ObExecContext *exec_ctx);
};

// 考虑兼容，目前设置为不是互斥
class ObPxEstimateSizeUtil
{
public:
  static int get_px_size(ObExecContext *exec_ctx, const PxOpSizeFactor factor,
    const int64_t total_size, int64_t &ret_size);
};

class ObSlaveMapItem
{
public:
  ObSlaveMapItem() : group_id_(0), l_worker_count_(0), r_worker_count_(0),
  l_tablet_id_(OB_INVALID_INDEX_INT64), r_tablet_id_(OB_INVALID_INDEX_INT64)
  {
  }
  ~ObSlaveMapItem() = default;
  int64_t group_id_;
  /*
   *                HASH JOIN(parent worker_count)
   *                      |
   *      ---------------------------------
   *      |                               |
   *     TSC1(l_worker_count_)          TSC2(r_worker_count_)
   *
   */
  int64_t p_worker_count_;
  int64_t l_worker_count_;
  int64_t r_worker_count_;
  int64_t l_tablet_id_;
  int64_t r_tablet_id_;
};


typedef common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode> ObTabletIdxMap;

class ObPXServerAddrUtil
{
  class ObPxSqcTaskCountMeta {
  public:
    ObPxSqcTaskCountMeta() : partition_count_(0), thread_count_(0),
    time_(0), idx_(0), finish_(false) {}
    ~ObPxSqcTaskCountMeta() = default;
    int64_t partition_count_;
    int64_t thread_count_;
    double time_;
    int64_t idx_;
    bool finish_;
    TO_STRING_KV(K_(partition_count), K_(thread_count), K_(time), K_(idx), K_(finish));
  };
public:
  ObPXServerAddrUtil() = default;
  ~ObPXServerAddrUtil() = default;
  static int alloc_by_data_distribution(const ObIArray<ObTableLocation> *table_locations,
                                        ObExecContext &ctx,
                                        ObDfo &dfo);

  static int alloc_by_data_distribution_inner(
      const ObIArray<ObTableLocation> *table_locations,
      ObExecContext &ctx, ObDfo &dfo);

  static int alloc_by_child_distribution(const ObDfo &child,
                                         ObDfo &parent);
  static int alloc_by_random_distribution(ObExecContext &exec_ctx,
                                          const ObDfo &child,
                                          ObDfo &parent);
  static int alloc_by_temp_child_distribution(ObExecContext &ctx,
                                              ObDfo &child);
  static int alloc_by_temp_child_distribution_inner(ObExecContext &ctx,
                                                    ObDfo &child);
  static int alloc_by_local_distribution(ObExecContext &exec_ctx,
                                         ObDfo &root);
  static int alloc_by_reference_child_distribution(const ObIArray<ObTableLocation> *table_locations,
                                                   ObExecContext &exec_ctx,
                                                   ObDfo &child,
                                                   ObDfo &parent);
  static int split_parallel_into_task(const int64_t parallelism,
                                      const common::ObIArray<int64_t> &sqc_partition_count,
                                      common::ObIArray<int64_t> &results);
  static int build_tablet_idx_map(
      const share::schema::ObTableSchema *table_schema,
      ObTabletIdxMap &idx_map);
  static int find_dml_ops(common::ObIArray<const ObTableModifySpec *> &insert_ops,
                          const ObOpSpec &op);
  static int get_external_table_loc(
      ObExecContext &ctx,
      uint64_t table_id,
      uint64_t ref_table_id,
      const ObQueryRange &pre_query_range,
      ObDfo &dfo,
      ObDASTableLoc *&table_loc);

private:
  static int find_dml_ops_inner(common::ObIArray<const ObTableModifySpec *> &insert_ops,
                             const ObOpSpec &op);

  static int check_partition_wise_location_valid(DASTabletLocIArray &tsc_locations);
  static int build_tablet_idx_map(
      ObTaskExecutorCtx &task_exec_ctx,
      int64_t tenant_id,
      uint64_t ref_table_id,
      ObTabletIdxMap &idx_map);
  static int reorder_all_partitions(int64_t location_key,
      int64_t ref_table_id,
      const DASTabletLocList &src_locations,
      DASTabletLocIArray &tsc_locations,
      bool asc, ObExecContext &exec_ctx, ObIArray<int64_t> &base_order);
  static int build_dynamic_partition_table_location(common::ObIArray<const ObTableScanSpec*> &scan_ops,
      const ObIArray<ObTableLocation> *table_locations, ObDfo &dfo);

  static int build_dfo_sqc(ObExecContext &ctx,
                           const DASTabletLocList &locations,
                           ObDfo &dfo);

  /**
   * Calculate the partition information of all tables involved in the current DFO,
   * and record the partition information in the corresponding SQC.
   * At present, the partition information in a DFO can include:
   * 1. partition information of the tsc corresponding table
   * 2. the partition information of the INSERT/REPLACE corresponding table;
   *      Do not consider the information of the table corresponding to DELETE, UPDATE
   *      (the table corresponding to DELETE or UPDATE must appear in the TSC)
   * TODO: Consider the problem of deduplication of the same partition of the table
   *       corresponding to insert and tsc in the presence of INSERT
   */
  static int set_dfo_accessed_location(ObExecContext &ctx,
                                       int64_t base_table_location_key,
                                       ObDfo &dfo,
                                       common::ObIArray<const ObTableScanSpec *> &scan_ops,
                                       const ObTableModifySpec* dml_op,
                                       ObDASTableLoc *dml_loc);
  /**
   * Add the partition information (table_loc) involved in the
   * current phy_op to the corresponding SQC access location
   */
  static int set_sqcs_accessed_location(ObExecContext &ctx,
                                        int64_t base_table_location_key,
                                        ObDfo &dfo,
                                        ObIArray<int64_t> &base_order,
                                        const ObDASTableLoc *table_loc,
                                        const ObOpSpec *phy_op);
  /**
   * Get the access sequence of the partition of the current phy_op,
   * the access sequence of the phy_op partition is determined by
   * the access sequence of the GI of the dfo where it is located:
   * 1. DESC
   * 2. ASC
   * 目前phy_op的类型只可能是TSC或者INSERT
   */
  static int get_access_partition_order(
    ObDfo &dfo,
    const ObOpSpec *phy_op,
    bool &asc_order);

  /**
   * Recursively query the corresponding GI operator in the DFO
   * from the phy_op to obtain the corresponding GI access sequence;
   * root represents the root op of the current DFO
   */
  static int get_access_partition_order_recursively(
    const ObOpSpec *root,
    const ObOpSpec *phy_op,
    bool &asc_order);


  static int adjust_sqc_task_count(common::ObIArray<ObPxSqcTaskCountMeta> &sqc_tasks,
                                   int64_t parallel,
                                   int64_t partition);
  static int do_random_dfo_distribution(const common::ObIArray<common::ObAddr> &src_addrs,
                                        int64_t dst_addrs_count,
                                        common::ObIArray<common::ObAddr> &dst_addrs);
  static int sort_and_collect_local_file_distribution(common::ObIArray<share::ObExternalFileInfo> &files,
                                                      common::ObIArray<common::ObAddr> &dst_addrs);
  static int assign_external_files_to_sqc(const common::ObIArray<share::ObExternalFileInfo> &files,
                                          bool is_file_on_disk,
                                          common::ObIArray<ObPxSqcMeta *> &sqcs);
private:
  static int generate_dh_map_info(ObDfo &dfo);
  DISALLOW_COPY_AND_ASSIGN(ObPXServerAddrUtil);
};


class ObPxOperatorVisitor
{
public:
  class ApplyFunc
  {
  public:
    virtual int apply(ObExecContext &ctx, ObOpSpec &input) = 0;
    //TODO. For compatibilty now, to be remove on 4.2
    virtual int reset(ObOpSpec &input) = 0;
  };
public:
  static int visit(ObExecContext &ctx, ObOpSpec &root, ApplyFunc &func);
};


class ObPxPartitionLocationUtil
{
public:
  /**
   * get all tables' partition info, and store them to sqc_ctx's partition_array_.
   * we need these infos to start trans.
   * IN        tscs
   * IN        tsc_locations
   * OUT       tablets
   */
  static int get_all_tables_tablets(const common::ObIArray<const ObTableScanSpec*> &scan_ops,
                                    const DASTabletLocIArray &all_locations,
                                    const common::ObIArray<ObSqcTableLocationKey> &tsc_location_keys,
                                    ObSqcTableLocationKey dml_location_key,
                                    common::ObIArray<DASTabletLocArray> &tablets);
};

class ObPxTreeSerializer
{
public:
  // serialize plan tree for engine3.0
  static int serialize_tree(char *buf,
                            int64_t buf_len,
                            int64_t &pos,
                            ObOpSpec &root,
                            bool is_fulltree,
                            const common::ObAddr &run_svr,
                            ObPhyOpSeriCtx *seri_ctx = NULL);
  static int deserialize_tree(const char *buf,
                              int64_t data_len,
                              int64_t &pos,
                              ObPhysicalPlan &phy_plan,
                              ObOpSpec *&root,
                              ObIArray<const ObTableScanSpec *> &tsc_ops);
  static int deserialize_tree(const char *buf,
                              int64_t data_len,
                              int64_t &pos,
                              ObPhysicalPlan &phy_plan,
                              ObOpSpec *&root);
  static int deserialize_op(const char *buf,
                            int64_t data_len,
                            int64_t &pos,
                            ObPhysicalPlan &phy_plan,
                            ObOpSpec *&root);
  static int serialize_sub_plan(char *buf,
                                int64_t buf_len,
                                int64_t &pos,
                                ObOpSpec &root);
  static int deserialize_sub_plan(const char *buf,
                                  int64_t data_len,
                                  int64_t &pos,
                                  ObPhysicalPlan &phy_plan,
                                  ObOpSpec *&op);
  static int serialize_op_input(char *buf,
                                int64_t buf_len,
                                int64_t &pos,
                                ObOpSpec &op_spec,
                                ObOpKitStore &op_kit_store,
                                bool is_fulltree);
  static int deserialize_op_input(const char *buf,
                                  int64_t buf_len,
                                  int64_t &pos,
                                  ObOpKitStore &op_kit_store);
  static int serialize_op_input_tree(
                              char *buf,
                              int64_t buf_len,
                              int64_t &pos,
                              ObOpSpec &op_spec,
                              ObOpKitStore &op_kit_store,
                              bool is_fulltree,
                              int32_t &real_input_count);
  static int serialize_op_input_subplan(
                              char *buf,
                              int64_t buf_len,
                              int64_t &pos,
                              ObOpSpec &op_spec,
                              ObOpKitStore &op_kit_store,
                              bool is_fulltree,
                              int32_t &real_input_count);
  static int64_t get_serialize_op_input_size(
                              ObOpSpec &op_spec,
                              ObOpKitStore &op_kit_store,
                              bool is_fulltree);
  static int64_t get_serialize_op_input_subplan_size(
                              ObOpSpec &op_spec,
                              ObOpKitStore &op_kit_store,
                              bool is_fulltree);
  static int64_t get_serialize_op_input_tree_size(
                              ObOpSpec &op_spec,
                              ObOpKitStore &op_kit_store,
                              bool is_fulltree);

  static int64_t get_sub_plan_serialize_size(ObOpSpec &root);

  static int64_t get_tree_serialize_size(ObOpSpec &root, bool is_fulltree,
      ObPhyOpSeriCtx *seri_ctx = NULL);

  static int serialize_expr_frame_info(char *buf,
                                       int64_t buf_len,
                                       int64_t &pos,
                                       ObExecContext &ctx,
                                       ObExprFrameInfo &expr_frame_info);
  static int serialize_frame_info(char *buf,
                                       int64_t buf_len,
                                       int64_t &pos,
                                       const ObIArray<ObFrameInfo> &all_frame,
                                       char **frames,
                                       const int64_t frame_cnt,
                                       bool no_ser_data = false);
  static int deserialize_frame_info(const char *buf,
                                      int64_t buf_len,
                                      int64_t &pos,
                                      ObIAllocator &allocator,
                                      ObIArray<ObFrameInfo> &all_frame,
                                      ObIArray<char *> *char_ptrs_,
                                      char **&frames,
                                      int64_t &frame_cnt,
                                      bool no_deser_data = false);
  static int deserialize_expr_frame_info(const char *buf,
                                       int64_t buf_len,
                                       int64_t &pos,
                                       ObExecContext &ctx,
                                       ObExprFrameInfo &expr_frame_info);
  static int64_t get_serialize_frame_info_size(
                                       const ObIArray<ObFrameInfo> &all_frame,
                                       char **frames,
                                       const int64_t frame_cnt,
                                       bool no_ser_data = false);
  static int64_t get_serialize_expr_frame_info_size(
                                      ObExecContext &ctx,
                                      ObExprFrameInfo &expr_frame_info);
};

class ObPxChannelUtil
{
public:
  static int unlink_ch_set(
             dtl::ObDtlChSet &ch_set, sql::dtl::ObDtlFlowControl *dfc, const bool batch_free_ch);
  static int flush_rows(common::ObIArray<dtl::ObDtlChannel*> &channels);

  static int set_transmit_metric(
    common::ObIArray<sql::dtl::ObDtlChannel*> &channels,
    sql::ObOpMetric &metric);
  static int set_receive_metric(
    common::ObIArray<sql::dtl::ObDtlChannel*> &channels,
    sql::ObOpMetric &metric);

  // asyn wait
  static int dtl_channles_asyn_wait(
            common::ObIArray<dtl::ObDtlChannel*> &channels, bool ignore_error = false);
  static int sqcs_channles_asyn_wait(common::ObIArray<sql::ObPxSqcMeta *> &sqcs);
};

class ObPxAffinityByRandom
{
public:
  struct TabletHashValue
  {
    int64_t tablet_id_;
    int64_t tablet_idx_;
    uint64_t hash_value_;
    int64_t worker_id_;
    ObPxTabletInfo partition_info_;
    TO_STRING_KV(K_(tablet_id), K_(tablet_idx), K_(hash_value), K_(worker_id), K_(partition_info));
  };
public:
  ObPxAffinityByRandom(bool order_partitions) :
  worker_cnt_(0), tablet_hash_values_(), order_partitions_(order_partitions) {}
  virtual ~ObPxAffinityByRandom() = default;
  int reserve(int64_t size) { return tablet_hash_values_.reserve(size); }
  int add_partition(int64_t tablet_id,
      int64_t tablet_idx,
      int64_t worker_cnt,
      uint64_t tenant_id,
      ObPxTabletInfo &partition_row_info);
  int do_random(bool use_partition_info, uint64_t tenant_id);
  const ObIArray<TabletHashValue> &get_result() { return tablet_hash_values_; }
  static int get_tablet_info(int64_t tablet_id, ObIArray<ObPxTabletInfo> &partitions_info, ObPxTabletInfo &partition_info);
private:
  int64_t worker_cnt_;
  ObSEArray<TabletHashValue, 8> tablet_hash_values_;
  bool order_partitions_;
};

class ObSlaveMapUtil
{
public:
  ObSlaveMapUtil() = default;
  ~ObSlaveMapUtil() = default;
  static int build_ch_map(ObExecContext &ctx, ObDfo &parent, ObDfo &child);
  static int build_mn_ch_map(ObExecContext &ctx,
                            ObDfo &child,
                            ObDfo &parent,
                            uint64_t tenant_id);
  static int build_mn_channel(ObPxChTotalInfos *dfo_ch_total_infos,
                              ObDfo &child,
                              ObDfo &parent,
                              const uint64_t tenant_id);
  static int build_bf_mn_channel(dtl::ObDtlChTotalInfo &transmit_ch_info,
                                ObDfo &child,
                                ObDfo &parent,
                                const uint64_t tenant_id);
private:
  // new channel map generate
  static int build_ppwj_ch_mn_map(ObExecContext &ctx, ObDfo &parent, ObDfo &child, uint64_t tenant_id);
  static int build_pkey_random_ch_mn_map(ObDfo &parent, ObDfo &child, uint64_t tenant_id);
  static int build_ppwj_slave_mn_map(ObDfo &parent, ObDfo &child, uint64_t tenant_id);
  static int build_ppwj_bcast_slave_mn_map(ObDfo &parent, ObDfo &child, uint64_t tenant_id);
  static int build_partition_map_by_sqcs(common::ObIArray<ObPxSqcMeta *> &sqcs,
                                         ObDfo &child,
                                         common::ObIArray<int64_t> &prefix_task_counts,
                                         ObPxPartChMapArray &map);
  // 用于构建 slave mapping类型的partition wise join的channel map
  // channel是在各自个SQC内部建立
  static int build_pwj_slave_map_mn_group(ObDfo &parent, ObDfo &child, uint64_t tenant_id);
  static int build_mn_channel_per_sqcs(ObPxChTotalInfos *dfo_ch_total_infos,
                                      ObDfo &child,
                                      ObDfo &parent,
                                      int64_t sqc_count, uint64_t tenant_id);
  // 下面两个函数是用于 pdml，由于 3.1 之前没有 pdml，所以无需实现非 mn 版
  static int build_pkey_affinitized_ch_mn_map(
      ObDfo &parent,
      ObDfo &child,
      uint64_t tenant_id);
  static int build_affinitized_partition_map_by_sqcs(
      common::ObIArray<ObPxSqcMeta *> &sqcs,
      ObDfo &child,
      ObIArray<int64_t> &prefix_task_counts,
      int64_t total_task_count,
      ObPxPartChMapArray &map);
  static int get_pkey_table_locations(int64_t table_location_key,
      ObPxSqcMeta &sqc,
      DASTabletLocIArray &pkey_locations);
};

using DTLChannelPredFunc = std::function<void(dtl::ObDtlChannel*)>;

class ObDtlChannelUtil
{
public:
  static int link_ch_set(dtl::ObDtlChSet &ch_set,
                        common::ObIArray<dtl::ObDtlChannel*> &channels,
                        DTLChannelPredFunc pred,
                        dtl::ObDtlFlowControl *dfc = nullptr);
  static int get_receive_dtl_channel_set(
              const int64_t sqc_id,
              const int64_t task_id,
              dtl::ObDtlChTotalInfo &ch_total_info,
              dtl::ObDtlChSet &ch_set) {
    return ch_total_info.is_local_shuffle_ ?
            get_sm_receive_dtl_channel_set(sqc_id, task_id, ch_total_info, ch_set) :
            get_mn_receive_dtl_channel_set(sqc_id, task_id, ch_total_info, ch_set);
  }
  static int get_mn_receive_dtl_channel_set(
              const int64_t sqc_id,
              const int64_t task_id,
              dtl::ObDtlChTotalInfo &ch_total_info,
              dtl::ObDtlChSet &ch_set);
  static int get_sm_receive_dtl_channel_set(
              const int64_t sqc_id,
              const int64_t task_id,
              dtl::ObDtlChTotalInfo &ch_total_info,
              dtl::ObDtlChSet &ch_set);
  static int get_transmit_dtl_channel_set(
              const int64_t sqc_id,
              const int64_t task_id,
              dtl::ObDtlChTotalInfo &ch_total_info,
              dtl::ObDtlChSet &ch_set) {
    return ch_total_info.is_local_shuffle_ ?
            get_sm_transmit_dtl_channel_set(sqc_id, task_id, ch_total_info, ch_set) :
            get_mn_transmit_dtl_channel_set(sqc_id, task_id, ch_total_info, ch_set);
  }
  static int get_mn_transmit_dtl_channel_set(
              const int64_t sqc_id,
              const int64_t task_id,
              dtl::ObDtlChTotalInfo &ch_total_info,
              dtl::ObDtlChSet &ch_set);
  static int get_sm_transmit_dtl_channel_set(
              const int64_t sqc_id,
              const int64_t task_id,
              dtl::ObDtlChTotalInfo &ch_total_info,
              dtl::ObDtlChSet &ch_set);
  static int get_transmit_bf_dtl_channel_set(
              const int64_t sqc_id,
              dtl::ObDtlChTotalInfo &ch_total_info,
              dtl::ObDtlChSet &ch_set);
  static int get_receive_bf_dtl_channel_set(
              const int64_t sqc_id,
              dtl::ObDtlChTotalInfo &ch_total_info,
              dtl::ObDtlChSet &ch_set);
};


class ObExtraServerAliveCheck : public ObIExtraStatusCheck
{
public:
  ObExtraServerAliveCheck(const ObAddr &qc_addr, int64_t query_start_time) :
    qc_addr_(qc_addr), dfo_mgr_(nullptr), last_check_time_(0), query_start_time_(query_start_time)
  {
    cluster_id_ = GCONF.cluster_id;
  }
  ObExtraServerAliveCheck(ObDfoMgr &dfo_mgr, int64_t query_start_time) :
    qc_addr_(), dfo_mgr_(&dfo_mgr), last_check_time_(0), query_start_time_(query_start_time)
  {
    cluster_id_ = GCONF.cluster_id;
  }
  const char *name() const override { return "qc alive check"; }
  int check() const override;
  int do_check() const;

private:
  ObAddr qc_addr_;
  ObDfoMgr *dfo_mgr_;
  int64_t cluster_id_;
  mutable int64_t last_check_time_;
  // when check dst server not in blacklist, also check its server_start_time_ < query_start_time_;
  int64_t query_start_time_;
};

class ObVirtualTableErrorWhitelist
{
public:
  static bool should_ignore_vtable_error(int error_code);
};

class ObPxCheckAlive
{
public:
  static bool is_in_blacklist(const common::ObAddr &addr, int64_t server_start_time);
};

class ObPxErrorUtil
{
public:
  static inline void update_qc_error_code(int &current_error_code,
                                           const int new_error_code,
                                           const ObPxUserErrorMsg &from)
  {
    int ret = OB_SUCCESS;
    // **replace** error code & error msg
    if (new_error_code != ObPxTask::TASK_DEFAULT_RET_VALUE) {
      if ((OB_SUCCESS == current_error_code ||
           OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == current_error_code ||
           OB_GOT_SIGNAL_ABORTING == current_error_code) &&
           OB_SUCCESS != new_error_code) {
        current_error_code = new_error_code;
        FORWARD_USER_ERROR(new_error_code, from.msg_);
      }
    }
    // **append** warning msg
    for (int i = 0; i < from.warnings_.count(); ++i) {
      const common::ObWarningBuffer::WarningItem &warning_item = from.warnings_.at(i);
      if (ObLogger::USER_WARN == warning_item.log_level_) {
        FORWARD_USER_WARN(warning_item.code_, warning_item.msg_);
      } else if (ObLogger::USER_NOTE == warning_item.log_level_) {
        FORWARD_USER_NOTE(warning_item.code_, warning_item.msg_);
      }
    }
  }

  static inline void update_sqc_error_code(int &current_error_code,
                                           const int new_error_code,
                                           const ObPxUserErrorMsg &from,
                                           ObPxUserErrorMsg &to)
  {
    int ret = OB_SUCCESS;
    // **replace** error code & error msg
    if (new_error_code != ObPxTask::TASK_DEFAULT_RET_VALUE) {
      if ((OB_SUCCESS == current_error_code) ||
          ((OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == current_error_code ||
            OB_GOT_SIGNAL_ABORTING == current_error_code) &&
           OB_SUCCESS != new_error_code)) {
        current_error_code = new_error_code;
        (void)snprintf(to.msg_, common::OB_MAX_ERROR_MSG_LEN, "%s", from.msg_);
      }
    }
    // **append** warning msg
    for (int i = 0; i < from.warnings_.count(); ++i) {
      if (OB_FAIL(to.warnings_.push_back(from.warnings_.at(i)))) {
        SQL_LOG(WARN, "Failed to add warning. ignore error & continue", K(ret));
      }
    }
  }

  //update the error code if it is OB_HASH_NOT_EXIST or OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER
  static inline void update_error_code(int &current_error_code, const int new_error_code)
  {
    if (new_error_code != ObPxTask::TASK_DEFAULT_RET_VALUE) {
      if ((OB_SUCCESS == current_error_code) ||
          ((OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == current_error_code ||
            OB_GOT_SIGNAL_ABORTING == current_error_code) &&
           OB_SUCCESS != new_error_code)) {
        current_error_code = new_error_code;
      }
    }
  }
};

template<class T>
static int get_location_addrs(const T &locations,
                              ObIArray<ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<ObAddr> addr_set;
  if (OB_FAIL(addr_set.create(locations.size()))) {
    SQL_LOG(WARN, "fail create addr set", K(locations.size()), K(ret));
  }
  for (auto iter = locations.begin(); OB_SUCC(ret) && iter != locations.end(); ++iter) {
    ret = addr_set.exist_refactored((*iter)->server_);
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(addrs.push_back((*iter)->server_))) {
        SQL_LOG(WARN, "fail push back server", K(ret));
      } else if (OB_FAIL(addr_set.set_refactored((*iter)->server_))) {
        SQL_LOG(WARN, "fail set addr to addr_set", K(ret));
      }
    } else {
      SQL_LOG(WARN, "fail check server exist in addr_set", K(ret));
    }
  }
  (void)addr_set.destroy();
  return ret;
}

class LowestCommonAncestorFinder
{
public:
  static int find_op_common_ancestor(
      const ObOpSpec *left, const ObOpSpec *right, const ObOpSpec *&ancestor);
  static int get_op_dfo(const ObOpSpec *op, ObDfo *root_dfo, ObDfo *&op_dfo);
};

}
}

#endif /* __OCEANBASE_SQL_ENGINE_PX_UTIL_H__ */
//// end of header file
