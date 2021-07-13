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

#include "common/ob_unit_info.h"
#include "lib/container/ob_array.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/ob_phy_table_location.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/px/ob_granule_iterator.h"
#include "sql/engine/px/ob_px_op_size_factor.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/ob_engine_op_traits.h"

namespace oceanbase {
namespace sql {
enum ObBcastOptimization {
  BC_TO_WORKER,
  BC_TO_SERVER,
};

// monitor various events, such as root dfo scheduling events, etc
class ObIPxCoordEventListener {
public:
  virtual int on_root_data_channel_setup() = 0;
};

class ObPxSqcUtil {
public:
  static double get_sqc_partition_ratio(ObExecContext* exec_ctx);
  static double get_sqc_est_worker_ratio(ObExecContext* exec_ctx);

  static int64_t get_est_total_worker_count(ObExecContext* exec_ctx);
  static int64_t get_est_sqc_worker_count(ObExecContext* exec_ctx);

  static int64_t get_total_partition_count(ObExecContext* exec_ctx);
  static int64_t get_sqc_total_partition_count(ObExecContext* exec_ctx);

  static int64_t get_actual_total_worker_count(ObExecContext* exec_ctx);
  static int64_t get_actual_worker_count(ObExecContext* exec_ctx);

  static uint64_t get_plan_id(ObExecContext* exec_ctx);
  static const char* get_sql_id(ObExecContext* exec_ctx);
  static uint64_t get_exec_id(ObExecContext* exec_ctx);
  static uint64_t get_session_id(ObExecContext* exec_ctx);
};

// consider compatibility, currently set as not mutually exclusive
class ObPxEstimateSizeUtil {
public:
  static int get_px_size(
      ObExecContext* exec_ctx, const PxOpSizeFactor factor, const int64_t total_size, int64_t& ret_size);
};

class ObSlaveMapItem {
public:
  ObSlaveMapItem()
      : group_id_(0),
        l_worker_count_(0),
        r_worker_count_(0),
        l_partition_id_(OB_INVALID_INDEX_INT64),
        r_partition_id_(OB_INVALID_INDEX_INT64)
  {}
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
  int64_t l_partition_id_;
  int64_t r_partition_id_;
};

#define ENG_OP typename ObEngineOpTraits<NEW_ENG>

class ObPXServerAddrUtil {
  class ObPxSqcTaskCountMeta {
  public:
    ObPxSqcTaskCountMeta() : partition_count_(0), thread_count_(0), time_(0), idx_(0), finish_(false)
    {}
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
  static int alloc_by_data_distribution(ObExecContext& ctx, ObDfo& dfo);

  template <bool NEW_ENG>
  static int alloc_by_data_distribution_inner(ObExecContext& ctx, ObDfo& dfo);

  static int alloc_by_child_distribution(const ObDfo& child, ObDfo& parent);
  static int alloc_by_temp_child_distribution(ObExecContext& ctx, ObDfo& child);
  static int alloc_by_local_distribution(ObExecContext& exec_ctx, ObDfo& root);
  static int alloc_by_reference_child_distribution(ObExecContext& exec_ctx, ObDfo& child, ObDfo& parent);
  static int split_parallel_into_task(const int64_t parallelism, const common::ObIArray<int64_t>& sqc_partition_count,
      common::ObIArray<int64_t>& results);

private:
  static int find_dml_ops(common::ObIArray<const ObTableModify*>& insert_ops, const ObPhyOperator& op);

  static int find_dml_ops(common::ObIArray<const ObTableModifySpec*>& insert_ops, const ObOpSpec& op);
  template <bool NEW_ENG>
  static int find_dml_ops_inner(common::ObIArray<const ENG_OP::TableModify*>& insert_ops, const ENG_OP::Root& op);
  static int check_partition_wise_location_valid(ObPartitionReplicaLocationIArray& tsc_locations);

  static int reorder_all_partitions(int64_t location_key, const ObPartitionReplicaLocationIArray& src_locations,
      ObPartitionReplicaLocationIArray& tsc_locations, bool asc, ObExecContext& exec_ctx,
      ObIArray<int64_t>& base_order);

  static int get_location_addrs(
      const ObPartitionReplicaLocationIArray& locations, common::ObIArray<common::ObAddr>& addrs);
  static int build_dfo_sqc(ObExecContext& ctx, const ObPartitionReplicaLocationIArray& locations, ObDfo& dfo);
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
  template <bool NEW_ENG>
  static int set_dfo_accessed_location(ObExecContext& ctx, ObDfo& dfo, common::ObIArray<const ENG_OP::TSC*>& scan_ops,
      const ENG_OP::TableModify* dml_op);
  /**
   * Add the partition information (table_loc) involved in the
   * current phy_op to the corresponding SQC access location
   */
  template <bool NEW_ENG>
  static int set_sqcs_accessed_location(ObExecContext& ctx, ObDfo& dfo, ObIArray<int64_t>& base_order,
      const ObPhyTableLocation* table_loc, const ENG_OP::Root* phy_op);
  /**
   * Get the access sequence of the partition of the current phy_op,
   * the access sequence of the phy_op partition is determined by
   * the access sequence of the GI of the dfo where it is located:
   * 1. DESC
   * 2. ASC
   * Currently the type of phy_op can only be TSC or INSERT
   */
  template <bool NEW_ENG>
  static int get_access_partition_order(ObDfo& dfo, const ENG_OP::Root* phy_op, bool& asc_order);

  /**
   * Recursively query the corresponding GI operator in the DFO
   * from the phy_op to obtain the corresponding GI access sequence;
   * root represents the root op of the current DFO
   */
  template <bool NEW_ENG>
  static int get_access_partition_order_recursively(
      const ENG_OP::Root* root, const ENG_OP::Root* phy_op, bool& asc_order);

  static int adjust_sqc_task_count(
      common::ObIArray<ObPxSqcTaskCountMeta>& sqc_tasks, int64_t parallel, int64_t partition);
  DISALLOW_COPY_AND_ASSIGN(ObPXServerAddrUtil);
};

class ObPxPartitionLocationUtil {
public:
  /**
   * get all tables' partition info, and store them to sqc_ctx's partition_array_.
   * we need these infos to start trans.
   * IN        tscs
   * IN        tsc_locations
   * OUT       partitions
   */
  static int get_all_tables_partitions(const int64_t& tsc_dml_op_count, ObPartitionReplicaLocationIArray& tsc_locations,
      int64_t& tsc_location_idx, common::ObIArray<common::ObPartitionArray>& partitions, int64_t part_count);
};

class ObPxOperatorVisitor {
public:
  class ApplyFunc {
  public:
    virtual int apply(ObExecContext& ctx, ObPhyOperator& input) = 0;
    virtual int reset(ObPhyOperator& input) = 0;
    virtual int apply(ObExecContext& ctx, ObOpSpec& input) = 0;
    virtual int reset(ObOpSpec& input) = 0;
  };

public:
  static int visit(ObExecContext& ctx, ObPhyOperator& root, ApplyFunc& func);
  static int visit(ObExecContext& ctx, ObOpSpec& root, ApplyFunc& func);
};

class ObPxTreeSerializer {
public:
  static int serialize_tree(
      char* buf, int64_t buf_len, int64_t& pos, ObPhyOperator& root, bool is_fulltree, ObPhyOpSeriCtx* seri_ctx = NULL);
  static int deserialize_tree(const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan,
      ObPhyOperator*& root, bool is_fulltree, ObIArray<const ObTableScan*>& tsc_ops);
  static int deserialize_tree(const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan,
      ObPhyOperator*& root, bool is_fulltree);
  static int deserialize_op(
      const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObPhyOperator*& root);
  static int serialize_sub_plan(char* buf, int64_t buf_len, int64_t& pos, ObPhyOperator& root);
  static int deserialize_sub_plan(
      const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObPhyOperator*& op);

  static int64_t get_sub_plan_serialize_size(ObPhyOperator& root);

  static int64_t get_tree_serialize_size(ObPhyOperator& root, bool is_fulltree, ObPhyOpSeriCtx* seri_ctx = NULL);

  // serialize plan tree for engine3.0
  static int serialize_tree(
      char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& root, bool is_fulltree, ObPhyOpSeriCtx* seri_ctx = NULL);
  static int deserialize_tree(const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan,
      ObOpSpec*& root, ObIArray<const ObTableScanSpec*>& tsc_ops);
  static int deserialize_tree(
      const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObOpSpec*& root);
  static int deserialize_op(const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObOpSpec*& root);
  static int serialize_sub_plan(char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& root);
  static int deserialize_sub_plan(
      const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObOpSpec*& op);
  static int serialize_op_input(
      char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& op_spec, ObOpKitStore& op_kit_store, bool is_fulltree);
  static int deserialize_op_input(const char* buf, int64_t buf_len, int64_t& pos, ObOpKitStore& op_kit_store);
  static int serialize_op_input_tree(char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& op_spec,
      ObOpKitStore& op_kit_store, bool is_fulltree, int32_t& real_input_count);
  static int serialize_op_input_subplan(char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& op_spec,
      ObOpKitStore& op_kit_store, bool is_fulltree, int32_t& real_input_count);
  static int64_t get_serialize_op_input_size(ObOpSpec& op_spec, ObOpKitStore& op_kit_store, bool is_fulltree);
  static int64_t get_serialize_op_input_subplan_size(ObOpSpec& op_spec, ObOpKitStore& op_kit_store, bool is_fulltree);
  static int64_t get_serialize_op_input_tree_size(ObOpSpec& op_spec, ObOpKitStore& op_kit_store, bool is_fulltree);

  static int64_t get_sub_plan_serialize_size(ObOpSpec& root);

  static int64_t get_tree_serialize_size(ObOpSpec& root, bool is_fulltree, ObPhyOpSeriCtx* seri_ctx = NULL);

  static int serialize_expr_frame_info(
      char* buf, int64_t buf_len, int64_t& pos, ObExecContext& ctx, ObExprFrameInfo& expr_frame_info);
  static int serialize_frame_info(char* buf, int64_t buf_len, int64_t& pos, ObIArray<ObFrameInfo>& all_frame,
      char** frames, const int64_t frame_cnt, bool no_ser_data = false);
  static int deserialize_frame_info(const char* buf, int64_t buf_len, int64_t& pos, ObIAllocator& allocator,
      ObIArray<ObFrameInfo>& all_frame, ObIArray<char*>* char_ptrs_, char**& frames, int64_t& frame_cnt,
      bool no_deser_data = false);
  static int deserialize_expr_frame_info(
      const char* buf, int64_t buf_len, int64_t& pos, ObExecContext& ctx, ObExprFrameInfo& expr_frame_info);
  static int64_t get_serialize_frame_info_size(
      ObIArray<ObFrameInfo>& all_frame, char** frames, const int64_t frame_cnt, bool no_ser_data = false);
  static int64_t get_serialize_expr_frame_info_size(ObExecContext& ctx, ObExprFrameInfo& expr_frame_info);
};

class ObPxChannelUtil {
public:
  static int unlink_ch_set(ObPxTaskChSet& ch_set, sql::dtl::ObDtlFlowControl* dfc);
  static int flush_rows(common::ObIArray<dtl::ObDtlChannel*>& channels);

  static int set_transmit_metric(common::ObIArray<sql::dtl::ObDtlChannel*>& channels, sql::ObOpMetric& metric);
  static int set_receive_metric(common::ObIArray<sql::dtl::ObDtlChannel*>& channels, sql::ObOpMetric& metric);

  // asyn wait
  static int dtl_channles_asyn_wait(common::ObIArray<dtl::ObDtlChannel*>& channels, bool ignore_error = false);
  static int sqcs_channles_asyn_wait(common::ObIArray<sql::ObPxSqcMeta*>& sqcs);
};

class ObPxAffinityByRandom {
public:
  struct PartitionHashValue {
    int64_t partition_id_;
    int64_t partition_idx_;
    uint64_t hash_value_;
    int64_t worker_id_;
    ObPxPartitionInfo partition_info_;
    TO_STRING_KV(K_(partition_id), K_(partition_idx), K_(hash_value), K_(worker_id), K_(partition_info));
  };

public:
  ObPxAffinityByRandom() : worker_cnt_(0), partition_hash_values_()
  {}
  virtual ~ObPxAffinityByRandom() = default;
  int add_partition(int64_t partition_id, int64_t partition_idx, int64_t worker_cnt, uint64_t tenant_id,
      ObPxPartitionInfo& partition_row_info);
  int do_random(bool use_partition_info);
  const ObIArray<PartitionHashValue>& get_result()
  {
    return partition_hash_values_;
  }
  static int get_partition_info(
      int64_t partition_id, ObIArray<ObPxPartitionInfo>& partitions_info, ObPxPartitionInfo& partition_info);

private:
  int64_t worker_cnt_;
  ObSEArray<PartitionHashValue, 8> partition_hash_values_;
};

class ObPxAdmissionUtil {
public:
  static int check_parallel_max_servers_value(
      const common::ObIArray<share::ObUnitInfo>& units, const int64_t user_max_servers, int64_t& max_servers);
};

class ObSlaveMapUtil {
public:
  ObSlaveMapUtil() = default;
  ~ObSlaveMapUtil() = default;
  static int build_mn_ch_map(ObExecContext& ctx, ObDfo& child, ObDfo& parent, uint64_t tenant_id);
  static int build_mn_channel(
      ObPxChTotalInfos* dfo_ch_total_infos, ObDfo& child, ObDfo& parent, const uint64_t tenant_id);
  static int build_bf_mn_channel(
      dtl::ObDtlChTotalInfo& transmit_ch_info, ObDfo& child, ObDfo& parent, const uint64_t tenant_id);

private:
  // new channel map generate
  static int build_ppwj_ch_mn_map(ObExecContext& ctx, ObDfo& parent, ObDfo& child, uint64_t tenant_id);
  static int build_pkey_random_ch_mn_map(ObDfo& parent, ObDfo& child, uint64_t tenant_id);
  static int build_ppwj_slave_mn_map(ObDfo& parent, ObDfo& child, uint64_t tenant_id);
  static int build_ppwj_bcast_slave_mn_map(ObDfo& parent, ObDfo& child, uint64_t tenant_id);
  static int build_partition_map_by_sqcs(const common::ObIArray<const ObPxSqcMeta*>& sqcs,
      common::ObIArray<int64_t>& prefix_task_counts, ObPxPartChMapArray& map);
  static int build_pwj_slave_map_mn_group(ObDfo& parent, ObDfo& child, uint64_t tenant_id);
  static int build_mn_channel_per_sqcs(
      ObPxChTotalInfos* dfo_ch_total_infos, ObDfo& child, ObDfo& parent, int64_t sqc_count, uint64_t tenant_id);
  // the following two functions are used in pdml
  static int build_pkey_affinitized_ch_mn_map(ObDfo& parent, ObDfo& child, uint64_t tenant_id);
  static int build_affinitized_partition_map_by_sqcs(const common::ObIArray<const ObPxSqcMeta*>& sqcs,
      ObIArray<int64_t>& prefix_task_counts, int64_t total_task_count, ObPxPartChMapArray& map);
};

class ObDtlChannelUtil {
public:
  static int link_ch_set(
      dtl::ObDtlChSet& ch_set, common::ObIArray<dtl::ObDtlChannel*>& channels, dtl::ObDtlFlowControl* dfc = nullptr);
  static int get_receive_dtl_channel_set(
      const int64_t sqc_id, const int64_t task_id, dtl::ObDtlChTotalInfo& ch_total_info, dtl::ObDtlChSet& ch_set);
  static int get_transmit_dtl_channel_set(
      const int64_t sqc_id, const int64_t task_id, dtl::ObDtlChTotalInfo& ch_total_info, dtl::ObDtlChSet& ch_set);
  static int get_transmit_bf_dtl_channel_set(
      const int64_t sqc_id, dtl::ObDtlChTotalInfo& ch_total_info, dtl::ObDtlChSet& ch_set);
  static int get_receive_bf_dtl_channel_set(
      const int64_t sqc_id, dtl::ObDtlChTotalInfo& ch_total_info, dtl::ObDtlChSet& ch_set);
};

#undef ENG_OP

}  // namespace sql
}  // namespace oceanbase

#endif /* __OCEANBASE_SQL_ENGINE_PX_UTIL_H__ */
//// end of header file
