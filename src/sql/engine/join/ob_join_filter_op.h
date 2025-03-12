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

#ifndef _SQL_ENGINE_JOIN_OB_JOIN_FILTER_OP_H
#define _SQL_ENGINE_JOIN_OB_JOIN_FILTER_OP_H 1

#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_se_array.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_op_metric.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_msg.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_vec_msg.h"
#include "share/detect/ob_detectable_id.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_query_range.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/join/ob_join_filter_partition_splitter.h"
#include "sql/engine/join/ob_join_filter_store_row.h"
#include "sql/engine/join/ob_join_filter_material_control_info.h"
#include "sql/engine/px/datahub/components/ob_dh_join_filter_count_row.h"
#include "deps/oblib/src/lib/utility/ob_hyperloglog.h"


namespace oceanbase
{
namespace sql
{

class ObPxSQCProxy;
class ObJoinFilterOp;

class SharedJoinFilterConstructor
{
public:
  inline bool try_acquire_constructor() { return !ATOMIC_CAS(&is_acquired_, false, true); }
  inline bool try_release_constructor() { return ATOMIC_CAS(&is_acquired_, true, false); }
  int init();
  int reset_for_rescan();
  int wait_constructed(ObOperator *join_filter_op, ObRFBloomFilterMsg *bf_msg);
  int notify_constructed();
private:
  static constexpr uint64_t COND_WAIT_TIME_USEC = 100; // 100 us
  ObThreadCond cond_;
  bool is_acquired_{false};
  bool is_bloom_filter_constructed_{false};
} CACHE_ALIGNED;

struct ObJoinFilterShareInfo
{
  ObJoinFilterShareInfo()
      : unfinished_count_ptr_(0), ch_provider_ptr_(0), release_ref_ptr_(0), filter_ptr_(0),
        shared_msgs_(0), shared_jf_constructor_(nullptr)
  {}
  uint64_t unfinished_count_ptr_; // send_filter引用计数, 初始值为worker个数
  uint64_t ch_provider_ptr_; // sqc_proxy, 由于序列化需要, 使用指针表示.
  uint64_t release_ref_ptr_; // 释放内存引用计数, 初始值为worker个数.
  uint64_t filter_ptr_;   //此指针将作为PX JOIN FILTER CREATE算子共享内存.
  uint64_t shared_msgs_;  //sqc-shared dh msgs
  union {
    SharedJoinFilterConstructor *shared_jf_constructor_;
    uint64_t ser_shared_jf_constructor_;
  };
  OB_UNIS_VERSION_V(1);
public:
  TO_STRING_KV(KP(unfinished_count_ptr_), KP(ch_provider_ptr_), KP(release_ref_ptr_), KP(filter_ptr_), K(shared_msgs_));
};

struct ObJoinFilterRuntimeConfig
{
  OB_UNIS_VERSION_V(1);
public:
  TO_STRING_KV(K_(bloom_filter_ratio), K_(each_group_size), K_(bf_piece_size),
               K_(runtime_filter_wait_time_ms), K_(runtime_filter_max_in_num),
               K_(runtime_bloom_filter_max_size), K_(px_message_compression));
public:
  ObJoinFilterRuntimeConfig() :
      bloom_filter_ratio_(0.0),
      each_group_size_(OB_INVALID_ID),
      bf_piece_size_(0),
      runtime_filter_wait_time_ms_(0),
      runtime_filter_max_in_num_(0),
      runtime_bloom_filter_max_size_(0),
      px_message_compression_(false),
      build_send_opt_{false} {}
  double bloom_filter_ratio_;
  int64_t each_group_size_;
  int64_t bf_piece_size_; // how many int64_t a piece bloom filter contains
  int64_t runtime_filter_wait_time_ms_;
  int64_t runtime_filter_max_in_num_;
  int64_t runtime_bloom_filter_max_size_;
  bool px_message_compression_;
  bool build_send_opt_;
};

class ObJoinFilterOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObJoinFilterOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec),
      share_info_(),
      task_id_(0),
      px_sequence_id_(OB_INVALID_ID),
      bf_idx_at_sqc_proxy_(-1),
      config_(),
      register_dm_info_()
  {}
  virtual ~ObJoinFilterOpInput() {}

  virtual int init(ObTaskInfo &task_info);
  virtual void reset() override
  {
    auto &ctx = exec_ctx_;
    auto &spec = spec_;
    void *ptr = this;
    this->~ObJoinFilterOpInput();
    new (ptr) ObJoinFilterOpInput(ctx, spec);
  }
  bool is_finish();
  bool check_release();
  // 每个worker共享同一块sqc_proxy
  void set_sqc_proxy(ObPxSQCProxy &sqc_proxy)
  {
    share_info_.ch_provider_ptr_ = reinterpret_cast<uint64_t>(&sqc_proxy);
  }
  ObJoinFilterOp *get_filter()
  {
    return reinterpret_cast<ObJoinFilterOp *>(share_info_.filter_ptr_);
  }
  int init_share_info(
      const ObJoinFilterSpec &spec,
      ObExecContext &ctx,
      int64_t task_count,
      int64_t sqc_count);
  int init_shared_msgs(const ObJoinFilterSpec &spec,
      ObExecContext &ctx,
      int64_t sqc_count);
  static int construct_msg_details(const ObJoinFilterSpec &spec,
      ObPxSQCProxy *sqc_proxy,
      ObJoinFilterRuntimeConfig &config,
      ObP2PDatahubMsgBase &msg, int64_t sqc_count, int64_t estimated_rows);
  void set_task_id(int64_t task_id)  { task_id_ = task_id; }

  inline void set_bf_idx_at_sqc_proxy(int64_t idx) { bf_idx_at_sqc_proxy_ = idx; }

  inline int64_t get_bf_idx_at_sqc_proxy() { return bf_idx_at_sqc_proxy_; }
  void set_px_sequence_id(int64_t id) { px_sequence_id_ = id; }
  int64_t get_px_sequence_id() { return px_sequence_id_; }
  int load_runtime_config(const ObJoinFilterSpec &spec, ObExecContext &ctx);
  void init_register_dm_info(const ObDetectableId &id, const common::ObAddr &addr)
  {
    register_dm_info_.detectable_id_ = id;
    register_dm_info_.addr_ = addr;
  }
public:
  ObJoinFilterShareInfo share_info_; //bloom filter共享内存
  int64_t task_id_; //在pwj join场景中会用到此task_id作为bf_key
  int64_t px_sequence_id_;
  int64_t bf_idx_at_sqc_proxy_;
  ObJoinFilterRuntimeConfig config_;
  common::ObRegisterDmInfo register_dm_info_;
  DISALLOW_COPY_AND_ASSIGN(ObJoinFilterOpInput);
};

struct ObRuntimeFilterInfo
{
  OB_UNIS_VERSION_V(1);
public:
  TO_STRING_KV(K_(filter_expr_id), K_(p2p_datahub_id), K_(filter_shared_type));
public:
  ObRuntimeFilterInfo() :
      filter_expr_id_(OB_INVALID_ID),
      p2p_datahub_id_(OB_INVALID_ID),
      filter_shared_type_(INVALID_TYPE),
      dh_msg_type_(ObP2PDatahubMsgBase::ObP2PDatahubMsgType::NOT_INIT)
      {}
  virtual ~ObRuntimeFilterInfo() = default;
  void reset () {
    filter_expr_id_ = OB_INVALID_ID;
    p2p_datahub_id_ = OB_INVALID_ID;
    dh_msg_type_ = ObP2PDatahubMsgBase::ObP2PDatahubMsgType::NOT_INIT;
  }
  int64_t filter_expr_id_;
  int64_t p2p_datahub_id_;
  JoinFilterSharedType filter_shared_type_;
  ObP2PDatahubMsgBase::ObP2PDatahubMsgType dh_msg_type_;
};

class ObJoinFilterSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(2);
public:
  ObJoinFilterSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(mode), K_(filter_id), K_(filter_len), K_(rf_infos),
                       K_(bloom_filter_ratio), K_(send_bloom_filter_size));

  inline void set_mode(JoinFilterMode mode) { mode_ = mode; }
  inline JoinFilterMode get_mode() const { return mode_; }
  inline void set_filter_id(int64_t id) { filter_id_ = id; }
  inline int64_t get_filter_id() const { return filter_id_; }
  inline void set_filter_length(int64_t len) { filter_len_ = len; }
  inline int64_t get_filter_length() const { return filter_len_; }
  inline ObIArray<ObExpr*> &get_exprs() { return join_keys_; }
  inline bool is_create_mode() const { return JoinFilterMode::CREATE == mode_; }
  inline bool is_use_mode() const { return JoinFilterMode::USE == mode_; }
  inline bool is_partition_filter() const
  { return filter_shared_type_ == JoinFilterSharedType::NONSHARED_PARTITION_JOIN_FILTER ||
           filter_shared_type_ == JoinFilterSharedType::SHARED_PARTITION_JOIN_FILTER; };
  inline void set_shared_filter_type(JoinFilterSharedType type) { filter_shared_type_ = type; }
  inline bool is_shared_join_filter() const
  { return filter_shared_type_ == JoinFilterSharedType::SHARED_JOIN_FILTER ||
           filter_shared_type_ == JoinFilterSharedType::SHARED_PARTITION_JOIN_FILTER; }

  int register_to_datahub(ObExecContext &ctx) const;
  inline bool use_realistic_runtime_bloom_filter_size() const
  {
    return jf_material_control_info_.enable_material_;
  }

  inline bool is_material_controller() const
  {
    return use_realistic_runtime_bloom_filter_size() && jf_material_control_info_.is_controller_;
  }

  inline int16_t under_control_join_filter_count() const
  {
    return jf_material_control_info_.join_filter_count_;
  }

  inline bool can_reuse_hash_join_hash_value() const
  {
    return jf_material_control_info_.hash_id_ == 0;
  }

  inline bool need_sync_row_count() const {
    return is_material_controller() && jf_material_control_info_.need_sync_row_count_;
  }

  inline bool use_ndv_runtime_bloom_filter_size() const
  {
    return use_ndv_runtime_bloom_filter_size_;
  }

  int update_sync_row_count_flag();

  JoinFilterMode mode_;
  int64_t filter_id_;
  int64_t filter_len_;
  ExprFixedArray join_keys_;
  common::ObHashFuncs hash_funcs_;
  ObCmpFuncs cmp_funcs_;
  JoinFilterSharedType filter_shared_type_;
  ObExpr *calc_tablet_id_expr_;
  common::ObFixedArray<ObRuntimeFilterInfo, common::ObIAllocator> rf_infos_;
  common::ObFixedArray<bool, common::ObIAllocator> need_null_cmp_flags_;
  bool is_shuffle_;
  int64_t each_group_size_;
  common::ObFixedArray<ObRFCmpInfo, common::ObIAllocator> rf_build_cmp_infos_;
  common::ObFixedArray<ObRFCmpInfo, common::ObIAllocator> rf_probe_cmp_infos_;
  ObPxQueryRangeInfo px_query_range_info_;
  int64_t bloom_filter_ratio_;
  int64_t send_bloom_filter_size_; // how many KB a piece bloom filter has
  ObJoinFilterMaterialControlInfo jf_material_control_info_;
  ObJoinType join_type_ {UNKNOWN_JOIN};
  ExprFixedArray full_hash_join_keys_;
  common::ObFixedArray<bool, common::ObIAllocator> hash_join_is_ns_equal_cond_;
  int64_t rf_max_wait_time_ms_{0};
  bool use_ndv_runtime_bloom_filter_size_{false}; //whether use ndv size build bloom filter
};

class ObJoinFilterMaterialGroupController
{
public:
  ObJoinFilterMaterialGroupController(uint16_t group_count, uint64_t extra_hash_count,
                                      common::ObIAllocator &alloc)
      : group_count_(group_count), extra_hash_count_(extra_hash_count), join_filter_ops_(alloc)
  {}
  template<typename FUNC, typename... ARGS>
  int apply(FUNC func, ARGS&&... args) {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < join_filter_ops_.count() && OB_SUCC(ret); ++i) {
      ObJoinFilterOp *join_filter_op = join_filter_ops_.at(i);
      if (OB_FAIL(func(join_filter_op, std::forward<ARGS>(args)...))) {
        SQL_LOG(WARN, "failed to do op");
      }
    }
    return ret;
  }
public:
  uint16_t group_count_{0};
  uint64_t extra_hash_count_{0};
  common::ObFixedArray<ObJoinFilterOp *, common::ObIAllocator> join_filter_ops_;
  uint64_t **group_join_filter_hash_values_{nullptr};
  /* map from group_id to hash id,
    e.g
                      Hash Join
                        /
                Join Filter Create (group id = 0, hash id = 1)
                      /
            Join Filter Create (group id = 1, hash id = 0)
                   /
        Join Filter Create (group id = 2, hash id = 2)
  */
  uint16_t *hash_id_map_{nullptr};
  ObHyperLogLogCalculator* hash_join_keys_hllc_{nullptr};
};

class ObJoinFilterOp : public ObOperator
{
  struct ObJoinFilterMsg {
    ObJoinFilterMsg() : bf_msg_(nullptr), range_msg_(nullptr), in_msg_(nullptr) {}
    ObRFBloomFilterMsg *bf_msg_;
    ObRFRangeFilterMsg *range_msg_;
    ObRFInFilterMsg *in_msg_;
  };
public:
  TO_STRING_KV(K(force_dump_));
public:
  ObJoinFilterOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObJoinFilterOp();

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override; // for batch
  virtual int inner_drain_exch() override;
  int do_drain_exch() override;
  virtual void destroy() override {
    lucky_devil_champions_.reset();
    local_rf_msgs_.reset();
    shared_rf_msgs_.reset();
    row_meta_.reset();
    sql_mem_processor_.unregister_profile_if_necessary();
    if (OB_NOT_NULL(partition_splitter_)){
      partition_splitter_->~ObJoinFilterPartitionSplitter();
      partition_splitter_ = nullptr;
    }
    if (OB_LIKELY(NULL != mem_context_)) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = NULL;
    }
    ObOperator::destroy();
  }
  static int link_ch_sets(ObPxBloomFilterChSets &ch_sets,
                          common::ObIArray<dtl::ObDtlChannel *> &channels);
  ObJoinFilterPartitionSplitter *get_partition_splitter() { return partition_splitter_; }
private:
  bool is_valid();
  int destroy_filter();
  int insert_by_row();
  int insert_by_row_batch(const ObBatchRows *child_brs);
  int calc_expr_values(ObDatum *&datum);
  int do_create_filter_rescan();
  int do_use_filter_rescan();
  int try_send_join_filter();
  int try_merge_join_filter();
  int calc_each_bf_group_size(int64_t &);
  int update_plan_monitor_info();
  int prepre_bloom_filter_ctx(ObBloomFilterSendCtx *bf_ctx);
  int open_join_filter_create();
  int open_join_filter_use();
  int join_filter_create_get_next_batch(const int64_t max_row_cnt);
  int join_filter_create_do_material(const int64_t max_row_cnt);
  int join_filter_create_bypass_all(const int64_t max_row_cnt);

  int join_filter_use_get_next_batch(const int64_t max_row_cnt);
  int close_join_filter_create();
  int close_join_filter_use();
  int init_shared_msgs_from_input();
  int init_local_msg_from_shared_msg(ObP2PDatahubMsgBase &msg);
  int release_local_msg();
  int release_shared_msg();
  int mark_not_need_send_bf_msg();
  int prepare_extra_use_info_for_vec20(ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx,
                                   ObP2PDatahubMsgBase::ObP2PDatahubMsgType dh_msg_type);

  int init_material_parameters();
  int init_material_group_exec_info();
  int process_dump();
  inline bool need_dump() const
  {
    return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound();
  }
  int calc_join_filter_hash_values(const ObBatchRows &brs);
  uint64_t *get_join_filter_hash_values() { return join_filter_hash_values_; }
  void read_join_filter_hash_values_from_store(const ObBatchRows &brs,
                                               const ObJoinFilterStoreRow **store_rows,
                                               const RowMeta &row_meta,
                                               uint64_t *join_filter_hash_values);

  int get_exec_row_count_and_ndv(const int64_t worker_row_count, int64_t &total_row_count, bool is_in_drain);
  bool can_sync_row_count_locally();
  int send_datahub_count_row_msg(int64_t &total_row_count, ObTMArray<ObJoinFilterNdv *> &ndv_info,
                                 bool need_wait_whole_msg);

  int fill_range_filter(const ObBatchRows &brs);
  int fill_in_filter(const ObBatchRows &brs, uint64_t *hash_join_hash_values);
  int build_ndv_info_before_aggregate(ObTMArray<ObJoinFilterNdv *> &ndv_info);
  void check_in_filter_active(int64_t &in_filter_ndv);
  int init_bloom_filter(const int64_t worker_row_count, const int64_t total_row_count);
  int fill_bloom_filter();

  inline bool build_send_opt() {
    return MY_INPUT.config_.build_send_opt_;
  }

  inline bool skip_fill_bloom_filter() {
    return build_send_opt() && in_filter_active_;
  }

  inline bool use_hllc_estimate_ndv()
  {
    return build_send_opt() && get_my_spec(*this).use_ndv_runtime_bloom_filter_size();
  }

  static inline int group_fill_range_filter(ObJoinFilterOp *join_filter_op,
                                            const ObBatchRows &brs)
  {
    return join_filter_op->fill_range_filter(brs);
  }
  static inline int group_calc_join_filter_hash_values(ObJoinFilterOp *join_filter_op,
                                                       const ObBatchRows &brs)
  {
    return join_filter_op->calc_join_filter_hash_values(brs);
  }
  static inline int group_fill_in_filter(ObJoinFilterOp *join_filter_op,
                                         const ObBatchRows &brs,
                                         uint64_t *hash_join_hash_values)
  {
    return join_filter_op->fill_in_filter(brs, hash_join_hash_values);
  }

  static int group_build_ndv_info_before_aggregate(ObJoinFilterOp *join_filter_op,
                                                   ObTMArray<ObJoinFilterNdv *> &ndv_info)
  {
    return join_filter_op->build_ndv_info_before_aggregate(ndv_info);
  }

  static int group_collect_worker_ndv_by_hllc(ObJoinFilterOp *join_filter_op)
  {
    join_filter_op->worker_ndv_ = join_filter_op->hllc_->estimate();
    return OB_SUCCESS;
  }

  static int group_init_bloom_filter(ObJoinFilterOp *join_filter_op,
                                     int64_t worker_row_count,
                                     int64_t total_row_count);

  static int group_fill_bloom_filter(ObJoinFilterOp *join_filter_op,
                                     const ObBatchRows &brs_from_controller,
                                     const ObJoinFilterStoreRow **part_stored_rows,
                                     const RowMeta &row_meta);
  static int group_merge_and_send_join_filter(ObJoinFilterOp *join_filter_op);

private:
  static const int64_t ADAPTIVE_BF_WINDOW_ORG_SIZE = 4096;
  static constexpr double ACCEPTABLE_FILTER_RATE = 0.98;
  static const int64_t N_HYPERLOGLOG_BIT = 14;
public:
  ObJoinFilterMsg *filter_create_msg_;
  ObArray<ObP2PDatahubMsgBase *> shared_rf_msgs_; // sqc level share
  ObArray<ObP2PDatahubMsgBase *> local_rf_msgs_;
  uint64_t *join_filter_hash_values_;
  ObArray<bool> lucky_devil_champions_;

  // only for vectorize 2.0 and join filter material
  ObRFInFilterVecMsg *in_vec_msg_{nullptr};
  ObRFRangeFilterVecMsg *range_vec_msg_{nullptr};
  ObRFBloomFilterMsg *bf_vec_msg_{nullptr};

  // only the controller has the right to use these variables
  bool skip_left_null_{false};
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  lib::MemoryContext mem_context_{nullptr};
  ObJoinFilterPartitionSplitter *partition_splitter_{nullptr};
  RowMeta row_meta_;
  const ObJoinFilterStoreRow **part_stored_rows_{nullptr};
  uint64_t *hash_join_hash_values_{nullptr};

  ObJoinFilterMaterialGroupController *group_controller_{nullptr};
  bool force_dump_{false};
  bool has_sync_row_count_{false};
  const ExprFixedArray *build_rows_output_{nullptr};
  ExprFixedArray build_rows_output_for_compat_;

  // for build count opt
  bool in_filter_active_{false};
  ObJoinFilterNdv dh_ndv_;
  // build count opt end

  //Considering compatibility, >= 435 BP1 will use the variables below to estimate NDV.
  //For each join filter will use this hllc
  //If this join filter can *reuse* hash join keys, this hllc_ will point to group_controller_.hash_join_keys_hllc_
  ObHyperLogLogCalculator* hllc_{nullptr};
  int64_t worker_ndv_{0}; // ndv of each thread, used when this is a non-shared join filter
  int64_t total_ndv_{0};  // ndv of total dfo, used when this is a shared join filter
};

};

}


#endif /* _SQL_ENGINE_JOIN_OB_JOIN_FILTER_OP_H */


