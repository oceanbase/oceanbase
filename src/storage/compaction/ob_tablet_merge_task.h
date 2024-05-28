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

#ifndef STORAGE_COMPACTION_OB_TABLET_MERGE_TASK_H_
#define STORAGE_COMPACTION_OB_TABLET_MERGE_TASK_H_

#include "share/ob_occam_time_guard.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_i_table.h"
#include "observer/report/ob_i_meta_report.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/compaction/ob_i_compaction_filter.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/ob_storage_struct.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "share/compaction/ob_compaction_time_guard.h"

namespace oceanbase
{
namespace storage
{
class ObITable;
class ObTablet;
class ObTabletHandle;
struct ObUpdateTableStoreParam;
}

namespace blocksstable
{
class ObSSTable;
}
namespace compaction
{
using namespace storage;
struct ObBasicTabletMergeCtx;
struct ObTabletMergeCtx;
struct ObStaticMergeParam;
class ObPartitionMerger;
struct ObCachedTransStateMgr;
class ObPartitionMergeProgress;
/*
DAG : *PrepareTask -> ObTabletMergeTask* -> ObTabletMergeFinishTask

Mini Compaction      ObTabletMiniMergeDag       ObTabletMiniMergeCtx
Minor Compaction     ObTabletMergeExecuteDag    ObTabletExeMergeCtx
Medium Compaction    ObTabletMajorMergeDag      ObTabletMajorMergeCtx
Tx Table Compaction  ObTxTableMergeDag          ObTabletMiniMergeCtx/ObTabletExeMergeCtx
*/

struct ObMergeParameter {
  ObMergeParameter(
    const ObStaticMergeParam &static_param);
  ~ObMergeParameter() { reset(); }
  bool is_valid() const;
  void reset();
  int init(ObBasicTabletMergeCtx &merge_ctx, const int64_t idx, ObIAllocator *allocator = nullptr);
  const storage::ObTablesHandleArray & get_tables_handle() const;
  const ObStorageSchema *get_schema() const;
  bool is_full_merge() const;

  const ObStaticMergeParam &static_param_;
  /* rest variables are different for MergeTask */
  ObVersionRange merge_version_range_; // modify for different merge_type
  blocksstable::ObDatumRange merge_range_; // rowkey_range
  blocksstable::ObDatumRange merge_rowid_range_;
  ObITableReadInfo *cg_rowkey_read_info_;
  compaction::ObCachedTransStateMgr *trans_state_mgr_;
  share::ObDiagnoseLocation *error_location_;
  share::SCN merge_scn_;
  ObIAllocator *allocator_;

  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  int set_merge_rowid_range(ObIAllocator *allocator);
  DISALLOW_COPY_AND_ASSIGN(ObMergeParameter);
};

// TODO(@DanLing) optimize this struct
struct ObCompactionParam
{
public:
  ObCompactionParam();
  ~ObCompactionParam() = default;
  void estimate_concurrent_count(const compaction::ObMergeType merge_type);
  TO_STRING_KV(K_(score), K_(occupy_size), K_(estimate_phy_size), K_(replay_interval), K_(add_time), K_(last_end_scn),
      K_(sstable_cnt), K_(parallel_dag_cnt), K_(parallel_sstable_cnt), K_(estimate_concurrent_cnt), K_(batch_size));
public:
  int64_t score_; // used for final sort, the lower score, the higher priority.
  uint64_t occupy_size_;
  uint64_t estimate_phy_size_;
  uint64_t replay_interval_;
  uint64_t add_time_;
  share::SCN last_end_scn_;
  uint16_t sstable_cnt_;
  uint16_t parallel_dag_cnt_;
  uint16_t parallel_sstable_cnt_;
  uint16_t estimate_concurrent_cnt_;
  uint16_t batch_size_;
};

struct ObTabletMergeDagParam : public share::ObIDagInitParam
{
  ObTabletMergeDagParam();
  ObTabletMergeDagParam(
    const compaction::ObMergeType merge_type,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t transfer_seq);
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(skip_get_tablet), "merge_type", merge_type_to_str(merge_type_), K_(merge_version),
     K_(ls_id), K_(tablet_id), K_(need_swap_tablet_flag), K_(is_reserve_mode), K_(transfer_seq));

  bool skip_get_tablet_;
  bool need_swap_tablet_flag_;
  bool is_reserve_mode_;
  compaction::ObMergeType merge_type_;
  int64_t merge_version_;
  int64_t transfer_seq_; // only affect minor and major now
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObCompactionParam compaction_param_; // used for adaptive compaction dag scheduling
};

class ObTabletMergePrepareTask: public share::ObITask
{
public:
  ObTabletMergePrepareTask();
  virtual ~ObTabletMergePrepareTask();
  int init();
protected:
  virtual int process() override;
protected:
  bool is_inited_;
  ObTabletMergeDag *merge_dag_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergePrepareTask);
};

class ObTabletMergeFinishTask: public share::ObITask
{
public:
  ObTabletMergeFinishTask();
  virtual ~ObTabletMergeFinishTask();
  int init();
  int report_checkpoint_diagnose_info(ObTabletMergeCtx &ctx);
  virtual int process() override;
private:
  bool is_inited_;
  ObTabletMergeDag *merge_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergeFinishTask);
};

class ObMergeDagHash
{
public:
  ObMergeDagHash()
   : merge_type_(compaction::ObMergeType::INVALID_MERGE_TYPE),
     ls_id_(),
     tablet_id_()
  {}
  virtual ~ObMergeDagHash() {}

  bool is_valid() const
  {
    return merge_type_ > INVALID_MERGE_TYPE && merge_type_ < MERGE_TYPE_MAX
        && ls_id_.is_valid() && tablet_id_.is_valid();
  }

  virtual int64_t inner_hash() const;
  bool belong_to_same_tablet(const ObMergeDagHash *other) const;

  TO_STRING_KV("merge_type", merge_type_to_str(merge_type_), K_(ls_id), K_(tablet_id));

  compaction::ObMergeType merge_type_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};

class ObTabletMergeDag: public share::ObIDag, public ObMergeDagHash
{
public:
  ObTabletMergeDag(const share::ObDagType::ObDagTypeEnum type);
  virtual ~ObTabletMergeDag();
  int init_by_param(const share::ObIDagInitParam *param);
  virtual int create_first_task() override;
  virtual ObBasicTabletMergeCtx *get_ctx() { return ctx_; }
  ObTabletMergeDagParam &get_param() { return param_; }
  const ObTabletMergeDagParam &get_param() const { return param_; }
  virtual const share::ObLSID & get_ls_id() const { return param_.ls_id_; }
  bool is_reserve_mode() const { return param_.is_reserve_mode_; }
  void set_reserve_mode() { param_.is_reserve_mode_ = true; }
  virtual bool operator == (const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual bool ignore_warning() override
  {
    return can_not_retry_warning(dag_ret_);
  }
  virtual int64_t get_data_size() const override { return param_.compaction_param_.occupy_size_; }
  static bool can_not_retry_warning(const int dag_ret) {
    return OB_NO_NEED_MERGE == dag_ret
        || OB_TABLE_IS_DELETED == dag_ret
        || OB_TENANT_HAS_BEEN_DROPPED == dag_ret
        || OB_LS_NOT_EXIST == dag_ret
        || OB_TABLET_NOT_EXIST == dag_ret
        || OB_CANCELED == dag_ret
        || OB_TABLET_TRANSFER_SEQ_NOT_MATCH == dag_ret;
  }
  int get_tablet_and_compat_mode();
  int prepare_merge_ctx(bool &finish_flag); // should be called when the first task of dag starts running
  virtual int64_t to_string(char* buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual int gene_compaction_info(compaction::ObTabletCompactionProgress &progress) override;
  virtual int diagnose_compaction_info(compaction::ObDiagnoseTabletCompProgress &progress) override;
  virtual void set_dag_error_location() override;
  int update_compaction_param(const ObTabletMergeDagParam &param);
  int generate_merge_task(ObBasicTabletMergeCtx &ctx, share::ObITask *prepare_task);
  virtual bool is_ha_dag() const override { return false; }
  int alloc_merge_ctx();
protected:
  int inner_init(const ObTabletMergeDagParam *param);
  int collect_compaction_param(const ObTabletHandle &tablet_handle);
  void fill_compaction_progress(compaction::ObTabletCompactionProgress &progress,
      ObBasicTabletMergeCtx &ctx,
      compaction::ObPartitionMergeProgress *input_progress,
      int64_t start_cg_idx = 0, int64_t end_cg_idx = 0);
  void fill_diagnose_compaction_progress(compaction::ObDiagnoseTabletCompProgress &progress,
      ObBasicTabletMergeCtx *ctx,
      compaction::ObPartitionMergeProgress *input_progress,
      int64_t start_cg_idx = 0, int64_t end_cg_idx = 0);

  bool is_inited_;
  lib::Worker::CompatMode compat_mode_;
  ObBasicTabletMergeCtx *ctx_;
  ObTabletMergeDagParam param_;
  common::ObArenaAllocator allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergeDag);
};

// for MINOR / META_MAJOR
class ObTabletMergeExecuteDag: public ObTabletMergeDag
{
public:
  ObTabletMergeExecuteDag();
  virtual ~ObTabletMergeExecuteDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override; // for diagnose
  int prepare_init(
      const ObTabletMergeDagParam &param,
      const lib::Worker::CompatMode compat_mode,
      const ObGetMergeTablesResult &result,
      storage::ObLSHandle &ls_handle);
  virtual bool operator == (const ObIDag &other) const override;
  const share::ObScnRange& get_merge_range() const { return result_.scn_range_; }
  const ObGetMergeTablesResult& get_result() const { return result_; }
  const ObIArray<ObITable::TableKey> &get_table_key_array() const { return table_key_array_; }

  INHERIT_TO_STRING_KV("ObTabletMergeDag", ObTabletMergeDag, K_(result), K_(table_key_array));
private:
  const static int64_t TABLET_KEY_ARRAY_CNT = 20;
  ObGetMergeTablesResult result_;
  ObSEArray<ObITable::TableKey, TABLET_KEY_ARRAY_CNT> table_key_array_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergeExecuteDag);
};

class ObTabletMergeTask: public share::ObITask
{
public:
  ObTabletMergeTask();
  virtual ~ObTabletMergeTask();
  int init(const int64_t idx, ObBasicTabletMergeCtx &ctx);
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;
private:
  compaction::ObLocalArena allocator_;
  int64_t idx_;
  ObBasicTabletMergeCtx *ctx_;
  ObPartitionMerger *merger_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergeTask);
};

static void prepare_allocator(
    const compaction::ObMergeType &merge_type,
    const bool is_reserve_mode,
    common::ObIAllocator &allocator,
    const bool is_global_mem = true);

#define DEFINE_MERGE_DAG(DAG_NAME, DAG_TYPE)                                   \
  class DAG_NAME : public ObTabletMergeDag {                                   \
  public:                                                                      \
    DAG_NAME() : ObTabletMergeDag(DAG_TYPE) {}                                 \
    virtual ~DAG_NAME() = default;                                             \
  };

DEFINE_MERGE_DAG(ObTabletMajorMergeDag, share::ObDagType::DAG_TYPE_MAJOR_MERGE);

class ObTxTableMergeDag : public ObTabletMergeDag {
public:
  ObTxTableMergeDag()
    : ObTabletMergeDag(share::ObDagType::DAG_TYPE_TX_TABLE_MERGE)
  {}
  virtual ~ObTxTableMergeDag();
};

class ObTabletMiniMergeDag : public ObTabletMergeDag {
public:
  ObTabletMiniMergeDag()
    : ObTabletMergeDag(share::ObDagType::DAG_TYPE_MINI_MERGE)
  {}
  virtual ~ObTabletMiniMergeDag();
};


struct ObTabletSchedulePair
{
public:
  ObTabletSchedulePair()
    : tablet_id_(),
      schedule_merge_scn_(0)
  { }
  ObTabletSchedulePair(
      const common::ObTabletID &tablet_id,
      const int64_t schedule_merge_scn)
    : tablet_id_(tablet_id),
      schedule_merge_scn_(schedule_merge_scn)
  { }
  bool is_valid() const { return tablet_id_.is_valid() && schedule_merge_scn_ > 0; }
  bool need_force_freeze() const { return schedule_merge_scn_ > 0; }
  void reset() { tablet_id_.reset(); schedule_merge_scn_ = 0; }
  TO_STRING_KV(K_(tablet_id), K_(schedule_merge_scn));
public:
  common::ObTabletID tablet_id_;
  int64_t schedule_merge_scn_;
};


struct ObBatchFreezeTabletsParam : public share::ObIDagInitParam
{
public:
  ObBatchFreezeTabletsParam();
  virtual ~ObBatchFreezeTabletsParam() { tablet_pairs_.reset(); }
  virtual bool is_valid() const override { return ls_id_.is_valid() && tablet_pairs_.count() > 0; }
  int assign(const ObBatchFreezeTabletsParam &other);
  bool operator == (const ObBatchFreezeTabletsParam &other) const;
  bool operator != (const ObBatchFreezeTabletsParam &other) const { return !this->operator==(other); }
  int64_t get_hash() const;
  VIRTUAL_TO_STRING_KV(K_(ls_id), "tablet_pair_cnt", tablet_pairs_.count(), K_(tablet_pairs));
public:
  static constexpr int64_t DEFAULT_BATCH_SIZE = 16;
  share::ObLSID ls_id_;
  common::ObSEArray<ObTabletSchedulePair, DEFAULT_BATCH_SIZE> tablet_pairs_;
};


class ObBatchFreezeTabletsDag : public share::ObIDag
{
public:
  ObBatchFreezeTabletsDag();
  virtual ~ObBatchFreezeTabletsDag();
  int init_by_param(const share::ObIDagInitParam *param);
  virtual int create_first_task() override;
  virtual bool operator == (const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  const ObBatchFreezeTabletsParam &get_param() const { return param_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(param));
private:
  bool is_inited_;
  ObBatchFreezeTabletsParam param_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchFreezeTabletsDag);
};


class ObBatchFreezeTabletsTask : public share::ObITask
{
public:
  ObBatchFreezeTabletsTask();
  virtual ~ObBatchFreezeTabletsTask();
  int init();
  virtual int process() override;
private:
  bool is_inited_;
  ObBatchFreezeTabletsDag *base_dag_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchFreezeTabletsTask);
};


} // namespace compaction
} // namespace oceanbase

#endif
