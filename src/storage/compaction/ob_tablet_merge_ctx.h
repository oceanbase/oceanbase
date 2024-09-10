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

#ifndef STORAGE_COMPACTION_OB_TABLET_MERGE_CTX_H_
#define STORAGE_COMPACTION_OB_TABLET_MERGE_CTX_H_

#include "lib/utility/ob_print_utils.h"
#include "storage/compaction/ob_partition_merge_progress.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/scn.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/compaction/ob_tablet_merge_info.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction/ob_major_task_checkpoint_mgr.h"
#include "storage/shared_storage/prewarm/ob_mc_prewarm_struct.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/compaction/ob_ss_macro_block_validator.h"
#endif

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}

namespace compaction
{
/*
ObBasicTabletMergeCtx
  - ObTabletMergeCtx (Have only one merge_info/merged_table_handle for row store)
      - ObTabletMiniMergeCtx
      - ObTabletExeMergeCtx (For minor/meta_major)
      - ObTabletMajorMergeCtx
  - ObCOTabletMergeCtx (For columnar store)
*/

#define DEFAULT_CONSTRUCTOR(DagName, ParentDag)                                \
  DagName(ObTabletMergeDagParam &param, common::ObArenaAllocator &allocator)   \
      : ParentDag(param, allocator) {}                                         \
  virtual ~DagName() {}
struct ObTabletMergeCtx : public ObBasicTabletMergeCtx
{
public:
  DEFAULT_CONSTRUCTOR(ObTabletMergeCtx, ObBasicTabletMergeCtx);
  ObTabletMergeInfo& get_merge_info() { return merge_info_; }
  virtual int init_tablet_merge_info() override;
  virtual int prepare_index_tree() override;
  virtual void update_and_analyze_progress() override;
  virtual int create_sstable(const blocksstable::ObSSTable *&new_sstable) override;
  virtual int collect_running_info() override;
  const ObSSTableMergeHistory &get_merge_history() { return merge_info_.get_merge_history(); }
  virtual int update_block_info(const ObMergeBlockInfo &block_info) override
  {
    return merge_info_.get_merge_history().update_block_info(block_info);
  }
  INHERIT_TO_STRING_KV("ObBasicTabletMergeCtx", ObBasicTabletMergeCtx, K_(merge_info));
  storage::ObTableHandleV2 merged_table_handle_;
  ObTabletMergeInfo merge_info_;
};

struct ObTabletMiniMergeCtx : public ObTabletMergeCtx
{
  DEFAULT_CONSTRUCTOR(ObTabletMiniMergeCtx, ObTabletMergeCtx);
protected:
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result) override;
  virtual int prepare_schema() override; // update with memtables
private:
  virtual int update_tablet_directly(ObGetMergeTablesResult &get_merge_table_result) override;
  int pre_process_tx_data_table_merge();
  virtual int update_tablet(
    ObTabletHandle &new_tablet_handle) override;
  void try_schedule_compaction_after_mini(storage::ObTabletHandle &tablet_handle);
  int try_schedule_adaptive_merge(ObTabletHandle &tablet_handle, bool &create_meta_dag);
  int try_report_tablet_stat_after_mini();
};

// for minor & meta_major
struct ObTabletExeMergeCtx : public ObTabletMergeCtx
{
  DEFAULT_CONSTRUCTOR(ObTabletExeMergeCtx, ObTabletMergeCtx);
protected:
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result) override;
  virtual int cal_merge_param() override;
  int get_tables_by_key(ObGetMergeTablesResult &get_merge_table_result);
private:
  int prepare_compaction_filter(); // for tx_minor
  int init_static_param_tx_id();
};

struct ObTabletMajorMergeCtx : public ObTabletMergeCtx
{
  DEFAULT_CONSTRUCTOR(ObTabletMajorMergeCtx, ObTabletMergeCtx);
protected:
  virtual int prepare_schema() override;
  virtual int try_swap_tablet(ObGetMergeTablesResult &get_merge_table_result) override
  { return ObBasicTabletMergeCtx::swap_tablet(get_merge_table_result); }
  virtual int cal_merge_param() override {
    return static_param_.cal_major_merge_param(false /*force_full_merge*/,
                                               progressive_merge_mgr_);
  }
};

#ifdef OB_BUILD_SHARED_STORAGE
struct ObSSMergeCtx : public ObTabletMajorMergeCtx
{
  ObSSMergeCtx(ObTabletMergeDagParam &param,
               common::ObArenaAllocator &allocator)
    : ObTabletMajorMergeCtx(param, allocator),
      task_ckp_mgr_()
  {}
  virtual ~ObSSMergeCtx() { destroy(); }
  void destroy();
  int init_major_task_ckp_mgr();
  virtual int generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq) override;
  virtual int get_macro_seq_by_stage(const ObGetMacroSeqStage stage,
                                     int64_t &macro_start_seq) const override;
  int check_exec_mode();
protected:
  ObMajorTaskCheckpointMgr task_ckp_mgr_;
};

struct ObTabletMajorOutputMergeCtx : public ObSSMergeCtx
{
  ObTabletMajorOutputMergeCtx(ObTabletMergeDagParam &param,
                              common::ObArenaAllocator &allocator)
      : ObSSMergeCtx(param, allocator),
        pre_warm_writer_(param.tablet_id_.id(), param.merge_version_),
        major_pre_warm_param_(pre_warm_writer_)
      {}
  virtual ~ObTabletMajorOutputMergeCtx() {}
  virtual int init_tablet_merge_info() override;
  virtual int mark_task_finish(const int64_t task_idx) override;
  virtual int64_t get_start_task_idx() const override;
  virtual bool check_task_finish(const int64_t task_idx) const override;
  virtual const share::ObPreWarmerParam &get_pre_warm_param() const override { return major_pre_warm_param_; }
  virtual int check_medium_info(
    const ObMediumCompactionInfo &next_medium_info,
    const int64_t last_major_snapshot) override;
protected:
  virtual void after_update_tablet_for_major() override;
  virtual int update_tablet(ObTabletHandle &new_tablet_handle) override;

  storage::ObHotTabletInfoWriter pre_warm_writer_;
  ObMajorPreWarmerParam major_pre_warm_param_;
};

struct ObTabletMajorCalcCkmMergeCtx : public ObSSMergeCtx
{
  DEFAULT_CONSTRUCTOR(ObTabletMajorCalcCkmMergeCtx, ObSSMergeCtx);
  virtual int check_medium_info(
    const ObMediumCompactionInfo &next_medium_info,
    const int64_t last_major_snapshot) override;
  virtual int init_tablet_merge_info() override;
protected:
  virtual int update_tablet_after_merge() override;
};

struct ObTabletMajorValidateMergeCtx : public ObSSMergeCtx
{
  ObTabletMajorValidateMergeCtx(ObTabletMergeDagParam &param,
                              common::ObArenaAllocator &allocator)
    : ObSSMergeCtx(param, allocator),
      verify_mgr_()
  {}
  virtual int init_tablet_merge_info() override;
  int alloc_validator(
    const ObMergeParameter &merge_param,
    ObArenaAllocator &allocator,
    ObIMacroBlockValidator *&validator)
  {
    return verify_mgr_.alloc_validator(merge_param, allocator, validator);
  }
protected:
  virtual int update_tablet_after_merge() override;
  ObSSMacroBlockValidatorMgr verify_mgr_;
};
#endif

} // namespace compaction
} // namespace oceanbase

#endif
