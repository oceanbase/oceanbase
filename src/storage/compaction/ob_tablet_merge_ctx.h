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
#include "storage/incremental/ob_ls_inc_sstable_uploader.h"
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
  void update_block_info(const ObMergeBlockInfo &block_info, const int64_t cost_time);
  void update_block_info_with_sstable_block_info(const ObMergeBlockInfo &block_info, const int64_t cost_time, ObIArray<ObSSTableMergeBlockInfo> &array);
  INHERIT_TO_STRING_KV("ObBasicTabletMergeCtx", ObBasicTabletMergeCtx, K_(merge_info));
  storage::ObTableHandleV2 merged_table_handle_;
  ObTabletMergeInfo merge_info_;
};

struct ObTabletMiniMergeCtx : public ObTabletMergeCtx
{
  ObTabletMiniMergeCtx(ObTabletMergeDagParam &param, common::ObArenaAllocator &allocator)
    : ObTabletMergeCtx(param, allocator)
#ifdef OB_BUILD_SHARED_STORAGE
    , upload_register_handle_()
#endif
  {}
  virtual ~ObTabletMiniMergeCtx() {}
protected:
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result) override;
  virtual int prepare_schema() override; // update with memtables
private:
  virtual int update_tablet_directly(ObGetMergeTablesResult &get_merge_table_result) override;
  int pre_process_tx_data_table_merge();
  virtual int update_tablet(
    ObTabletHandle &new_tablet_handle) override;
  void try_schedule_compaction_after_mini(storage::ObTabletHandle &tablet_handle);
  void record_uncommitted_sstable_cnt();
  int try_report_tablet_stat_after_mini();
private:
#ifdef OB_BUILD_SHARED_STORAGE
  void register_upload_task_(ObTabletHandle &new_tablet_handle);
  ObSSTableUploadRegHandle upload_register_handle_;
#endif
};

class ObTxDataMinorFilter;
// for minor & meta_major
struct ObTabletExeMergeCtx : public ObTabletMergeCtx
{
  DEFAULT_CONSTRUCTOR(ObTabletExeMergeCtx, ObTabletMergeCtx);
protected:
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result) override;
  virtual int cal_merge_param() override;
  int get_tables_by_key(ObGetMergeTablesResult &get_merge_table_result);
  virtual int prepare_compaction_filter() override; // for tx_minor
private:
  int init_static_param_tx_id();
  int prepare_tx_table_compaction_filter_();
  int prepare_reorg_info_table_compaction_filter_();
  int init_tx_table_compaction_filter_(ObTxDataMinorFilter *compaction_filter);
};

struct ObTabletMajorMergeCtx : public ObTabletMergeCtx
{
  DEFAULT_CONSTRUCTOR(ObTabletMajorMergeCtx, ObTabletMergeCtx);
protected:
  virtual int prepare_schema() override;
  virtual int try_swap_tablet(ObGetMergeTablesResult &get_merge_table_result) override
  { return ObBasicTabletMergeCtx::swap_tablet(get_merge_table_result); }
  virtual int cal_merge_param() override {
    return ObBasicTabletMergeCtx::cal_major_merge_param(
        has_filter() /*force_full_merge*/, progressive_merge_mgr_);
  }
  virtual int prepare_compaction_filter() override
  { return alloc_mds_info_compaction_filter(); }
};
} // namespace compaction
} // namespace oceanbase

#endif
