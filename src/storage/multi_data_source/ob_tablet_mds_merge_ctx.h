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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MDS_MERGE_CTX
#define OCEANBASE_STORAGE_OB_TABLET_MDS_MERGE_CTX

#include "storage/compaction/ob_tablet_merge_ctx.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/atomic_protocol/ob_atomic_file_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file_handle.h"
#include "storage/incremental/atomic_protocol/ob_atomic_sstablelist_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_sstablelist_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_op_handle.h"
#endif

namespace oceanbase
{
namespace common
{
class ObArenaAllocator;
}

namespace compaction
{
struct ObTabletMergeDagParam;
}

namespace storage
{
class ObTabletMdsMinorMergeCtx : public compaction::ObTabletExeMergeCtx
{
public:
  ObTabletMdsMinorMergeCtx(compaction::ObTabletMergeDagParam &param, common::ObArenaAllocator &allocator);
  virtual ~ObTabletMdsMinorMergeCtx() { free_schema(); }
  static int prepare_compaction_filter(ObIAllocator &allocator, ObTablet &tablet, compaction::ObICompactionFilter *&filter);
protected:
  virtual int prepare_schema() override;
  virtual int prepare_index_tree() override;
  virtual void free_schema() override;
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result) override;
  virtual int update_tablet(ObTabletHandle &new_tablet_handle) override;
};

class ObTabletCrossLSMdsMinorMergeCtx : public compaction::ObTabletMergeCtx
{
public:
  ObTabletCrossLSMdsMinorMergeCtx(compaction::ObTabletMergeDagParam &param, common::ObArenaAllocator &allocator);
  virtual ~ObTabletCrossLSMdsMinorMergeCtx() { free_schema(); }
public:
  virtual int prepare_schema() override;
  virtual int prepare_index_tree() override;
  virtual void free_schema() override;
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result) override;
  virtual int update_tablet(ObTabletHandle &new_tablet_handle) override;
  int prepare_merge_tables(const common::ObIArray<ObTableHandleV2> &table_handle_array);
private:
  int prepare_compaction_filter();

};

#ifdef OB_BUILD_SHARED_STORAGE
class ObSSTabletCrossLSMdsMinorMergeCtx : public ObTabletCrossLSMdsMinorMergeCtx
{
public:
  ObSSTabletCrossLSMdsMinorMergeCtx(compaction::ObTabletMergeDagParam &param, common::ObArenaAllocator &allocator);
  virtual ~ObSSTabletCrossLSMdsMinorMergeCtx();
  int init_tablet_merge_info();
  int generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq) override;
  int get_macro_seq_by_stage(const compaction::ObGetMacroSeqStage stage, int64_t &macro_start_seq) const override;
  int build_sstable(ObTableHandleV2 &table_handle, uint64_t &op_id);

private:
  int start_add_minor_op_();
  int finish_add_minor_op_();
  int fail_add_minor_op_();
private:
  mutable int64_t macro_seq_max_step_;
  ObAtomicFileHandle<ObAtomicSSTableListFile> minor_sstable_list_handle_;
  ObAtomicOpHandle<ObAtomicSSTableListAddOp> add_minor_op_handle_;
};
#endif

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MDS_MERGE_CTX
