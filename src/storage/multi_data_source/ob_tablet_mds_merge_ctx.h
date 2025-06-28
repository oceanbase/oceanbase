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
#include "storage/incremental/ob_ss_minor_compaction.h"
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

class ObTabletCrossLSMdsMinorMergeCtxHelper
{
public:
  static int get_merge_tables(
      const compaction::ObMergeType merge_type,
      const ObTablesHandleArray &table_handle_array,
      const ObTabletHandle &tablet_handle,
      compaction::ObGetMergeTablesResult &get_merge_table_result);
};

#ifdef OB_BUILD_SHARED_STORAGE
class ObSSTabletCrossLSMdsMinorMergeCtx : public compaction::ObTabletSSMinorMergeCtx
{
public:
  ObSSTabletCrossLSMdsMinorMergeCtx(compaction::ObTabletMergeDagParam &param, common::ObArenaAllocator &allocator);
  virtual ~ObSSTabletCrossLSMdsMinorMergeCtx();
  virtual int get_merge_tables(compaction::ObGetMergeTablesResult &get_merge_table_result) override;
  virtual int update_tablet(ObTabletHandle &new_tablet_handle) override;
  int prepare_merge_tables(const common::ObIArray<ObTableHandleV2> &table_handle_array);
  int build_sstable(ObTableHandleV2 &table_handle, uint64_t &op_id);
protected:
  virtual int prepare_compaction_filter() override;
};
#endif


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MDS_MERGE_CTX
