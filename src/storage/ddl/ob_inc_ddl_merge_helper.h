/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_INC_DDL_MERGE_HELPER_
#define OCEANBASE_STORAGE_INC_DDL_MERGE_HELPER_

#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_ddl_merge_helper.h"
namespace oceanbase
{
namespace storage
{
class ObIncMinDDLMergeHelper: public ObIDDLMergeHelper
{
public:
  int process_prepare_task(ObIDag *dag,
                           ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                           ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices) override;
  int merge_cg_slice(ObIDag* dag,
                     ObDDLTabletMergeDagParamV2 &merge_param,
                     const int64_t cg_idx,
                     const int64_t start_slice,
                     const int64_t end_slice) override;
  int assemble_sstable(ObDDLTabletMergeDagParamV2 &param) override;
  int get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param) override;
private:
  bool is_supported_direct_load_type(const ObDirectLoadType direct_load_type) override
  {
    return ObDirectLoadType::DIRECT_LOAD_INCREMENTAL == direct_load_type;
  }
};

class ObIncMajorDDLMergeHelper: public ObIDDLMergeHelper
{
public:
  ObIncMajorDDLMergeHelper();
  virtual ~ObIncMajorDDLMergeHelper();
  int check_need_merge(ObIDag *dag,
                       ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                       bool &need_merge) override;
  int process_prepare_task(ObIDag *dag,
                           ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                           common::ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices) override;
  int merge_cg_slice(ObIDag* dag,
                     ObDDLTabletMergeDagParamV2 &merge_param,
                     const int64_t cg_idx,
                     const int64_t start_slice,
                     const int64_t end_slice) override;
  int assemble_sstable(ObDDLTabletMergeDagParamV2 &param) override;
  int get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param) override;
protected:
  bool is_supported_direct_load_type(const ObDirectLoadType direct_load_type) override ;
private:
  int calculate_scn_range(const common::ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
                          const common::ObIArray<blocksstable::ObSSTable *> &ddl_sstables,
                          const bool for_major,
                          ObTabletDDLParam &ddl_param);
  int calculate_rec_scn(const common::ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
                        ObTableStoreIterator &ddl_table_iter,
                        share::SCN &rec_scn);
  int check_sstables_empty(const ObDDLTabletMergeDagParamV2 &merge_param,
                           const ObTablesHandleArray &table_array,
                           bool &is_empty);
  int verify_inc_major_sstable(const ObLSID &ls_id,
                               const ObSSTable &inc_major_sstable,
                               ObTabletHandle &tablet_handle);
  int update_tablet_table_store(
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      ObTablesHandleArray &table_array,
      ObSSTable *inc_major_sstable);
};


} // namespace storage
} // namespace oceanbase

#endif
