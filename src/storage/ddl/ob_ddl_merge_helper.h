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

#ifndef OCEANBASE_STORAGE_DDL_MERGE_HELPER_
#define OCEANBASE_STORAGE_DDL_MERGE_HELPER_

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
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/ddl/ob_ss_ddl_util.h"
#endif
namespace oceanbase
{
namespace storage
{
class ObSNDDLMergeHelperV2;
class ObIDDLMergeHelper
{
public:
  static int get_merge_helper(ObIAllocator &allocator,
                              const ObDirectLoadType direct_load_type,
                              ObIDDLMergeHelper *&helper);
  static int get_rec_scn_from_ddl_kvs(ObDDLTabletMergeDagParamV2 &merge_param);
public:
/* interface used for prpare_task*/
  ObIDDLMergeHelper() {}
  virtual ~ObIDDLMergeHelper() {}
  /*
  * process_prepare_task will generate the following task for merge single tablet, actions are follwings
  * 1. check majaor & freeze ddl kv
  * 2. calc slice info, defiene number for merge slice task & assemble task
  * 3. generate merge_slice_task & assemble task
  */
  virtual int check_need_merge(ObIDag *dag,
                               ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                               bool &need_merge)
  {
    int ret = OB_SUCCESS;
    need_merge = true;
    return ret;
  }
  virtual int process_prepare_task(ObIDag *dag,
                                   ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                                   ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices) = 0 ;
  virtual int merge_cg_slice(ObIDag* dag,
                             ObDDLTabletMergeDagParamV2 &merge_param,
                             const int64_t cg_idx,
                             const int64_t start_slice,
                             const int64_t end_slice)
  { return OB_NOT_SUPPORTED; }
  virtual int assemble_sstable(ObDDLTabletMergeDagParamV2 &param)
  { return OB_NOT_SUPPORTED; }
  virtual int freeze_ddl_kv(ObDDLTabletMergeDagParamV2 &param);

  virtual int prepare_ddl_param(const ObDDLTabletMergeDagParamV2 &merge_param,
                                const int64_t cg_idx,
                                ObTabletDDLParam &ddl_param);
  virtual int prepare_ddl_param(const ObDDLTabletMergeDagParamV2 &merge_param,
                                const int64_t cg_idx,
                                const int64_t start_slice_idx,
                                const int64_t end_slice_idx,
                                ObTabletDDLParam &ddl_param);
  virtual int get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param)
  { return OB_NOT_SUPPORTED; }
protected:
  virtual bool is_supported_direct_load_type(const ObDirectLoadType direct_load_type)
  { return OB_NOT_SUPPORTED; }
public:
    TO_STRING_KV(KP(this));
};

class ObSNDDLMergeHelperV2: public ObIDDLMergeHelper
{
public:
  ObSNDDLMergeHelperV2() { };
  virtual ~ObSNDDLMergeHelperV2() {}
  int process_prepare_task(ObIDag *dag,
                           ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                           ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices) override;
  int merge_cg_slice(ObIDag* dag,
                     ObDDLTabletMergeDagParamV2 &merge_param,
                     const int64_t cg_idx,
                     const int64_t start_slice,
                     const int64_t end_slice);
  int assemble_sstable(ObDDLTabletMergeDagParamV2 &param) override;

  int get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param) override;

  // 分区级全量旁路导入为未指定分区补空major
  static int set_ddl_complete_for_direct_load(const share::ObLSID &ls_id,
                                              const ObTabletID &tablet_id,
                                              const ObDirectLoadType direct_load_type,
                                              const int64_t snapshot_version,
                                              const int64_t data_version);

protected:
  bool is_supported_direct_load_type(const ObDirectLoadType direct_load_type) override ;
  int set_ddl_complete(ObIDag *dag, ObTablet &tablet, ObDDLTabletMergeDagParamV2 &ddl_merge_param);
};

#ifdef OB_BUILD_SHARED_STORAGE
class ObSSDDLMergeHelper: public ObIDDLMergeHelper
{
public:
  ObSSDDLMergeHelper(): max_meta_seq_(0), lock_() {}
  virtual ~ObSSDDLMergeHelper() {}
  int process_prepare_task(ObIDag *dag,
                           ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                           ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices) override;
  int merge_cg_slice(ObIDag* dag,
                     ObDDLTabletMergeDagParamV2 &merge_param,
                     const int64_t cg_idx,
                     const int64_t start_slice,
                     const int64_t end_slice);
  int assemble_sstable(ObDDLTabletMergeDagParamV2 &param) override;
  int get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param) override;
public:
  static int build_sstable(
             ObDDLTabletMergeDagParamV2 &dag_merge_param,
             ObTablesHandleArray &co_sstable_array,
             ObSSTable *&major_sstable);
  static int get_meta_store_store(
             ObDDLTabletMergeDagParamV2 &merge_param,
             const ObTabletID tablet_id,
             const int64_t cg_idx,
             ObArray<ObMacroMetaStoreManager::StoreItem> &sorted_meta_stores);

protected:
  int update_tablet_table_store(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                ObTablesHandleArray &table_array,
                                ObSSTable *&major_sstable);
  void update_max_meta_seq(const int64_t seq);
  int64_t get_next_max_meta_seq();
  int merge_dump_sstable(ObDDLTabletMergeDagParamV2 &merge_param);
  int merge_cg_sstable(ObIDag* dag, ObDDLTabletMergeDagParamV2 &merge_param, int64_t cg_idx);
  int update_major_table_store(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                               ObSSTable *&major_sstable);
  bool is_supported_direct_load_type(const ObDirectLoadType direct_load_type) override ;
  int prepare_cg_table_key(const ObDDLTabletMergeDagParamV2 &dag_merge_param,
                           const int64_t cg_idx,
                           const ObStorageSchema *storage_schema,
                           ObITable::TableKey &cur_cg_table_key);
  int write_partial_sstable(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                            ObSSTable *&major_sstable,
                            ObSSTable *&out_sstable);
  int write_ddl_finish_log(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                           ObSSTable *&major_sstable);
private:
  int64_t max_meta_seq_;
  common::ObLatch lock_;
};
#endif
} // namespace storage
} // namespace oceanbase

#endif