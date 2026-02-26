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

#ifndef OCEANBASE_STORAGE_DDL_MERGE_UTILS
#define OCEANBASE_STORAGE_DDL_MERGE_UTILS

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

namespace oceanbase
{
namespace storage
{
class ObDDLMergeTaskUtils
{

/* TODO @zhuoran.zzr
 * these func are both used by merge_task & merge_task_v2
 * wait to remove them when merge_task_v2 ready for all direct load type
*/

/* some utils function which can be used by all helper clas*/
public:
  static int get_slice_indexes(const ObIArray<const ObSSTable *> &ddl_sstables, hash::ObHashSet<int64_t> &slice_idxes);
  static int get_merge_slice_idx(const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs, int64_t &merge_slice_idx);
  static int get_sorted_meta_array(ObTablet &tablet,
                                   const ObTabletDDLParam &ddl_param,
                                   const ObStorageSchema *storage_schema,
                                   const ObIArray<ObSSTable *> &sstables,
                                   const ObITableReadInfo &read_info,
                                   ObIAllocator &allocator,
                                   ObArray<ObDDLBlockMeta> &sorted_metas);

  static int freeze_ddl_kv(const ObLSID &ls_id,
                           const ObTabletID &tablet_id,
                           const ObDirectLoadType &direct_load_type,
                           const share::SCN start_scn,
                           const int64_t snapshot_version,
                           const uint64_t tenant_data_version);

  /*
   * the return value it's  < slice_idx, cg_sstable_array<sstable_handle>> pairs, required cg sstable sorted as cg_idx
  */
  static int get_cg_slice_sstable_from_dag(ObIDag *dag,
                                           const ObLSID &ls_id,
                                           const ObTabletID &tablet_id,
                                           hash::ObHashMap<int64_t, ObArray<ObTableHandleV2>>  *slice_idx_cg_sstables);

  static int check_idempodency(const ObIArray<ObDDLBlockMeta> &input_metas, ObIArray<ObDDLBlockMeta> &output_metas, ObDDLWriteStat *write_stat);

  static int report_ddl_checksum();
  static int get_ddl_memtables(const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
                               ObIArray<const ObSSTable *> &all_ddl_memtables);

  /* some static functional for tablet utils */

  static int prepare_incremental_direct_load_ddl_kvs(ObTablet &tablet, ObIArray<ObDDLKVHandle> &ddl_kvs_handle);

  static int refine_incremental_direct_load_merge_param(const ObTablet &tablet,
                                                        ObITable::TableKey &ddl_param,
                                                        bool &need_check_tablet);
  static int only_update_ddl_checkpoint(ObDDLTabletMergeDagParamV2 &dag_merge_param);
  static int update_storage_schema(ObTablet &tablet,
                                   const ObTabletDDLParam &ddl_param,
                                   ObArenaAllocator &allocator,
                                   ObStorageSchema *&storage_schema,
                                   const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs);
  static int get_ddl_tables_from_ddl_kvs(const ObArray<ObDDLKVHandle> &frozen_ddl_kvs,
                                         const int64_t cg_idx,
                                         const int64_t start_slice_idx,
                                         const int64_t end_slice_idx,
                                         ObIArray<ObSSTable*> &ddl_sstable);
  static int get_ddl_tables_from_dump_tables(const bool for_row_store,
                                             ObTableStoreIterator &ddl_sstable_iter,
                                             const int64_t cg_idx,
                                             const int64_t start_slice_idx,
                                             const int64_t end_slice_idx,
                                             ObIArray<ObSSTable*> &ddl_sstable,
                                             ObIArray<ObStorageMetaHandle> &meta_handles);
  static int update_tablet_table_store(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                       ObTablesHandleArray &table_array,
                                       ObSSTable *&major_sstable);
  static int build_sstable(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                           ObTablesHandleArray &table_array,
                           ObSSTable *&major_sstable);
};
} // namespace storage
} // namespace oceanbase
#endif