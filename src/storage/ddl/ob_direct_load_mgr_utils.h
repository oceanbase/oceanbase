/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_UTILS_H
#define OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_UTILS_H

#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_i_direct_load_mgr.h"
#include "storage/ddl/ob_direct_load_struct.h"
namespace oceanbase
{
namespace storage
{
class ObInsertMonitor;
class ObDirectLoadMgrUtil
{
public:
static bool need_process_vec_index(const ObIndexType & index_type)
{
  return schema::is_local_vec_ivf_centroid_index(index_type)
      || schema::is_vec_ivfsq8_meta_index(index_type)
      || schema::is_vec_ivfpq_pq_centroid_index(index_type)
      || schema::is_vec_index_snapshot_data_type(index_type);
  }
  static int get_tablet_handle(const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  static int create_tablet_direct_load_mgr(const int64_t tenant_id,
                                           const int64_t execution_id,
                                           const int64_t context_id,
                                           const ObTabletDirectLoadInsertParam &build_param,
                                           ObIAllocator &allocator,
                                           bool &is_major_eixst,
                                           ObTabletDirectLoadMgrHandle &data_mgr_handle,
                                           ObTabletDirectLoadMgrHandle &lob_mgr_handle);
  static ObDirectLoadType ddl_get_direct_load_type(const bool is_shared_storage_mode, const uint64_t data_format_version);
  static ObDirectLoadType load_data_get_direct_load_type(const bool is_incremental,
                                                         const uint64_t data_format_version,
                                                         const bool is_shared_storage_mode,
                                                         const bool is_inc_major);
  static int check_major_exist(const ObLSID &ls_id, const ObTabletID &talbet_id, bool is_major_eixst);
  static int generate_merge_param(const ObTabletDDLCompleteArg &arg, ObDDLTableMergeDagParam &merge_param);
  static int generate_merge_param(const ObTabletDDLCompleteMdsUserData &data, ObTablet &tablet, ObDDLTableMergeDagParam &merge_param);
  static int check_cs_replica_exist(const ObLSID &ls_id, const ObTabletID &tablet_id, bool &is_cs_replica_exist);
  static int is_ddl_need_major_merge(const ObTablet &tablet, bool &ddl_need_merging);
  static int alloc_direct_load_mgr(ObIAllocator &allocator, const ObDirectLoadType &direct_load_type, ObBaseTabletDirectLoadMgr *&direct_load_mgr);
  static int prepare_schema_item_for_vec_idx_data(const uint64_t tenant_id,
                                                  ObSchemaGetterGuard &schema_guard,
                                                  const ObTableSchema *table_schema,
                                                  const ObTableSchema *&data_table_schema,
                                                  ObIAllocator &allocator,
                                                  ObTableSchemaItem &schema_item);
protected:
  static int create_idem_tablet_direct_load_mgr(const uint64_t tenant_id,
                                                const int64_t execution_id,
                                                ObIAllocator &allocator,
                                                const ObTabletDirectLoadInsertParam &build_param,
                                                bool &is_major_sstable_exist,
                                                ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
                                                ObTabletDirectLoadMgrHandle &lob_direct_load_mgr_handle);
  static int check_tablet_major_exist(const ObLSID &ls_id, const ObTabletID &tablet_id, bool &is_major_sstable_exist);
  static int get_lob_tablet_id(const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletID &lob_tablet_id);
};
} // namespace storage
} // namespace oceanbaes

#endif //OB_DIRECT_LOAD_MGR_UTILS_H
