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

#ifndef OCEANBASE_STORAGE_INC_DDL_MERGE_TASK_UTILS
#define OCEANBASE_STORAGE_INC_DDL_MERGE_TASK_UTILS

#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"

namespace oceanbase
{
namespace storage
{
struct ObDDLTabletMergeDagParamV2;
class ObIncDDLMergeTaskUtils
{
public:
  // 在clog callback的时候要保证可以冻结成功
  static int prepare_freeze_inc_major_ddl_kv(
      ObTabletHandle &tablet_handle);
  static int freeze_inc_major_ddl_kv(
      const ObTabletHandle &tablet_handle,
      const share::SCN &commit_scn,
      const transaction::ObTransID &trans_id,
      const transaction::ObTxSEQ &seq_no,
      const int64_t snapshot_version,
      const uint64_t data_format_version,
      const bool is_replay);
  static int update_tablet_table_store(
        ObLS *ls,
        ObTabletHandle &tablet_handle);
  static int update_tablet_table_store_with_storage_schema(
      ObLS *ls,
      ObTabletHandle &tablet_handle,
      const ObStorageSchema *storage_schema);
  static int link_inc_major(
      ObLS *ls,
      ObTabletHandle &tablet_handle,
      const transaction::ObTransID &trans_id,
      const transaction::ObTxSEQ &seq_no);

  static int get_all_inc_major_ddl_sstables(
    const ObTablet *tablet,
    ObTableStoreIterator &ddl_table_iter);
  static int get_inc_major_ddl_sstables(
    const transaction::ObTransID &trans_id,
    const transaction::ObTxSEQ &seq_no,
    const ObTablet *tablet,
    ObTableStoreIterator &ddl_table_iter);
  static int get_all_frozen_ddl_kvs(
      ObDDLKvMgrHandle &ddl_kv_mgr_handle,
      common::ObIArray<ObDDLKVHandle> &frozen_ddl_kvs);
  static int get_all_frozen_inc_major_ddl_kvs(
      ObDDLKvMgrHandle &ddl_kv_mgr_handle,
      common::ObIArray<ObDDLKVHandle> &frozen_ddl_kvs);
  static int get_frozen_inc_major_ddl_kvs(
      const transaction::ObTransID &trans_id,
      const transaction::ObTxSEQ &seq_no,
      ObDDLKvMgrHandle &ddl_kv_mgr_handle,
      common::ObIArray<ObDDLKVHandle> &frozen_ddl_kvs);
  static int get_all_inc_major_ddl_kvs(
      const ObDDLKvMgrHandle &ddl_kv_mgr_handle,
      common::ObIArray<ObDDLKVHandle> &inc_major_ddl_kvs);
  static int get_all_inc_major_ddl_kvs(
      const ObTablet *tablet,
      common::ObIArray<ObDDLKVHandle> &inc_major_ddl_kvs);
  static int check_unfinished_inc_major_before_merge(
      ObLS *ls,
      const ObTablet *tablet,
      const int64_t merge_snapshot_version,
      bool &exists_unfinished_inc_major);
  static int close_ddl_kvs(ObIArray<ObDDLKVHandle> &ddl_kvs);
  static int close_ddl_kv(const ObDDLKVHandle &ddl_kv_handle);
  static int calculate_tx_data_recycle_scn(
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    bool &contain_uncommitted_row,
    share::SCN &recycle_end_scn);
  static int check_inc_major_write_stat(
    const ObTabletHandle &tablet_handle,
    const ObDDLTabletMergeDagParamV2 &merge_param,
    const int64_t cg_idx,
    const ObDDLWriteStat &write_stat);
#ifdef OB_BUILD_SHARED_STORAGE
  static int gc_ss_inc_major_ddl_dump(const ObIArray<std::pair<share::ObLSID, ObTabletID>> &ls_tablet_ids);
#endif
private:
  static int calculate_recycle_scn_from_inc_major(
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    bool &contain_uncommitted_row,
    share::SCN &inc_recycle_end_scn);
  static int calculate_recycle_scn_from_inc_ddl_dump(
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    bool &contain_uncommitted_row,
    share::SCN &inc_recycle_end_scn);
  static int calculate_recycle_scn_from_inc_ddl_kv(
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    bool &contain_uncommitted_row,
    share::SCN &inc_recycle_end_scn);
};
} // namespace storage
} // namespace oceanbase
#endif