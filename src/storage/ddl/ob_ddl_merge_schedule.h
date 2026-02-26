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

#ifndef OCEANBASE_STORAGE_DDL_MERGE_SCHEDULE_
#define OCEANBASE_STORAGE_DDL_MERGE_SCHEDULE_

#include "storage/tablet/ob_tablet.h"
#include "storage/ddl/ob_ddl_merge_task.h"

#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_direct_load_struct.h"


namespace oceanbase
{
namespace transaction
{
 class ObTransID;
 class ObTxSEQ;
}
namespace storage
{
class ObLSHandle;
class ObTabletHandle;
class ObDDLMergeScheduler
{
public:
  static int freeze_ddl_kv(const share::SCN &rec_scn, ObLS *ls, ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  static int schedule_ddl_merge(ObLS *ls, ObTabletHandle &tablet_handle);
  static int schedule_ddl_merge(ObLS *ls, ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  static int schedule_ddl_merge(ObLS *ls, ObTabletHandle &tablet_handle, ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  #ifdef OB_BUILD_SHARED_STORAGE
  static int schedule_ddl_minor_merge_on_demand(const bool need_freeze,
                                                const share::ObLSID &ls_id,
                                                ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  static int schedule_ss_gc_inc_major_ddl_dump(ObLS *ls, ObTabletHandle &tablet_handle);
  static int schedule_ss_update_inc_major_and_gc_inc_major(ObLS *ls, const ObTabletHandle &tablet_handle);
  static int finish_log_freeze_ddl_kv(const ObLSID &ls_id, ObTabletHandle &tablet_handle);
  #endif
  static int schedule_tablet_ddl_major_merge(ObLS *ls, ObTabletHandle &tablet_handle);
  static int schedule_tablet_ddl_inc_major_merge(ObLS *ls, ObTabletHandle &tablet_handle);
  static int schedule_tablet_ddl_inc_major_merge(ObLSHandle &ls_handle, ObTabletHandle &tablet_handle);
  static int schedule_tablet_ddl_inc_major_merge(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id);

private:
  /* check need merge */
  static int check_tablet_need_merge(ObTablet &tablet, ObDDLKvMgrHandle &ddl_kv_mgr_handle, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type);
  static int check_need_merge_for_nidem_sn(ObTablet &tablet, ObArray<ObDDLKVHandle> &ddl_kvs, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type);
  static int check_need_merge_for_idem_sn(ObTablet &tablet, ObArray<ObDDLKVHandle> &ddl_kvs, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type);
  static int check_need_merge_for_inc_major(ObTablet &tablet, ObArray<ObDDLKVHandle> &ddl_kvs, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type);

#ifdef OB_BUILD_SHARED_STORAGE
  static int check_need_merge_for_ss(ObTablet &tablet, ObArray<ObDDLKVHandle> &ddl_kvs, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type);

  static int schedule_task_if_split_src(ObTabletHandle &tablet_handle);
#endif

  static int check_ddl_kv_dump_delay(ObDDLKV &ddl_kv);
  static int check_inc_major_merge_delay(
      const ObTabletHandle &tablet_handle,
      const transaction::ObTransID &cur_trans_id,
      const transaction::ObTxSEQ &cur_seq_no,
      const share::SCN &trans_version);
  static int schedule_tablet_ddl_inc_major_merge_for_sn(ObLS *ls, ObTabletHandle &tablet_handle);
#ifdef OB_BUILD_SHARED_STORAGE
  static int schedule_tablet_ddl_inc_major_merge_for_ss(ObLS *ls, ObTabletHandle &tablet_handle);
#endif

private:
  static const int64_t PRINT_LOG_INTERVAL = 2 * 60 * 1000 * 1000L; // 2m
};

} // namespace storage
} // namespace oceanbase

#endif
