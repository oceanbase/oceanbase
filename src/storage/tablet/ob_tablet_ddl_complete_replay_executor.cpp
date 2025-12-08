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

#include "storage/tablet/ob_tablet_ddl_complete_replay_executor.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/tx_storage/ob_ls_service.h"
#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
ObTabletDDLCompleteReplayExecutor::ObTabletDDLCompleteReplayExecutor()
  : logservice::ObTabletReplayExecutor(),
    user_ctx_(nullptr), user_data_(nullptr)
{
}

int ObTabletDDLCompleteReplayExecutor::init(
    mds::BufferCtx &user_ctx,
    const share::SCN &scn,
    const bool for_old_mds,
    const ObTabletDDLCompleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet create replay executor init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(scn));
  } else {
    user_ctx_ = &user_ctx;
    scn_ = scn;
    is_inited_ = true;
    for_old_mds_ = for_old_mds;
    user_data_ = &user_data;
  }
  return ret;
}

int ObTabletDDLCompleteReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret     = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*user_ctx_);

  /* freeze ddl kv & update table sotre make ddl kv can be used for reading */
  if (!tablet_handle.is_valid() || nullptr == user_data_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle), KP(user_data_));
  } else if (OB_FAIL(freeze_ddl_kv(*tablet_handle.get_obj(), *user_data_))) {
    LOG_WARN("failed to freeze ddl kv", K(ret));
  } else if (OB_FAIL(update_tablet_table_store(*tablet_handle.get_obj(), *user_data_))) {
    LOG_WARN("failed to update tablet table store", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(replay_to_mds_table_(tablet_handle, *user_data_, user_ctx, scn_, for_old_mds_))) {
    LOG_WARN("failed to replay to tablet", K(ret));
  } else if (user_data_->has_complete_) {
    /* use tmp ret to avoid pending replay */
    if (OB_TMP_FAIL(schedule_merge(*tablet_handle.get_obj(), *user_data_))) {
      LOG_WARN("failed to schedule merge", K(tmp_ret), KPC(tablet_handle.get_obj()), KPC(user_data_));
    }
  }
  return ret;
}

int ObTabletDDLCompleteReplayExecutor::freeze_ddl_kv(ObTablet &tablet, const ObTabletDDLCompleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  share::SCN mock_start_scn;
  if (OB_FAIL(mock_start_scn.convert_for_tx(SS_DDL_START_SCN_VAL))) {
    LOG_WARN("failed to convert for tx", K(ret));
  } else if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle, true /* create if need*/))) {
    LOG_WARN("failed to create ddl kv mgr", K(ret));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(mock_start_scn, user_data.snapshot_version_, user_data.data_format_version_))) {
    LOG_WARN("failed freeze ddl kv", K(ret), K(user_data));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->remove_idempotence_checker())) {
    LOG_WARN("remove idempotence checker failed", K(ret));
  }
  return ret;
}

int ObTabletDDLCompleteReplayExecutor::update_tablet_table_store(ObTablet &tablet, const ObTabletDDLCompleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTabletHandle new_tablet_handle;

  const ObSSTable *first_major_sstable = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FALSE_IT(first_major_sstable = static_cast<ObSSTable *>(
                                                table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
  } else if (nullptr != first_major_sstable) {
    /* do nothing */
    LOG_INFO("first major sstable exist, do nothing", K(ret), K(user_data));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(tablet.get_ls_id(), ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(user_data));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else {
    ObUpdateTableStoreParam param(user_data.snapshot_version_,
                                    ObVersionRange::MIN_VERSION, // multi_version_start
                                    &user_data.storage_schema_,
                                    ls_handle.get_ls()->get_rebuild_seq());
    param.ddl_info_.keep_old_ddl_sstable_ = true;
    if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(user_data.table_key_.tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update table store", K(ret));
      if (OB_TIMEOUT == ret || OB_SERVER_OUTOF_DISK_SPACE == ret) {
        ret = OB_EAGAIN;
        LOG_WARN("update table store timeout, bur retry again", K(ret), K(user_data));
      }
    }
  }
  return ret;
}

int ObTabletDDLCompleteReplayExecutor::schedule_merge(ObTablet &tablet, const ObTabletDDLCompleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;

  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLTableMergeDagParam merge_param;
  if (!tablet.is_valid() || !user_data.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet), K(user_data));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::generate_merge_param(user_data, tablet, merge_param))) {
    LOG_WARN("failed to generate merge param", K(ret), K(user_data));
  } else if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle, true /* create if need*/))) {
    LOG_WARN("failed to create ddl kv mgr", K(ret));
  } else {
    merge_param.rec_scn_ = ddl_kv_mgr_handle.get_obj()->get_max_freeze_scn();
  }
  /* schedule merge task */
  if (OB_FAIL(ret)) {
  } else if (!merge_param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge param", K(ret), K(merge_param));
  } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(merge_param))) {
    LOG_WARN("schedule ddl merge dag failed", K(ret), K(merge_param));
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
