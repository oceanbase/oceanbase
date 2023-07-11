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

#include "storage/multi_data_source/ob_mds_table_merge_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
namespace mds
{
ObMdsTableMergeTask::ObMdsTableMergeTask()
  : ObITask(ObITaskType::TASK_TYPE_MDS_TABLE_MERGE),
    is_inited_(false),
    param_()
{
}

int ObMdsTableMergeTask::init(const ObMdsTableMergeDagParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else {
    param_ = param;
    param_.generate_ts_ = ObClockGenerator::getClock();
    is_inited_ = true;
  }

  return ret;
}

int ObMdsTableMergeTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  const share::ObLSID &ls_id = param_.ls_id_;
  const common::ObTabletID &tablet_id = param_.tablet_id_;
  const share::SCN &flush_scn = param_.flush_scn_;

  DEBUG_SYNC(AFTER_EMPTY_SHELL_TABLET_CREATE);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(ls_id), K(tablet_handle));
  } else if (OB_FAIL(ls->get_tablet_svr()->build_new_tablet_from_mds_table(tablet_id, flush_scn))) {
    LOG_WARN("failed to build new tablet from mds table", K(ret), K(ls_id), K(tablet_id), K(flush_scn));
  } else {
    share::dag_yield();
  }

  // always notify flush ret
  if (OB_NOT_NULL(tablet) && OB_TMP_FAIL(tablet->notify_mds_table_flush_ret(flush_scn, ret))) {
    LOG_WARN("failed to notify mds table flush ret", K(tmp_ret), K(ls_id), K(tablet_id), K(flush_scn), "flush_ret", ret);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
    ret = tmp_ret;
  }

  return ret;
}
} // namespace mds
} // namespace storage
} // namespace oceanbase
