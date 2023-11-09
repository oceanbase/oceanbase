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

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_slog_helper.h"

#include "lib/oblog/ob_log.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/slog/ob_storage_log_struct.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
int ObTabletSlogHelper::write_update_tablet_slog(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMetaDiskAddr &disk_addr)
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  const ObTabletMapKey tablet_key(ls_id, tablet_id);
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !disk_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
  } else if (CLICK_FAIL(THE_IO_DEVICE->fsync_block())) { // make sure that all data or meta written on the macro block is flushed
    LOG_WARN("fail to fsync_block", K(ret));
  } else {
    ObUpdateTabletLog slog_entry(ls_id, tablet_id, disk_addr);
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_UPDATE_TABLET);
    log_param.data_ = &slog_entry;
    if (CLICK_FAIL(MTL(ObStorageLogger*)->write_log(log_param))) {
      LOG_WARN("fail to write slog for creating tablet", K(ret), K(log_param));
    } else {
      do {
        if (CLICK_FAIL(MTL(ObTenantCheckpointSlogHandler*)->report_slog(tablet_key, log_param.disk_addr_))) {
          if (OB_ALLOCATE_MEMORY_FAILED != ret) {
            LOG_WARN("fail to report slog", K(ret), K(tablet_key));
          } else if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
            LOG_WARN("fail to report slog due to memory limit", K(ret), K(tablet_key));
          }
        }
      } while (OB_ALLOCATE_MEMORY_FAILED == ret);
    }
  }
  return ret;
}

int ObTabletSlogHelper::write_empty_shell_tablet_slog(ObTablet *tablet, ObMetaDiskAddr &disk_addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet->is_empty_shell())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("the tablet is not empty shell", K(ret), K(tablet));
  } else {
    const ObTabletMapKey tablet_key(tablet->get_tablet_meta().ls_id_, tablet->get_tablet_meta().tablet_id_);
    ObEmptyShellTabletLog slog_entry(tablet->get_tablet_meta().ls_id_,
                                     tablet->get_tablet_meta().tablet_id_,
                                     tablet);
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_EMPTY_SHELL_TABLET);
    log_param.data_ = &slog_entry;
    if (OB_FAIL(MTL(ObStorageLogger*)->write_log(log_param))) {
      LOG_WARN("fail to write slog for empty shell tablet", K(ret), K(log_param));
    } else if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->report_slog(tablet_key, log_param.disk_addr_))) {
      LOG_WARN("fail to report slog", K(ret), K(tablet_key));
    } else {
      disk_addr = log_param.disk_addr_;
    }
  }
  return ret;
}

int ObTabletSlogHelper::write_remove_tablet_slog(
    const ObLSID &ls_id,
    const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  // We can split the tablet_ids array due to following reasons:
  // 1. batch remove tablets doesn't need atomic semantic, they can be written in different log items
  // 2. log item batch header count is int16_t type, we can't over the limit
  const int64_t MAX_ARRAY_SIZE = 32000;
  const int64_t total_cnt = tablet_ids.count();
  ObSEArray<ObTabletID, 16> current_tablet_arr;
  int64_t finish_cnt = 0;
  int64_t cur_cnt = 0;
  while (OB_SUCC(ret) && finish_cnt < total_cnt) {
    current_tablet_arr.reset();
    cur_cnt = MIN(MAX_ARRAY_SIZE, total_cnt - finish_cnt);

    if (OB_FAIL(current_tablet_arr.reserve(cur_cnt))) {
      STORAGE_REDO_LOG(WARN, "reserve array fail", K(ret), K(cur_cnt), K(total_cnt), K(finish_cnt));
    }

    for (int64_t i = finish_cnt; OB_SUCC(ret) && i < finish_cnt + cur_cnt; ++i) {
      if (OB_FAIL(current_tablet_arr.push_back(tablet_ids.at(i)))) {
        STORAGE_REDO_LOG(WARN, "push back tablet id fail", K(ret), K(cur_cnt), K(total_cnt),
            K(finish_cnt), K(i));
      }
    }

    if (OB_FAIL(ret)){
    } else if (OB_FAIL(safe_batch_write_remove_tablet_slog(ls_id, current_tablet_arr))){
      STORAGE_REDO_LOG(WARN, "inner write log fail", K(ret), K(cur_cnt), K(total_cnt), K(finish_cnt));
    } else {
      finish_cnt += cur_cnt;
    }
  }

  return ret;
}

int ObTabletSlogHelper::write_remove_tablet_slog(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id));
  } else {
    ObDeleteTabletLog slog_entry(ls_id, tablet_id);
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET);
    log_param.data_ = &slog_entry;
    if (OB_FAIL(MTL(ObStorageLogger*)->write_log(log_param))) {
      LOG_WARN("fail to write slog for creating tablet", K(ret), K(log_param));
    }
  }
  return ret;
}

int ObTabletSlogHelper::safe_batch_write_remove_tablet_slog(
    const ObLSID &ls_id,
    const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_count = tablet_ids.count();
  const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET);
  ObSArray<ObDeleteTabletLog> slog_array;
  ObSArray<ObStorageLogParam> param_array;
  const bool need_write = (tablet_count > 0);

  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id));
  } else if (!need_write) {
  } else if (OB_FAIL(slog_array.reserve(tablet_count))) {
    LOG_WARN("failed to reserve for slog array", K(ret), K(tablet_count));
  } else if (OB_FAIL(param_array.reserve(tablet_count))) {
    LOG_WARN("failed to reserve for param array", K(ret), K(tablet_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      ObDeleteTabletLog slog_entry(ls_id, tablet_id);
      if (OB_UNLIKELY(!tablet_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet id is invalid", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(slog_array.push_back(slog_entry))) {
        LOG_WARN("fail to push slog entry into slog array", K(ret), K(slog_entry), K(i));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; i++) {
      ObStorageLogParam log_param(cmd, &slog_array[i]);
      if (OB_FAIL(param_array.push_back(log_param))) {
        LOG_WARN("fail to push log param into param array", K(ret), K(log_param), K(i));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!need_write) {
  } else if (OB_FAIL(MTL(ObStorageLogger*)->write_log(param_array))) {
    LOG_WARN("fail to write slog for batch deleting tablet", K(ret), K(param_array));
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase
