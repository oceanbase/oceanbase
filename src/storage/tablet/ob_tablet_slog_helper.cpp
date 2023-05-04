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
int ObTabletSlogHelper::write_create_tablet_slog(
    const ObTabletHandle &tablet_handle,
    ObMetaDiskAddr &disk_addr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_handle));
  } else if (OB_FAIL(THE_IO_DEVICE->fsync_block())) { // make sure that all data or meta written on the macro block is flushed
    LOG_WARN("fail to fsync_block", K(ret));
  } else {
    ObCreateTabletLog slog_entry(tablet_handle.get_obj());
    ObStorageLogParam log_param;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
        ObRedoLogSubType::OB_REDO_LOG_PUT_TABLET);
    log_param.data_ = &slog_entry;
    if (OB_FAIL(MTL(ObStorageLogger*)->write_log(log_param))) {
      LOG_WARN("fail to write slog for creating tablet", K(ret), K(log_param));
    } else {
      disk_addr = log_param.disk_addr_;;
    }
  }

  return ret;
}

int ObTabletSlogHelper::write_create_tablet_slog(
    const common::ObIArray<ObTabletHandle> &tablet_handle_array,
    common::ObIArray<ObMetaDiskAddr> &disk_addr_array)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_count = tablet_handle_array.count();
  const int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_PUT_TABLET);
  ObSArray<ObCreateTabletLog> slog_array;
  ObSArray<ObStorageLogParam> param_array;

  if (OB_FAIL(slog_array.reserve(tablet_count))) {
    LOG_WARN("failed to reserve for slog array", K(ret), K(tablet_count));
  } else if (OB_FAIL(param_array.reserve(tablet_count))) {
    LOG_WARN("failed to reserve for param array", K(ret), K(tablet_count));
  } else if (OB_FAIL(disk_addr_array.reserve(tablet_count))) {
    LOG_WARN("failed to reserve for disk addr array", K(ret), K(tablet_count));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; ++i) {
    const ObTabletHandle &tablet_handle = tablet_handle_array.at(i);
    if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet handle is invalid", K(ret), K(tablet_handle));
    } else {
      ObCreateTabletLog slog_entry(tablet_handle.get_obj());
      if (OB_FAIL(slog_array.push_back(slog_entry))) {
        LOG_WARN("fail to push slog entry into slog array", K(ret), K(slog_entry), K(i));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; i++) {
    ObStorageLogParam log_param(cmd, &slog_array[i]);
    if (OB_FAIL(param_array.push_back(log_param))) {
      LOG_WARN("fail to push log param into param array", K(ret), K(log_param), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(THE_IO_DEVICE->fsync_block())) { // make sure that all data or meta written on the macro block is flushed
    LOG_WARN("fail to fsync_block", K(ret));
  } else if (OB_FAIL(MTL(ObStorageLogger*)->write_log(param_array))) {
    LOG_WARN("fail to write slog for batch creating tablet", K(ret), K(param_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_count; i++) {
      if (OB_FAIL(disk_addr_array.push_back(param_array[i].disk_addr_))) {
        LOG_WARN("fail to push disk addr into disk addr array", K(ret), K(i), K(tablet_count));
      }
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
