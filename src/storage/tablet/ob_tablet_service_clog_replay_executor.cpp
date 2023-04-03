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

#include "storage/tablet/ob_tablet_service_clog_replay_executor.h"
#include "logservice/ob_log_base_header.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase::logservice;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
ObTabletServiceClogReplayExecutor::ObTabletServiceClogReplayExecutor(
    ObLS *ls,
    const palf::LSN &lsn,
    const SCN &scn)
  : ls_(ls),
    lsn_(lsn),
    scn_(scn)
{
}

int ObTabletServiceClogReplayExecutor::execute(
    ObLS *ls,
    const char *buf,
    const int64_t buf_size,
    const int64_t pos,
    const palf::LSN &lsn,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ObTabletServiceClogReplayExecutor replay_executor(ls, lsn, scn);
  ObLogBaseHeader base_header;

  if (OB_ISNULL(ls)
      || OB_ISNULL(buf)
      || OB_UNLIKELY(buf_size <= 0)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(!lsn.is_valid())
      || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(replay_executor), K(buf), K(buf_size), K(pos),
        K(lsn), K(scn), K(ret));
  } else if (OB_FAIL(base_header.deserialize(buf, buf_size, tmp_pos))) {
    LOG_WARN("log base header deserialize error", K(ret));
  } else {
    const ObLogBaseType log_type = base_header.get_log_type();
    switch (log_type) {
    case ObLogBaseType::STORAGE_SCHEMA_LOG_BASE_TYPE:
      ret = replay_executor.replay_update_storage_schema(scn, buf, buf_size, tmp_pos);
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("log type not supported", K(ret), K(log_type));
    }
  }

  return ret;
}

int ObTabletServiceClogReplayExecutor::replay_update_storage_schema(
    const SCN &scn,
    const char *buf,
    const int64_t buf_size,
    const int64_t pos)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  ObTabletHandle handle;
  int64_t tmp_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_size <= pos)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(buf_size <= 0)
      || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_FAIL(tablet_id.deserialize(buf, buf_size, tmp_pos))) {
    LOG_WARN("fail to deserialize tablet id", K(ret), K(buf_size), K(pos), K(tablet_id));
  } else if (OB_FAIL(ls_->replay_get_tablet(tablet_id, scn, handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // TODO (bowen.gbw): unify multi data replay logic
      LOG_INFO("tablet does not exist, skip", K(ret), K(tablet_id));
    } else if (OB_EAGAIN == ret) {
      if (REACH_TIME_INTERVAL(3 * 1000 * 1000)) {
        LOG_WARN("unexpected EAGAIN error", K(ret), K(tablet_id), K(scn));
      }
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(handle.get_obj()->replay_update_storage_schema(scn, buf, buf_size, tmp_pos))) {
    LOG_WARN("update tablet storage schema fail", K(ret), K(tablet_id), K(scn));
  }
  return ret;
}
}
}
