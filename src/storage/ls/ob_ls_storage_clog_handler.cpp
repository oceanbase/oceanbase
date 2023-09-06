//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "storage/ls/ob_ls_storage_clog_handler.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_base_header.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace logservice;
namespace storage
{

int ObLSStorageClogHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSReservedSnapshotMgr is inited", K(ret), KP(ls));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls));
  } else {
    ls_ = ls;
    is_inited_ = true;
  }
  return ret;
}

void ObLSStorageClogHandler::reset()
{
  is_inited_ = false;
  ls_ = nullptr;
}

// for replay
int ObLSStorageClogHandler::replay(
    const void *buffer,
    const int64_t nbytes,
    const palf::LSN &lsn,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  const char *buf = nullptr;
  ObLogBaseHeader base_header;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == buffer
      || nbytes <= 0
      || !lsn.is_valid()
      || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(buffer), K(nbytes), K(lsn), K(scn));
  } else if (FALSE_IT(buf = static_cast<const char *>(buffer))) {
  } else if (OB_FAIL(base_header.deserialize(buf, nbytes, pos))) {
    LOG_WARN("log base header deserialize error", K(ret));
  } else if (OB_FAIL(inner_replay(base_header, scn, buf, nbytes, pos))) {
    LOG_WARN("failed to replay update reserved snapshot", K(ret));
  }
  return ret;
}

int ObLSResvSnapClogHandler::inner_replay(
    const ObLogBaseHeader &base_header,
    const share::SCN &scn,
    const char *buffer,
    const int64_t buffer_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos < 0 || buffer_size <= 0 || pos > buffer_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pos), K(buffer_size));
  } else if (ObLogBaseType::RESERVED_SNAPSHOT_LOG_BASE_TYPE != base_header.get_log_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log header is not valid", K(ret), K(base_header));
  } else if (OB_FAIL(ls_->replay_reserved_snapshot_log(scn, buffer, buffer_size, pos))) {
    LOG_WARN("failed to replay update reserved snapshot", K(ret));
  }
  return ret;
}

int ObMediumCompactionClogHandler::inner_replay(
    const ObLogBaseHeader &base_header,
    const share::SCN &scn,
    const char *buffer,
    const int64_t buffer_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  ObTabletHandle handle;
  int64_t new_pos = pos;
  const bool is_update_mds_table = true;

  if (OB_UNLIKELY(pos < 0 || buffer_size <= 0 || pos > buffer_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pos), K(buffer_size));
  } else if (ObLogBaseType::MEDIUM_COMPACTION_LOG_BASE_TYPE != base_header.get_log_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log header is not valid", K(ret), K(base_header));
  } else if (OB_FAIL(tablet_id.deserialize(buffer, buffer_size, new_pos))) {
    LOG_WARN("fail to deserialize tablet id", K(ret), K(buffer_size), K(pos), K(tablet_id));
  } else if (OB_FAIL(ls_->replay_get_tablet(tablet_id, scn, is_update_mds_table, handle))) {
    if (OB_OBSOLETE_CLOG_NEED_SKIP == ret) {
      LOG_INFO("clog is obsolete, should skip replay", K(ret), K(tablet_id), K(scn));
      ret = OB_SUCCESS;
    } else if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("retry get tablet for timeout error", K(ret), K(tablet_id), K(scn));
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id), K(scn));
    }
  } else if (OB_FAIL(handle.get_obj()->replay_medium_compaction_clog(scn, buffer, buffer_size, new_pos))) {
    LOG_WARN("failed to replay medium compaction clog", K(ret), K(tablet_id), K(scn), K(buffer_size), K(new_pos));
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
