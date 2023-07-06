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

#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/utility/serialization.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/ob_sync_tablet_seq_clog.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{

using namespace common;
using namespace share;

namespace storage
{

int ObSyncTabletSeqLog::init(const ObTabletID &tablet_id, const uint64_t autoinc_seq)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid() || autoinc_seq <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(autoinc_seq));
  } else {
    tablet_id_ = tablet_id;
    autoinc_seq_ = autoinc_seq;
  }
  return ret;
}

int ObSyncTabletSeqLog::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet id", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, len, new_pos, static_cast<int64_t>(autoinc_seq_)))) {
    LOG_WARN("failed to serialize auto inc seq", K(ret), K(len), K(new_pos));
  } else {
    pos = new_pos;
  }

  return ret;
}

int ObSyncTabletSeqLog::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize tablet id", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, (int64_t*)(&autoinc_seq_)))) {
    LOG_WARN("failed to deserialize auto inc seq", K(ret), K(len), K(new_pos));
  } else {
    pos = new_pos;
  }

  return ret;
}

int64_t ObSyncTabletSeqLog::get_serialize_size() const
{
  int64_t size = 0;
  size += tablet_id_.get_serialize_size();
  size += serialization::encoded_length_i64(autoinc_seq_);
  return size;
}


ObSyncTabletSeqMdsLogCb::ObSyncTabletSeqMdsLogCb()
  : state_(ObDDLClogState::STATE_INIT),
    the_other_release_this_(false),
    ret_code_(OB_SUCCESS),
    tablet_handle_(),
    mds_ctx_(mds::MdsWriter(mds::WriterType::AUTO_INC_SEQ))
{
}

int ObSyncTabletSeqMdsLogCb::init(const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t writer_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_srv = MTL(ObLSService *);
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id or tablet id", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ls_srv->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls handle failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is unexpected null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id,
                                                    tablet_handle_,
                                                    0,
                                                    ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ls_id), K(tablet_id));
  } else {
    mds_ctx_.set_writer(mds::MdsWriter(mds::WriterType::AUTO_INC_SEQ, writer_id));
  }
  return ret;
}

int ObSyncTabletSeqMdsLogCb::on_success()
{
  int ret = OB_SUCCESS;
  ret_code_ = ret;
  mds_ctx_.single_log_commit(__get_scn(), __get_scn());
  state_ = STATE_SUCCESS;
  try_release();
  return ret;
}

int ObSyncTabletSeqMdsLogCb::on_failure()
{
  int ret = OB_SUCCESS;
  ret_code_ = ret;
  state_ = STATE_FAILED;
  mds_ctx_.single_log_abort();
  try_release();
  return ret;
}

void ObSyncTabletSeqMdsLogCb::try_release()
{
  if (ATOMIC_BCAS(&the_other_release_this_, false, true)) {
  } else {
    op_free(this);
  }
}

}
}
