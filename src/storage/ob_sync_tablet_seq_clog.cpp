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

int ObSyncTabletSeqLogCb::init(const ObLSID &ls_id,
                               const ObTabletID &tablet_id, 
                               const uint64_t new_autoinc_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    new_autoinc_seq_ = new_autoinc_seq;
    is_inited_ = true;
  }
  return ret;
}

int ObSyncTabletSeqLogCb::on_success()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletAutoincSeq autoinc_seq;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls handle failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is unexpected null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                    tablet_handle,
                                                    ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      ob_abort();
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->get_latest_autoinc_seq(autoinc_seq))) {
    LOG_WARN("fail to get latest autoinc seq", K(ret));
  } else if (OB_FAIL(autoinc_seq.set_autoinc_seq_value(new_autoinc_seq_))) {
    LOG_WARN("failed to set autoinc seq value", K(ret), K(new_autoinc_seq_));
  } else if (OB_FAIL(tablet_handle.get_obj()->save_multi_source_data_unit(&autoinc_seq,
                                                                          __get_scn(),
                                                                          false/*for_replay*/,
                                                                          memtable::MemtableRefOp::DEC_REF,
                                                                          true/*is_callback*/))) {
    LOG_WARN("failed to save autoinc seq", K(ret), K(autoinc_seq));
  }
  ret_code_ = ret;
  state_ = STATE_SUCCESS;
  try_release();
  return OB_SUCCESS;
}

int ObSyncTabletSeqLogCb::on_failure()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletAutoincSeq autoinc_seq;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls handle failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is unexpected null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                    tablet_handle,
                                                    ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      ob_abort();
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->get_latest_autoinc_seq(autoinc_seq))) {
    LOG_WARN("fail to get latest autoinc seq", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->save_multi_source_data_unit(&autoinc_seq,
                                                                          SCN::invalid_scn()/*scn*/,
                                                                          false/*for_replay*/,
                                                                          memtable::MemtableRefOp::DEC_REF,
                                                                          true/*is_callback*/))) {
    LOG_WARN("failed to save autoinc seq", K(ret), K(autoinc_seq));
  }
  ret_code_ = ret;
  state_ = STATE_FAILED;
  try_release();
  return OB_SUCCESS;
}

void ObSyncTabletSeqLogCb::try_release()
{
  if (ATOMIC_BCAS(&the_other_release_this_, false, true)) {
  } else {
    op_free(this);
  }
}

}
}
