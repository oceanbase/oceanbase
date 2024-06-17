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
#include "ob_ls_transfer_info.h"

using namespace oceanbase;
using namespace share;
using namespace storage;


ObLSTransferInfo::ObLSTransferInfo()
  : ls_id_(TRANSFER_INIT_LS_ID),
    transfer_start_scn_(share::SCN::invalid_scn())
{
}

int ObLSTransferInfo::init(
    const share::ObLSID &ls_id,
    const share::SCN &transfer_start_scn)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !transfer_start_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init transfer info get invalid argument", K(ret), K(ls_id), K(transfer_start_scn));
  } else {
    ls_id_ = ls_id;
    transfer_start_scn_ = transfer_start_scn;
  }
  return ret;
}

void ObLSTransferInfo::reset()
{
  ls_id_.reset();
  transfer_start_scn_.reset();
}

bool ObLSTransferInfo::is_valid() const
{
  return ls_id_.is_valid()
      && transfer_start_scn_.is_valid();
}

bool ObLSTransferInfo::already_enable_replay() const
{
  return !is_valid();
}

bool ObTransferInTransStatus::is_valid(const ObTransferInTransStatus::STATUS &status)
{
  return status >= NONE && status < MAX;
}

bool ObTransferInTransStatus::can_skip_barrier(const ObTransferInTransStatus::STATUS &status)
{
  return NONE != status;
}

bool ObTransferInTransStatus::allow_gc(const ObTransferInTransStatus::STATUS &status)
{
  return NONE == status
      || ABORT == status;
}

int ObTransferInTransStatus::check_can_change_status(
    const ObTransferInTransStatus::STATUS &cur_status,
    const ObTransferInTransStatus::STATUS &change_status,
    bool &can_change)
{
  int ret = OB_SUCCESS;
  can_change = false;
  if (!is_valid(cur_status) || !is_valid(change_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can change status get invalid argument", K(ret), K(cur_status), K(change_status));
  } else {
    switch (cur_status) {
    case ObTransferInTransStatus::NONE: {
      if (ObTransferInTransStatus::NONE == change_status
          || ObTransferInTransStatus::PREPARE == change_status
          || ObTransferInTransStatus::ABORT == change_status) {
        can_change = true;
      }
      break;
    }
    case ObTransferInTransStatus::PREPARE: {
      if (ObTransferInTransStatus::PREPARE == change_status
          || ObTransferInTransStatus::NONE == change_status
          || ObTransferInTransStatus::ABORT == change_status) {
        can_change = true;
      }
      break;
    }
    case ObTransferInTransStatus::ABORT: {
      if (ObTransferInTransStatus::ABORT == change_status
          || ObTransferInTransStatus::NONE == change_status) {
        can_change = true;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(cur_status), K(change_status));
    }
    }
  }
  return ret;
}

ObTransferTabletIDArray::ObTransferTabletIDArray()
  : count_(0)
{
}

ObTransferTabletIDArray::~ObTransferTabletIDArray()
{
}

OB_DEF_SERIALIZE(ObTransferTabletIDArray)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(id_array_, count_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTransferTabletIDArray)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(id_array_, count_);
  return len;
}

OB_DEF_DESERIALIZE(ObTransferTabletIDArray)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;

  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret)) {
    count_ = count;
  }
  OB_UNIS_DECODE_ARRAY(id_array_, count_);
  return ret;
}

int ObTransferTabletIDArray::push_back(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet id is invalid", K(ret), K(tablet_id));
  } else if (count_ >= MAX_TABLET_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("transfer tablet id array is size overflow", K(ret), K(count_));
  } else {
    id_array_[count_] = tablet_id;
    count_++;
  }
  return ret;
}

int ObTransferTabletIDArray::assign(const common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  if (tablet_id_array.count() > MAX_TABLET_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot assign tablet id array", K(ret), K(tablet_id_array));
  } else {
    count_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
      const common::ObTabletID &tablet_id = tablet_id_array.at(i);
      if (OB_FAIL(push_back(tablet_id))) {
        LOG_WARN("failed to push tablet id into array", K(ret));
      }
    }
  }
  return ret;
}

int ObTransferTabletIDArray::get_tablet_id_array(
    common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  tablet_id_array.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
    if (OB_FAIL(tablet_id_array.push_back(id_array_[i]))) {
      LOG_WARN("failed to push tablet id into array", K(ret), K(count_), K(i));
    }
  }
  return ret;
}


ObLSTransferMetaInfo::ObLSTransferMetaInfo()
  : src_ls_(TRANSFER_INIT_LS_ID),
    src_scn_(share::SCN::base_scn()),
    trans_status_(ObTransferInTransStatus::NONE),
    tablet_id_array_(),
    data_version_(DEFAULT_MIN_DATA_VERSION)
{
}

int ObLSTransferMetaInfo::set_transfer_info(
    const share::ObLSID &src_ls,
    const share::SCN &src_scn,
    const ObTransferInTransStatus::STATUS &trans_status,
    const common::ObIArray<common::ObTabletID> &tablet_id_array,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  bool can_change = false;

  if (!src_ls.is_valid() || !src_scn.is_valid() || !ObTransferInTransStatus::is_valid(trans_status) || 0 == data_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set transfer info get invalid argument", K(ret), K(src_ls), K(src_scn), K(trans_status), K(data_version));
  } else if (OB_FAIL(update_trans_status_(trans_status))) {
    LOG_WARN("failed to update trans status", K(ret), K(trans_status));
  } else if (OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_id_array));
  } else {
    src_ls_ = src_ls;
    src_scn_ = src_scn;
    trans_status_ = trans_status;
    data_version_ = data_version;
  }
  return ret;
}

int ObLSTransferMetaInfo::cleanup_transfer_info()
{
  int ret = OB_SUCCESS;
  bool can_change = false;
  const ObTransferInTransStatus::STATUS trans_status = ObTransferInTransStatus::NONE;

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls transfer meta info is invalid, unexpected", K(ret), KPC(this));
  } else if (OB_FAIL(update_trans_status_(trans_status))) {
    LOG_WARN("failed to update trans status", K(ret), K(trans_status), KPC(this));
  } else {
    src_ls_ = TRANSFER_INIT_LS_ID;
    src_scn_.set_base();
    tablet_id_array_.reset();
  }
  return ret;
}

int ObLSTransferMetaInfo::update_trans_status(const ObTransferInTransStatus::STATUS &trans_status)
{
  int ret = OB_SUCCESS;
  if (!ObTransferInTransStatus::is_valid(trans_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update trans status get invalid argument", K(ret), K(trans_status));
  } else if (OB_FAIL(update_trans_status_(trans_status))) {
    LOG_WARN("failed to update trans status", K(ret), K(trans_status));
  }
  return ret;
}

void ObLSTransferMetaInfo::reset()
{
  src_ls_ = TRANSFER_INIT_LS_ID;
  src_scn_.set_base();
  trans_status_ = ObTransferInTransStatus::NONE;
  tablet_id_array_.reset();
  data_version_ = 0;
}

bool ObLSTransferMetaInfo::is_valid() const
{
  return src_ls_.is_valid()
      && src_scn_.is_valid()
      && ObTransferInTransStatus::is_valid(trans_status_)
      && data_version_ > 0;
}

bool ObLSTransferMetaInfo::is_equal(
    const share::ObLSID &src_ls,
    const share::SCN &src_scn,
    const ObTransferInTransStatus::STATUS &trans_status,
    const ObTransferTabletIDArray &tablet_id_array)
{
  bool b_ret = false;
  if (src_ls_ == src_ls
      && src_scn_ == src_scn
      && trans_status_ == trans_status) {
    b_ret = true;
  }

  if (b_ret) {
    if (tablet_id_array_.count() != tablet_id_array.count()) {
      b_ret = false;
    } else {
      for (int64_t i = 0; i < tablet_id_array_.count(); ++i) {
        if (tablet_id_array_.at(i) != tablet_id_array.at(i)) {
          b_ret = false;
          break;
        }
      }
    }
  }
  return b_ret;
}

bool ObLSTransferMetaInfo::is_empty()
{
  ObLSID init_ls(TRANSFER_INIT_LS_ID);
  return src_ls_ == init_ls
      && src_scn_.is_min()
      && ObTransferInTransStatus::NONE == trans_status_
      && tablet_id_array_.empty();
}

bool ObLSTransferMetaInfo::allow_src_ls_gc()
{
  return ObTransferInTransStatus::NONE == trans_status_
      || ObTransferInTransStatus::ABORT == trans_status_;
}

bool ObLSTransferMetaInfo::need_check_transfer_tablet()
{
  return ObTransferInTransStatus::ABORT == trans_status_
      || tablet_id_array_.empty();
}

int ObLSTransferMetaInfo::check_tablet_in_list(
    const common::ObTabletID &tablet_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;

  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check tablet in list get invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array_.count(); ++i) {
      if (tablet_id == tablet_id_array_.at(i)) {
        is_exist = true;
        break;
      }
    }
  }
  return ret;
}

bool ObLSTransferMetaInfo::is_in_trans()
{
  return ObTransferInTransStatus::ABORT == trans_status_
      || ObTransferInTransStatus::PREPARE == trans_status_;
}

bool ObLSTransferMetaInfo::is_trans_status_same(
    const ObTransferInTransStatus::STATUS &trans_status)
{
  return trans_status_ == trans_status;
}

bool ObLSTransferMetaInfo::is_abort_status()
{
  return ObTransferInTransStatus::ABORT == trans_status_;
}

int ObLSTransferMetaInfo::update_trans_status_(
    const ObTransferInTransStatus::STATUS &trans_status)
{
  int ret = OB_SUCCESS;
  bool can_change = false;
  if (OB_FAIL(ObTransferInTransStatus::check_can_change_status(trans_status_, trans_status, can_change))) {
    LOG_WARN("failed to check can change status", K(ret), K(trans_status_), K(trans_status));
  } else if (!can_change) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot change transfer in trans status", K(ret), K(trans_status_), K(trans_status));
  } else {
    trans_status_ = trans_status;
  }
  return ret;
}

int ObLSTransferMetaInfo::get_tablet_id_array(
    common::ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  tablet_id_array.reset();
  if (OB_FAIL(tablet_id_array_.get_tablet_id_array(tablet_id_array))) {
    LOG_WARN("failed to get tablet id array", K(ret), K(tablet_id_array_));
  }
  return ret;
}

bool ObLSTransferMetaInfo::is_in_compatible_status()
{
  return data_version_ < MOCK_DATA_VERSION_4_2_3_0;
}

int64_t ObLSTransferMetaInfo::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t save_pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    ObClusterVersion data_version;
    if (OB_FAIL(data_version.init(data_version_))) {
      LOG_WARN("failed to init data version", K(ret), K(data_version_));
    } else {
      J_OBJ_START();
      J_KV(K_(src_ls), K_(src_scn), K_(trans_status), K_(tablet_id_array), K(data_version));
      J_OBJ_END();
    }
  }
  return pos;
}

OB_SERIALIZE_MEMBER(ObLSTransferMetaInfo, src_ls_, src_scn_, trans_status_, tablet_id_array_, data_version_);
