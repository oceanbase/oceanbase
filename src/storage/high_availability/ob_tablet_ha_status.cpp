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

#include "ob_tablet_ha_status.h"
#include "lib/utility/serialization.h"


namespace oceanbase
{
namespace storage
{

bool ObTabletRestoreStatus::is_valid(const ObTabletRestoreStatus::STATUS &status)
{
  return status >= ObTabletRestoreStatus::FULL
      && status < ObTabletRestoreStatus::RESTORE_STATUS_MAX;
}

int ObTabletRestoreStatus::check_can_change_status(
    const ObTabletRestoreStatus::STATUS &cur_status,
    const ObTabletRestoreStatus::STATUS &change_status,
    bool &can_change)
{
  int ret = OB_SUCCESS;
  can_change = false;

  if (!ObTabletRestoreStatus::is_valid(cur_status) || !ObTabletRestoreStatus::is_valid(change_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can change status get invalid argument", K(ret), K(cur_status), K(change_status));
  } else {
    switch (cur_status) {
    case ObTabletRestoreStatus::PENDING: {
      if (ObTabletRestoreStatus::UNDEFINED == change_status
          || ObTabletRestoreStatus::EMPTY == change_status
          || ObTabletRestoreStatus::PENDING == change_status) {
        can_change = true;
      }
      break;
    }
    case ObTabletRestoreStatus::UNDEFINED: {
      if (ObTabletRestoreStatus::PENDING == change_status
          || ObTabletRestoreStatus::UNDEFINED == change_status) {
        can_change = true;
      }
      break;
    }
    case ObTabletRestoreStatus::EMPTY: {
      if (ObTabletRestoreStatus::MINOR_AND_MAJOR_META == change_status
          || ObTabletRestoreStatus::FULL == change_status
          || ObTabletRestoreStatus::EMPTY == change_status
          || ObTabletRestoreStatus::UNDEFINED == change_status) {
        can_change = true;
      }
      break;
    }
    case ObTabletRestoreStatus::MINOR_AND_MAJOR_META: {
      if (ObTabletRestoreStatus::FULL == change_status
          || ObTabletRestoreStatus::MINOR_AND_MAJOR_META == change_status) {
        can_change = true;
      }
      break;
    }
    case ObTabletRestoreStatus::FULL: {
      if (ObTabletRestoreStatus::FULL == change_status
          || ObTabletRestoreStatus::EMPTY == change_status) {
        can_change = true;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(cur_status));
    }
    }

    if (OB_SUCC(ret) && !can_change) {
      FLOG_INFO("can not change tablet restore status", K(cur_status), K(change_status));
    }

  }
  return ret;
}

bool ObTabletDataStatus::is_valid(const ObTabletDataStatus::STATUS &status)
{
  return status >= ObTabletDataStatus::COMPLETE
      && status < ObTabletDataStatus::DATA_STATUS_MAX;
}

int ObTabletDataStatus::check_can_change_status(
    const ObTabletDataStatus::STATUS &cur_status,
    const ObTabletDataStatus::STATUS &change_status,
    bool &can_change)
{
  int ret = OB_SUCCESS;
  can_change = false;

  if (!ObTabletDataStatus::is_valid(cur_status) || !ObTabletDataStatus::is_valid(change_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can change status get invalid argument", K(ret), K(cur_status), K(change_status));
  } else {
    switch (cur_status) {
    case ObTabletDataStatus::INCOMPLETE: {
      if (ObTabletDataStatus::COMPLETE == change_status
          || ObTabletDataStatus::INCOMPLETE == change_status) {
        can_change = true;
      }
      break;
    }
    case ObTabletDataStatus::COMPLETE: {
      if (ObTabletDataStatus::COMPLETE == change_status) {
        can_change = true;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(cur_status));
    }
    }

    if (OB_SUCC(ret) && !can_change) {
      FLOG_INFO("can not change tablet data status", K(cur_status), K(change_status));
    }
  }
  return ret;
}

bool ObTabletExpectedStatus::is_valid(const ObTabletExpectedStatus::STATUS &status)
{
  return status >= ObTabletExpectedStatus::NORMAL
      && status < ObTabletExpectedStatus::EXPECTED_STATUS_MAX;
}

int ObTabletExpectedStatus::check_can_change_status(
    const ObTabletExpectedStatus::STATUS &cur_status,
    const ObTabletExpectedStatus::STATUS &change_status,
    bool &can_change)
{
  int ret = OB_SUCCESS;
  can_change = false;

  if (!ObTabletExpectedStatus::is_valid(cur_status) || !ObTabletExpectedStatus::is_valid(change_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can change status get invalid argument", K(ret), K(cur_status), K(change_status));
  } else if (ObTabletExpectedStatus::NORMAL == cur_status && ObTabletExpectedStatus::DELETED == change_status) {
    can_change = true;
  } else {
    FLOG_INFO("can not change tablet meta status", K(cur_status), K(change_status));
  }
  return ret;
}

ObTabletHAStatus::ObTabletHAStatus()
  : restore_status_(ObTabletRestoreStatus::RESTORE_STATUS_MAX),
    data_status_(ObTabletDataStatus::DATA_STATUS_MAX),
    reserved_(0)
{
}

bool ObTabletHAStatus::is_valid() const
{
  return ObTabletRestoreStatus::is_valid(restore_status_)
      && ObTabletDataStatus::is_valid(data_status_);
}

int ObTabletHAStatus::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, len, new_pos, ha_status_))) {
    LOG_WARN("serialize ha status failed.", K(ret), K(new_pos), K(len), K(ha_status_), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObTabletHAStatus::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ha_status_))) {
    LOG_WARN("failed to deserialize ha status", K(ret), K(len), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int64_t ObTabletHAStatus::get_serialize_size() const
{
  return serialization::encoded_length_i64(ha_status_);
}

int ObTabletHAStatus::set_restore_status(const ObTabletRestoreStatus::STATUS &restore_status)
{
  int ret = OB_SUCCESS;

  if (!ObTabletRestoreStatus::is_valid(restore_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set restore status get invalid argument", K(ret), K(restore_status));
  } else {
    restore_status_ = restore_status;
  }
  return ret;
}

int ObTabletHAStatus::get_restore_status(ObTabletRestoreStatus::STATUS &restore_status) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ha status do not init", K(ret), KPC(this));
  } else {
    restore_status = restore_status_;
  }
  return ret;
}

int ObTabletHAStatus::set_data_status(const ObTabletDataStatus::STATUS &data_status)
{
  int ret = OB_SUCCESS;

  if (!ObTabletDataStatus::is_valid(data_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set data status get invalid argument", K(ret), K(data_status));
  } else {
    data_status_ = data_status;
  }
  return ret;
}

int ObTabletHAStatus::get_data_status(ObTabletDataStatus::STATUS &data_status) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ha status do not init", K(ret), KPC(this));
  } else {
    data_status = data_status_;
  }
  return ret;
}

int ObTabletHAStatus::set_expected_status(const ObTabletExpectedStatus::STATUS &expected_status)
{
  int ret = OB_SUCCESS;
  if (!ObTabletExpectedStatus::is_valid(expected_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set self status get invalid argument", K(ret), K(expected_status));
  } else {
    expected_status_ = expected_status;
  }
  return ret;
}

int ObTabletHAStatus::get_expected_status(ObTabletExpectedStatus::STATUS &expected_status) const
{
  int ret = OB_SUCCESS;
  expected_status = ObTabletExpectedStatus::EXPECTED_STATUS_MAX;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ha status do not init", K(ret), KPC(this));
  } else {
    expected_status = expected_status_;
  }
  return ret;
}

int ObTabletHAStatus::init_status()
{
  int ret = OB_SUCCESS;
  restore_status_ = ObTabletRestoreStatus::FULL;
  data_status_ = ObTabletDataStatus::COMPLETE;
  expected_status_ = ObTabletExpectedStatus::NORMAL;
  return ret;
}

int ObTabletHAStatus::init_status_for_ha(const ObTabletHAStatus &ha_status)
{
  int ret = OB_SUCCESS;
  ObTabletRestoreStatus::STATUS restore_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  ObTabletExpectedStatus::STATUS expected_status = ObTabletExpectedStatus::EXPECTED_STATUS_MAX;

  if (!ha_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ha status is invalid", K(ret), K(ha_status));
  } else if (OB_FAIL(ha_status.get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(ha_status));
  } else if (OB_FAIL(ha_status.get_expected_status(expected_status))) {
    LOG_WARN("failed to get self status", K(ret), K(ha_status));
  } else {
    restore_status_ = restore_status;
    data_status_ = ObTabletDataStatus::INCOMPLETE;
    expected_status_ = expected_status;
  }
  return ret;
}

void ObTabletHAStatus::reset()
{
  restore_status_ = ObTabletRestoreStatus::STATUS::RESTORE_STATUS_MAX;
  data_status_ = ObTabletDataStatus::STATUS::DATA_STATUS_MAX;
  expected_status_ = ObTabletExpectedStatus::EXPECTED_STATUS_MAX;
  reserved_ = 0;
}


}
}
