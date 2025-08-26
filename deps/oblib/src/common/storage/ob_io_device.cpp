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

#define USING_LOG_PREFIX COMMON
#include "ob_io_device.h"

namespace oceanbase {
namespace common {

/**
 * -------------------------------------ObIOFd--------------------------------------------
 */
ObIOFd::ObIOFd(ObIODevice *device_handle, const int64_t first_id,
    const int64_t second_id, const int64_t third_id,
    const int64_t fd_id, const int64_t slot_version)
  : first_id_(first_id), second_id_(second_id), third_id_(third_id),
    fd_id_(fd_id), slot_version_(slot_version), device_handle_(device_handle)
{
}

void ObIOFd::reset()
{
  first_id_ = -1;
  second_id_ = -1;
  third_id_ = -1;
  fd_id_ = -1;
  slot_version_ = -1;
  device_handle_ = nullptr;
}
uint64_t ObIOFd::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&first_id_, sizeof(first_id_), hash_val);
  hash_val = murmurhash(&second_id_, sizeof(second_id_), hash_val);
  hash_val = murmurhash(&third_id_, sizeof(third_id_), hash_val);
  hash_val = murmurhash(&fd_id_, sizeof(fd_id_), hash_val);
  hash_val = murmurhash(&slot_version_, sizeof(slot_version_), hash_val);
  return hash_val;
}

bool ObIOFd::is_valid() const
{
  bool is_valid = false;
  if (is_block_file()) {
    is_valid = first_id_ >= 0 && second_id_ >= 0;
  } else if (is_backup_block_file()) {
    is_valid = first_id_ >= 0 && second_id_ >= 0 && third_id_ >= 0;
  } else {
    is_valid = (first_id_ == NORMAL_FILE_ID && second_id_ >= 0) || (fd_id_ >= 0 || slot_version_ >= 0);
  }
  return is_valid;
}

bool ObIOFd::is_backup_block_file() const {
  // TODO(yanfeng): Due to the problem of circular dependency during compilation, we will temporarily handle it this way
  return BACKUP_BLOCK_ID_MODE == ((first_id_ >> BACKUP_BLOCK_ID_MODE_SHIFT_SIZE) & BACKUP_BLOCK_ID_MODE_FIELD_MASK);
}

DEFINE_SERIALIZE(ObIOFd)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  if (NULL == buf || buf_len <= 0 || (buf_len - pos) < ser_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len), K(pos), K(ser_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, first_id_))) {
    LOG_WARN("serialize first id failed.", K(ret), K(pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, second_id_))) {
    LOG_WARN("serialize second id failed.", K(ret), K(pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, third_id_))) {
    LOG_WARN("serialize third id failed.", K(ret), K(pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, fd_id_))) {
    LOG_WARN("serialize fd id failed.", K(ret), K(pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, slot_version_))) {
    LOG_WARN("serialize slot version failed.", K(ret), K(pos), K(buf_len), K(ser_len), K(*this));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObIOFd)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &first_id_))) {
    LOG_WARN("decode first_id_ failed.", K(ret), K(pos), K(data_len), K(*this));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &second_id_))) {
    LOG_WARN("decode second_id_ failed.", K(ret), K(pos), K(data_len), K(*this));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &third_id_))) {
    LOG_WARN("decode third_id_ failed.", K(ret), K(pos), K(data_len), K(*this));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &fd_id_))) {
    LOG_WARN("decode fouth_id_ failed.", K(ret), K(pos), K(data_len), K(*this));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &slot_version_))) {
    LOG_WARN("decode slot_version_ failed.", K(ret), K(pos), K(data_len), K(*this));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObIOFd)
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(first_id_);
  len += serialization::encoded_length_i64(second_id_);
  len += serialization::encoded_length_i64(third_id_);
  len += serialization::encoded_length_i64(fd_id_);
  len += serialization::encoded_length_i64(slot_version_);
  return len;
}

/**
 * -------------------------------------ObDirEntryNameFilter--------------------------------------------
 */
int ObDirRegularEntryNameFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (STRLEN(entry->d_name) >= STRLEN(filter_str_)){
    bool is_matched = false;
    switch(type_) {
    case PREFIX:
      if (0 == STRNCMP(entry->d_name, filter_str_, STRLEN(filter_str_))) {
        is_matched = true;
      }
      break;
    case KEY_WORD:
      if (NULL != STRSTR(entry->d_name, filter_str_)) {
        is_matched = true;
      }
      break;
    case SUFFIX:
      if (0 == STRCMP(entry->d_name + STRLEN(entry->d_name) - STRLEN(filter_str_),
                           filter_str_)) {
        is_matched = true;
      }
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid FilterOperator type", K(ret), K(type_));
      break;
    }
    if (OB_SUCC(ret) && is_matched) {
      ObIODirentEntry p_entry(entry->d_name, entry->d_type);
      if (OB_FAIL(d_entrys_.push_back(p_entry))) {
        LOG_WARN("fail to push back directory entry", K(ret), K(p_entry), KCSTRING(filter_str_));
      }
    }
  }
  return ret;
}

/**
 * -------------------------------------ObIODevice--------------------------------------------
 */
int ObIODevice::scan_dir_with_prefix(
    const char *dir_name,
    const char *file_prefix,
    common::ObIArray<ObIODirentEntry> &d_entrys)
{
  ObDirRegularEntryNameFilter f_prefix(file_prefix, ObDirRegularEntryNameFilter::PREFIX, d_entrys);
  return scan_dir(dir_name, f_prefix);
}

void ObIODevice::inc_ref()
{
  IGNORE_RETURN ATOMIC_FAA(&ref_cnt_, 1);
}

void ObIODevice::dec_ref()
{
  int ret = OB_SUCCESS;
  int64_t tmp_ref = ATOMIC_SAF(&ref_cnt_, 1);
  if (tmp_ref < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ref_cnt < 0", K(ret), K(tmp_ref), KCSTRING(lbt()));
  }
}

int64_t ObIODevice::get_ref_cnt()
{
  return ATOMIC_LOAD(&ref_cnt_);
}

int ObIODevice::get_device_name(char *buf, int32_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(len));
  } else {
    snprintf(buf, len, "DeviceType:%d/MediaID:%ld", device_type_, media_id_);
  }
  return ret;
}

}
}
