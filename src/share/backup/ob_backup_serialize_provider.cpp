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

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_backup_serialize_provider.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase;
using namespace common;
using namespace share;


/**
 * ------------------------------ObBackupSerializeHeaderWrapper---------------------
 */
bool ObBackupSerializeHeaderWrapper::is_valid() const
{
  return OB_NOT_NULL(serializer_) && serializer_->is_valid();
}

uint16_t ObBackupSerializeHeaderWrapper::get_data_type() const
{
  return serializer_->get_data_type();
}

uint16_t ObBackupSerializeHeaderWrapper::get_data_version() const
{
  return serializer_->get_data_version();
}

uint16_t ObBackupSerializeHeaderWrapper::get_compressor_type() const
{
  return serializer_->get_compressor_type();
}

int ObBackupSerializeHeaderWrapper::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t required_size = get_serialize_size();
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret), KP(buf));
  } else if (buf_len - pos < required_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not enough", K(ret), K(buf_len), K(pos), K(required_size));
  } else if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not valid", K(ret), K(*this));
  } else {
    // serialize common header.
    ObBackupCommonHeader *common_header = new(buf + pos) ObBackupCommonHeader;
    common_header->reset();
    common_header->compressor_type_ = get_compressor_type();
    common_header->data_type_ = get_data_type();
    common_header->data_version_ = get_data_version();
    pos += sizeof(ObBackupCommonHeader);
    int64_t saved_pos = pos;
    if (OB_FAIL(serializer_->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to do serialize", K(ret), K(*this));
    } else {
      common_header->data_length_ = pos - saved_pos;
      common_header->data_zlength_ = common_header->data_length_;
      if (OB_FAIL(common_header->set_checksum(buf + saved_pos, common_header->data_length_))) {
        LOG_WARN("failed to set common header checksum", K(ret));
      }
    }
  }

  return ret;
}

int ObBackupSerializeHeaderWrapper::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (data_len < sizeof(ObBackupCommonHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf size is too small", K(ret), K(data_len), K(sizeof(ObBackupCommonHeader)));
  } else if (OB_ISNULL(serializer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serializer is null", K(ret));
  } else {
    const ObBackupCommonHeader *common_header = reinterpret_cast<const ObBackupCommonHeader*>(buf);
    int64_t header_len = common_header->header_length_;
    uint16_t data_type = common_header->data_type_;
    uint16_t version = common_header->data_version_;
    pos = pos + header_len;
    if (OB_FAIL(common_header->check_header_checksum())) {
      LOG_WARN("failed to check common header", K(ret));
    } else if (common_header->data_zlength_ > data_len - header_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf size is too small", K(ret), K(data_len), K(*common_header));
    } else if (data_type != get_data_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data type not match", K(ret), K(*common_header), K(get_data_type()));
    } else if (version != get_data_version()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data version not match", K(ret), K(*common_header), K(get_data_version()));
    } else if (OB_FAIL(common_header->check_data_checksum(buf + header_len, common_header->data_zlength_))) {
      LOG_WARN("failed to check checksum", K(ret), K(*common_header));
    } else if (OB_FAIL(serializer_->deserialize(buf, header_len + common_header->data_zlength_, pos))) {
      LOG_WARN("failed to do deserialize", K(ret), K(*common_header));
    } else if (!serializer_->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("serializer is not valid after serialize", K(ret), K(*serializer_));
    }
  }

  return ret;
}

int64_t ObBackupSerializeHeaderWrapper::get_serialize_size() const
{
  int64_t len = sizeof(ObBackupCommonHeader);
  if (OB_NOT_NULL(serializer_)) {
    len += serializer_->get_serialize_size();
  }
  return len;
}