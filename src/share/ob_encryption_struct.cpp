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

#include "ob_encryption_struct.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace common;
namespace share
{

OB_DEF_SERIALIZE(ObEncryptMeta)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument, ", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, master_key_version_))) {
    SHARE_LOG(WARN, "serialize master key verison failed", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, encrypt_algorithm_))) {
    SHARE_LOG(WARN, "serialize encrypt algorithm failed", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(encrypted_table_key_.serialize(buf, buf_len, pos))) {
    SHARE_LOG(WARN, "serialize table key failed", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObEncryptMeta)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument, ", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, master_key_version_))) {
    SHARE_LOG(WARN, "deserialize master key version failed", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, encrypt_algorithm_))) {
    SHARE_LOG(WARN, "deserialize encrypt algorithm failed", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(encrypted_table_key_.deserialize(buf, data_len, pos))) {
    SHARE_LOG(WARN, "deserialize table key failed", K(ret), K(data_len), K(pos));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObEncryptMeta)
{
  int64_t size = 0;
  size += serialization::encoded_length(master_key_version_);
  size += serialization::encoded_length(encrypt_algorithm_);
  size += encrypted_table_key_.get_serialize_size();
  return size;
}

int ObEncryptMeta::assign(const ObEncryptMeta &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(master_key_.assign(other.master_key_))) {
    SHARE_LOG(WARN, "failed to assign master_key_", KR(ret), K(other));
  } else if (OB_FAIL(table_key_.assign(other.table_key_))) {
    SHARE_LOG(WARN, "failed to assign table_key_", KR(ret), K(other));
  } else if (OB_FAIL(encrypted_table_key_.assign(other.encrypted_table_key_))) {
    SHARE_LOG(WARN, "failed to assign encrypted_table_key_", KR(ret), K(other));
  } else {
    master_key_version_ = other.master_key_version_;
    encrypt_algorithm_ = other.encrypt_algorithm_;
  }
  return ret;
}

void ObEncryptMeta::reset()
{
  master_key_version_ = -1;
  encrypt_algorithm_ = -1;
  master_key_.reset();
  table_key_.reset();
  encrypted_table_key_.reset();
}

int ObEncryptMeta::replace_tenant_id(const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id_ = real_tenant_id;
  return ret;
}

OB_DEF_SERIALIZE(ObZoneEncryptMeta)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument, ", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    SHARE_LOG(WARN, "serialize verison failed", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, master_key_version_))) {
    SHARE_LOG(WARN, "serialize master key verison failed", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, encrypt_algorithm_))) {
    SHARE_LOG(WARN, "serialize encrypt algorithm failed", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObZoneEncryptMeta)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument, ", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
    SHARE_LOG(WARN, "deserialize master key version failed", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &master_key_version_))) {
    SHARE_LOG(WARN, "deserialize master key version failed", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &encrypt_algorithm_))) {
    SHARE_LOG(WARN, "deserialize encrypt algorithm failed", K(ret), K(data_len), K(pos));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObZoneEncryptMeta)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i64(master_key_version_);
  size += serialization::encoded_length_i64(encrypt_algorithm_);
  return size;
}

void ObZoneEncryptMeta::reset()
{
  version_ = 0;
  master_key_version_ = -1;
  encrypt_algorithm_ = -1;
}

bool ObZoneEncryptMeta::is_valid_except_master_key_version() const
{
  return ObBlockCipher::is_valid_cipher_opmode(static_cast<ObCipherOpMode>(encrypt_algorithm_));
}

bool ObZoneEncryptMeta::is_valid() const
{
  return (master_key_version_ > 0) && is_valid_except_master_key_version();
}

int ObZoneEncryptMeta::assign(const ObZoneEncryptMeta &other)
{
  int ret = OB_SUCCESS;
  version_ = other.version_;
  master_key_version_ = other.master_key_version_;
  encrypt_algorithm_ = other.encrypt_algorithm_;
  return ret;
}

void ObCLogEncryptStatMap::reset_map()
{
  ATOMIC_STORE(&val_, 0);
}

void ObCLogEncryptStatMap::set_map(const int64_t idx)
{
  uint16_t mask = static_cast<uint16_t>(1 << idx);
  uint16_t map = ATOMIC_LOAD(&val_) | mask;
  ATOMIC_STORE(&val_, map);
}

bool ObCLogEncryptStatMap::test_map(const int64_t idx) const
{
  return (ATOMIC_LOAD(&val_) & (1 << idx)) != 0;
}

};
};
