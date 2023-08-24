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

#include "ob_clog_encrypt_info.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/serialization.h"
#include "lib/utility/ob_tracepoint.h"
#ifndef OB_BUILD_TDE_SECURITY

namespace oceanbase
{

using namespace common;
using namespace share;

namespace transaction
{

int ObCLogEncryptInfo::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    TRANS_LOG(WARN, "ObCLogEncryptInfo init twice");
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
  }
  return ret;
}

bool ObCLogEncryptInfo::is_valid() const {
  return is_inited_;
}

void ObCLogEncryptInfo::reset()
{
  is_inited_ = false;
}

void ObCLogEncryptInfo::destroy()
{
}

int ObCLogEncryptInfo::get_encrypt_info(const uint64_t table_id, ObTxEncryptMeta *&meta) const
{
  int ret = OB_SUCCESS;
  return ret;
}

OB_DEF_SERIALIZE(ObCLogEncryptInfo)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  if ((OB_ISNULL(buf)) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument, ", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(ERROR, "serialize uninitialized clog encrypt info");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(encrypt_map_)) {
    size = 0;
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, size))) {
      TRANS_LOG(WARN, "failed to encode count of encrypt info map", K(ret));
    }
  } else {
    size = encrypt_map_->count();
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, size))) {
      TRANS_LOG(WARN, "failed to encode count of encrypt info map", K(ret));
    }
    for (auto it = encrypt_map_->begin(); OB_SUCC(ret) && it != encrypt_map_->end(); ++it) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, it->table_id_))) {
        TRANS_LOG(WARN, "failed to encode encrypt info map table_id", K(ret));
      } else if (OB_FAIL(it->meta_.serialize(buf, buf_len, pos))) {
        TRANS_LOG(WARN, "failed to encode encrypt info map item", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObCLogEncryptInfo)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument, ", K(ret), KP(buf), K(data_len));
  } else if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(ERROR, "deserialize uninitialized clog encrypt info");
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t size = 0;
    uint64_t table_id;
    ObEncryptMeta meta;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &size))) {
      TRANS_LOG(WARN, "failed to decode count of encrypt info map", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t *>(&table_id)))) {
        TRANS_LOG(WARN, "failed to decode encrypt info map table_id", K(ret));
      } else if (OB_FAIL(meta.deserialize(buf, data_len, pos))) {
        TRANS_LOG(WARN, "failed to decode encrypt info map item", K(ret), K(data_len), K(pos), K(table_id));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCLogEncryptInfo)
{
  int64_t size = 0;
  int64_t count = 0;
  if (OB_NOT_NULL(encrypt_map_)) {
    count = encrypt_map_->count();
    for (auto it = encrypt_map_->begin(); it != encrypt_map_->end(); ++it) {
      size += serialization::encoded_length_vi64(it->table_id_);
      size += it->meta_.get_serialize_size();
    }
  }
  size += serialization::encoded_length_vi64(count);
  return size;
}

OB_SERIALIZE_MEMBER(ObSerializeEncryptMeta, master_key_version_, encrypt_algorithm_, master_key_,
                    table_key_, encrypted_table_key_);
OB_SERIALIZE_MEMBER(ObEncryptMetaCache, table_id_, local_index_id_, meta_);

}//transaction
}//oceanbase
#endif
