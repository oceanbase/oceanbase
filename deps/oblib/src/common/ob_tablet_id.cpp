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

#include "ob_tablet_id.h"

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace common
{
int ObTabletID::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

uint64_t ObTabletID::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&id_, sizeof(id_), hash_val);
  return hash_val;
}

int ObTabletID::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(id_)))) {
    LOG_WARN("serialize tablet ID failed", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

int ObTabletID::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&id_)))) {
    LOG_WARN("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObTabletID::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(id_);
  return size;
}
} // end namespace common
} // end namespace oceanbase
