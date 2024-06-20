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
#include "lib/oblog/ob_log_module.h"       // LOG_*
#include "lib/oblog/ob_log.h"              // LOG_*
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"
#include "share/ob_errno.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"

namespace oceanbase
{
using namespace common;
namespace share
{

int ObTenantSnapshotID::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, id_))) {
    LOG_WARN("serialize tenant snapshot id failed", KR(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

int ObTenantSnapshotID::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &id_))) {
    LOG_WARN("deserialize tenant snapshot id failed", KR(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObTenantSnapshotID::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(id_);
  return size;
}

} // end namespace share
} // end namespace oceanbase
