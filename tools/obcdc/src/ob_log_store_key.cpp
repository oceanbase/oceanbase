/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_store_key.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"  // databuff_printf

using namespace oceanbase::common;

namespace oceanbase
{
namespace liboblog
{
ObLogStoreKey::ObLogStoreKey()
{
  reset();
}

ObLogStoreKey::~ObLogStoreKey()
{
  reset();
}

void ObLogStoreKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  participant_key_ = NULL;
  log_id_ = OB_INVALID_ID;
  log_offset_ = -1;
}

int ObLogStoreKey::init(const int64_t tenant_id,
    const char *participant_key,
    const uint64_t log_id,
    const int32_t log_offset)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
        || OB_ISNULL(participant_key)
        || OB_UNLIKELY(OB_INVALID_ID == log_id)
        || OB_UNLIKELY(log_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_id_ = tenant_id;
    participant_key_ = participant_key;
    log_id_ = log_id;
    log_offset_ = log_offset;
  }

  return ret;
}

bool ObLogStoreKey::is_valid() const
{
  bool bool_ret = false;

  bool_ret = (NULL != participant_key_)
    && (OB_INVALID_ID != log_id_)
    && (log_offset_ >= 0);

  return bool_ret;
}

int ObLogStoreKey::get_key(std::string &key)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(participant_key_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    key.append(participant_key_);
    key.append("_");
    key.append(std::to_string(log_id_));
    key.append("_");
    key.append(std::to_string(log_offset_));
  }

  return ret;
}

int64_t ObLogStoreKey::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos, "key:%s_", participant_key_);
    (void)common::databuff_printf(buf, buf_len, pos, "%lu_", log_id_);
    (void)common::databuff_printf(buf, buf_len, pos, "%d", log_offset_);
  }

  return pos;
}

}
}
