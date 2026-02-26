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

#define USING_LOG_PREFIX PALF
#include "log_io_context.h"

namespace oceanbase
{
namespace palf
{
LogIOContext::LogIOContext()
    : palf_id_(0),
      user_()
{
}

LogIOContext::LogIOContext(const LogIOUser &user)
    : palf_id_(0),
      user_(user)
{
  const bool allow_filling_cache = is_enable_fill_cache_user_();
  iterator_info_.set_allow_filling_cache(allow_filling_cache);
}

LogIOContext::LogIOContext(const uint64_t tenant_id, const int64_t palf_id, const LogIOUser &user)
    : palf_id_(palf_id),
      user_(user)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    // it's not allowed to fill cache when reading for FETCHLOG, RESTART, META_INFO
    const bool allow_filling_cache = is_enable_fill_cache_user_();
    iterator_info_.set_allow_filling_cache(allow_filling_cache);
  }
}

} // namespace palf
} // namespace oceanbase
