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

#include "rpc/ob_request.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace rpc {

char* ObRequest::easy_alloc(int64_t size) const
{
  void* buf = NULL;
  if (OB_ISNULL(ez_req_) || OB_ISNULL(ez_req_->ms) || OB_ISNULL(ez_req_->ms->pool)) {
    RPC_LOG(ERROR, "ez_req_ is not corret");
  } else {
    buf = easy_pool_alloc(ez_req_->ms->pool, static_cast<uint32_t>(size));
  }
  return static_cast<char*>(buf);
}

void ObRequest::on_process_begin()
{
  reusable_mem_.reuse();
}

char* ObRequest::easy_reusable_alloc(int64_t size) const
{
  void* buf = NULL;
  if (OB_ISNULL(ez_req_) || OB_ISNULL(ez_req_->ms) || OB_ISNULL(ez_req_->ms->pool)) {
    RPC_LOG(ERROR, "ez_req_ is not corret");
  } else {
    if (NULL == (buf = reusable_mem_.alloc(size))) {
      buf = easy_pool_alloc(ez_req_->ms->pool, static_cast<uint32_t>(size));
      if (NULL != buf) {
        reusable_mem_.add(buf, size);
      }
    }
  }
  return static_cast<char*>(buf);
}
}  // end of namespace rpc
}  // end of namespace oceanbase
