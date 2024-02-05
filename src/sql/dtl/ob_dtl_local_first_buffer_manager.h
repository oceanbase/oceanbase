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

#ifndef OB_DTL_LOCAL_FIRST_BUFFER_MANAGER_H
#define OB_DTL_LOCAL_FIRST_BUFFER_MANAGER_H

#include "lib/utility/ob_unify_serialize.h"
#include "lib/atomic/ob_atomic.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_tenant_mem_manager.h"
#include "lib/list/ob_dlist.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/ob_define.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
namespace sql {
namespace dtl {


class ObDtlCacheBufferInfo : public common::ObDLinkBase<ObDtlCacheBufferInfo>
{
public:
  ObDtlCacheBufferInfo() :buffer_(nullptr), chid_(common::OB_INVALID_ID), ts_(0)
  {}

  ObDtlCacheBufferInfo(ObDtlLinkedBuffer *buffer, int64_t ts) :
    buffer_(buffer), chid_(common::OB_INVALID_ID), ts_(ts)
  {}

  void set_buffer(ObDtlLinkedBuffer *buffer);
  ObDtlLinkedBuffer *buffer() { return buffer_; }
  int64_t &ts() { return ts_; }
  int64_t &chid() { return chid_; }
  uint64_t get_key() const { return chid_; }
private:
  ObDtlLinkedBuffer *buffer_;
  int64_t chid_;
  int64_t ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDtlCacheBufferInfo);
};

} // dtl
} // sql
} // oceanbase

#endif /* OB_DTL_LOCAL_FIRST_BUFFER_MANAGER_H */
