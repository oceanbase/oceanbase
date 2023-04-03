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

#ifndef OB_DTL_BUF_ALLOCATOR_H_
#define OB_DTL_BUF_ALLOCATOR_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_define.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlLinkedBuffer;
class ObDtlBasicChannel;

class ObDtlBufIAllocator
{
public:
  virtual ObDtlLinkedBuffer *alloc_buf(ObDtlBasicChannel &ch, const int64_t payload_size) = 0;
  virtual void free_buf(ObDtlBasicChannel &ch, ObDtlLinkedBuffer *&buf) = 0;
};

class ObDtlBufAllocator : public ObDtlBufIAllocator
{
public:
  ObDtlBufAllocator(int64_t tenant_id = common::OB_SERVER_TENANT_ID) : alloc_buffer_cnt_(0), free_buffer_cnt_(0),
    tenant_id_(tenant_id), sys_buffer_size_(0), timeout_ts_(0) {};
  virtual ~ObDtlBufAllocator() = default;
  virtual ObDtlLinkedBuffer *alloc_buf(ObDtlBasicChannel &ch, const int64_t payload_size);
  virtual void free_buf(ObDtlBasicChannel &ch, ObDtlLinkedBuffer *&buf);
  void set_tenant_id(int64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_sys_buffer_size(int64_t sys_buffer_size) { sys_buffer_size_ = sys_buffer_size; }
  void set_timeout_ts(int64_t timeout_ts) { timeout_ts_ = timeout_ts; }
private:
  int64_t alloc_buffer_cnt_;
  int64_t free_buffer_cnt_;
  int64_t tenant_id_;
  int64_t sys_buffer_size_;
  int64_t timeout_ts_;
};

}// end of namespace dtl
}// end of namespace sql
}// end of namespace oceanbase

#endif
