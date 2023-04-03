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

#include "ob_malloc_callback.h"

namespace oceanbase
{
namespace lib
{
void ObDefaultMallocCallback::operator()(const ObMemAttr& attr, int64_t used)
{
  UNUSED(attr);
  UNUSED(used);
}

_RLOCAL(ObMallocCallback *, malloc_callback);

void ObMallocCallback::unlink()
{
  prev_->next_ = next_;
  next_->prev_ = prev_;
  prev_ = this;
  next_ = this;
}

void ObMallocCallback::insert_before(ObMallocCallback *callback)
{
  prev_ = callback->prev_;
  next_ = callback;
  prev_->next_ = this;
  next_->prev_ = this;
}

ObMallocCallbackGuard::ObMallocCallbackGuard(ObMallocCallback& cb)
    : callback_(cb)
{
  if (OB_ISNULL(malloc_callback)) {
    malloc_callback = &callback_;
  } else {
    callback_.insert_before(malloc_callback);
  }
}

ObMallocCallbackGuard::~ObMallocCallbackGuard()
{
  if (callback_.next() == &callback_) {
    malloc_callback = nullptr;
  } else {
    callback_.unlink();
  }
}

}
}