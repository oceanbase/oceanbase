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

#ifndef _OB_MALLOC_CALLBACK_H_
#define _OB_MALLOC_CALLBACK_H_

#include "lib/alloc/alloc_struct.h"
#include "lib/coro/co_var.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace lib
{
class ObMallocCallback
{
public:
  ObMallocCallback() : prev_(this), next_(this) {}
  virtual void operator()(const ObMemAttr& attr, int64_t used) = 0;
  ObMallocCallback *prev() const { return prev_; }
  ObMallocCallback *next() const { return next_; }
  void unlink();
  void insert_before(ObMallocCallback *callback);
private:
  ObMallocCallback *prev_;
  ObMallocCallback *next_;
};

class ObDefaultMallocCallback final : public ObMallocCallback
{
public:
  virtual void operator()(const ObMemAttr& attr, int64_t used) override;
};

RLOCAL_EXTERN(ObMallocCallback *, malloc_callback);

class ObMallocCallbackGuard
{
public:
  ObMallocCallbackGuard(ObMallocCallback& cb);
  ~ObMallocCallbackGuard();
  DISABLE_COPY_ASSIGN(ObMallocCallbackGuard);
private:
  ObMallocCallback& callback_;
};

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OB_MALLOC_CALLBACK_H_ */