/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  virtual void operator()(const ObMemAttr& attr, int64_t used, const lib::AObject &obj) = 0;
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
  virtual void operator()(const ObMemAttr& attr, int64_t used, const lib::AObject &obj) override;
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