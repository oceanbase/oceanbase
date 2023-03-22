// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

#pragma once

#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
}
namespace observer
{
class ObTableLoadExecCtx
{
public:
  ObTableLoadExecCtx() : exec_ctx_(nullptr), allocator_(nullptr) {}
  virtual ~ObTableLoadExecCtx() {};
  virtual int check_status();

public:
  sql::ObExecContext *exec_ctx_;
  common::ObIAllocator *allocator_;
};

}  // namespace observer
}  // namespace oceanbase