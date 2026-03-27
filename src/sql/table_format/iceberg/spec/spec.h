/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SPEC_H
#define SPEC_H

#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{

namespace sql
{

namespace iceberg
{
class SpecWithAllocator
{
public:
  SpecWithAllocator(ObIAllocator &allocator) : allocator_(allocator) {}
  TO_STRING_EMPTY();

protected:
  ObIAllocator &allocator_;
};

} // namespace iceberg

} // namespace sql

} // namespace oceanbase

#endif // SPEC_H
