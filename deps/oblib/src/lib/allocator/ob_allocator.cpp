/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

using namespace oceanbase;
using namespace lib;
using namespace common;


ObWrapperAllocatorWithAttr::ObWrapperAllocatorWithAttr(ObIAllocator *alloc, ObMemAttr attr = ObMemAttr())
  : ObWrapperAllocator(alloc), mem_attr_(attr) {};