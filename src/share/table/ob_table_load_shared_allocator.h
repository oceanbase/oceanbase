// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

#pragma once

#include "lib/allocator/page_arena.h"
#include "share/table/ob_table_load_handle.h"

namespace oceanbase
{
namespace table
{

typedef ObTableLoadHandle<common::ObArenaAllocator> ObTableLoadSharedAllocatorHandle;

} // namespace table
} // namespace oceanbase
