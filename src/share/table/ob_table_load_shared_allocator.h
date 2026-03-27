/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

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
