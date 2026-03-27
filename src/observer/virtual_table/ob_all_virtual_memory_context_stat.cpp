/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_all_virtual_memory_context_stat.h"

namespace oceanbase
{
using namespace storage;
using namespace lib;
namespace observer
{
ObAllVirtualMemoryContextStat::ObAllVirtualMemoryContextStat()
{
}

ObAllVirtualMemoryContextStat::~ObAllVirtualMemoryContextStat()
{
  reset();
}

int ObAllVirtualMemoryContextStat::inner_get_next_row(common::ObNewRow *&row)
{
  UNUSEDx(row);
  return OB_ITER_END;
}

void ObAllVirtualMemoryContextStat::reset()
{
  is_inited_ = false;
  ObVirtualTableScannerIterator::reset();
}

} /* namespace observer */
} /* namespace oceanbase */
