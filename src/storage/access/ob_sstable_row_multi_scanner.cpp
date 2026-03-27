/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_sstable_row_multi_scanner.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
template<typename PrefetchType>
ObSSTableRowMultiScanner<PrefetchType>::~ObSSTableRowMultiScanner()
{
}

template<typename PrefetchType>
void ObSSTableRowMultiScanner<PrefetchType>::reset()
{
  ObSSTableRowScanner<PrefetchType>::reset();
}

template<typename PrefetchType>
void ObSSTableRowMultiScanner<PrefetchType>::reuse()
{
  ObSSTableRowScanner<PrefetchType>::reuse();
}

// Explicit instantiations.
template class ObSSTableRowMultiScanner<ObIndexTreeMultiPassPrefetcher<>>;
template class ObSSTableRowMultiScanner<ObCOPrefetcher>;

}
}
