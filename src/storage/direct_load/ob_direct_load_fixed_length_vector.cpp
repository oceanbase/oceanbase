/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_fixed_length_vector.h"
#include "share/vector/ob_uniform_vector.h"

namespace oceanbase
{
namespace storage
{
template class ObDirectLoadFixedLengthVector<int8_t>;
template class ObDirectLoadFixedLengthVector<int16_t>;
template class ObDirectLoadFixedLengthVector<int32_t>;
template class ObDirectLoadFixedLengthVector<int64_t>;
template class ObDirectLoadFixedLengthVector<int128_t>;
template class ObDirectLoadFixedLengthVector<int256_t>;
template class ObDirectLoadFixedLengthVector<int512_t>;

template class ObDirectLoadFixedLengthVector<uint8_t>;
template class ObDirectLoadFixedLengthVector<uint16_t>;
template class ObDirectLoadFixedLengthVector<uint32_t>;
template class ObDirectLoadFixedLengthVector<uint64_t>;

template class ObDirectLoadFixedLengthVector<float>;
template class ObDirectLoadFixedLengthVector<double>;

template class ObDirectLoadFixedLengthVector<ObOTimestampData>;
template class ObDirectLoadFixedLengthVector<ObOTimestampTinyData>;
template class ObDirectLoadFixedLengthVector<ObIntervalDSValue>;

} // namespace storage
} // namespace oceanbase
