/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "common/storage/ob_sequence.h"


using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{

int64_t ObSequence::max_seq_no_ = ObClockGenerator::getClock();

} // common
} // oceanbase
