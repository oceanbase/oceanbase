/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/container/ob_ring_buffer.h"

namespace oceanbase {
namespace common {

ObRingBuffer::Node* const ObRingBuffer::Node::LOCKED_ADDR = nullptr;

}  // common
}  // oceanbase
