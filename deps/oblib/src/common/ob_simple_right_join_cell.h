/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SIMPLE_RIGHT_JOIN_CELL_H_
#define OB_SIMPLE_RIGHT_JOIN_CELL_H_

#include "lib/container/ob_vector.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
namespace common
{
struct ObSimpleRightJoinCell
{
  uint64_t table_id;
  ObRowkey rowkey;
};
template <>
struct ob_vector_traits<ObSimpleRightJoinCell>
{
  typedef ObSimpleRightJoinCell *pointee_type;
  typedef ObSimpleRightJoinCell value_type;
  typedef const ObSimpleRightJoinCell const_value_type;
  typedef value_type *iterator;
  typedef const value_type *const_iterator;
  typedef int32_t difference_type;
};
}
}

#endif //OB_SIMPLE_RIGHT_JOIN_CELL_H_

