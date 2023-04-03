/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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

