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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_INTERFACE_MERGE_SORT_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_INTERFACE_MERGE_SORT_H_

#include "common/row/ob_row_iterator.h"

namespace oceanbase {
namespace sql {

class ObIMergeSort {
public:
  virtual int dump_base_run(common::ObOuterRowIterator& row_iterator, bool build_fragment = true) = 0;
  virtual int build_cur_fragment() = 0;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif