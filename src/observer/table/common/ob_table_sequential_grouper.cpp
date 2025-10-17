/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX SERVER

#include "ob_table_sequential_grouper.h"
#include "share/table/ob_table.h"

using namespace oceanbase::table;

// template <typename OpType, typename IndexType>
// template <typename Func>
// int GenericSequentialGrouper<OpType, IndexType>::process_groups(Func &&processor)
// {
//   int ret = OB_SUCCESS;
//   for (int i = 0; OB_SUCC(ret) && i < groups_.count(); ++i) {
//     if (OB_FAIL(processor(*groups_.at(i)))) {
//       LOG_WARN("failed to process group", K(ret), KP(groups_.at(i)));
//     }
//   }
//   return ret;
// }

// template <typename OpType, typename IndexType>
// int GenericSequentialGrouper<OpType, IndexType>::create_new_group(ObTableOperationType::Type type)
// {
//   int ret = OB_SUCCESS;
//   OpGroup *new_group = OB_NEWx(OpGroup, &allocator_);
//   if (OB_ISNULL(new_group)) {
//     ret = OB_ALLOCATE_MEMORY_FAILED;
//     LOG_WARN("failed to allocate memory", K(ret));
//   } else {
//     new_group->type_ = type;
//     if (OB_FAIL(groups_.push_back(new_group))) {
//       new_group->~OpGroup();
//       allocator_.free(new_group);
//       LOG_WARN("failed to add group", K(ret), K(groups_));
//     }
//   }
//   return ret;
// }
