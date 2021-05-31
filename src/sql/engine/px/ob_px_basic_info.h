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

#ifndef __OCEANBASE_SQL_ENGINE_PX_OB_PX_BASIC_INFO_H__
#define __OCEANBASE_SQL_ENGINE_PX_OB_PX_BASIC_INFO_H__

namespace oceanbase {
namespace sql {

#define IS_PX_COORD(type) ((type) == PHY_PX_FIFO_COORD || (type) == PHY_PX_MERGE_SORT_COORD)

#define IS_PX_RECEIVE(type)                                                                                     \
  ((type) == PHY_PX_FIFO_RECEIVE || (type) == PHY_PX_MERGE_SORT_RECEIVE || (type) == PHY_PX_MERGE_SORT_COORD || \
      (type) == PHY_PX_FIFO_COORD)

#define IS_PX_TRANSMIT(type) \
  ((type) == PHY_PX_DIST_TRANSMIT || (type) == PHY_PX_REPART_TRANSMIT || (type) == PHY_PX_REDUCE_TRANSMIT)

#define IS_PX_GI(type) ((type) == PHY_GRANULE_ITERATOR)

#define IS_PX_MODIFY(type) \
  ((type) == PHY_PX_MULTI_PART_UPDATE || (type) == PHY_PX_MULTI_PART_DELETE || (type) == PHY_PX_MULTI_PART_INSERT)

#define IS_TRANSMIT(type)                                                                                       \
  ((type) == PHY_ROOT_TRANSMIT || (type) == PHY_DIRECT_TRANSMIT || (type) == PHY_DISTRIBUTED_TRANSMIT ||        \
      (type) == PHY_PX_DIST_TRANSMIT || (type) == PHY_PX_REPART_TRANSMIT || (type) == PHY_PX_REDUCE_TRANSMIT || \
      (type) == PHY_DETERMINATE_TASK_TRANSMIT)

#define IS_DIST_TRANSMIT(type)                                                                                        \
  ((type) == PHY_DISTRIBUTED_TRANSMIT || (type) == PHY_DETERMINATE_TASK_TRANSMIT || (type) == PHY_PX_DIST_TRANSMIT || \
      (type) == PHY_PX_REPART_TRANSMIT || (type) == PHY_PX_REDUCE_TRANSMIT)

#define IS_PX_TRANSMIT(type) \
  ((type) == PHY_PX_DIST_TRANSMIT || (type) == PHY_PX_REPART_TRANSMIT || (type) == PHY_PX_REDUCE_TRANSMIT)

#define IS_DML(type)                                                                                                 \
  ((type) == PHY_INSERT || (type) == PHY_INSERT_ON_DUP || (type) == PHY_INSERT_RETURNING ||                          \
      (type) == PHY_INSERT_ON_DUP_RETURNING || (type) == PHY_DELETE || (type) == PHY_DELETE_RETURNING ||             \
      (type) == PHY_UPDATE || (type) == PHY_UPDATE_RETURNING || (type) == PHY_REPLACE ||                             \
      (type) == PHY_REPLACE_RETURNING || (type) == PHY_PX_MULTI_PART_DELETE || (type) == PHY_PX_MULTI_PART_INSERT || \
      (type) == PHY_PX_MULTI_PART_UPDATE || (type) == PHY_MERGE || (type) == PHY_LOCK)

}  // namespace sql
}  // namespace oceanbase
#endif /* __OCEANBASE_SQL_ENGINE_PX_OB_PX_BASIC_INFO_H__ */
