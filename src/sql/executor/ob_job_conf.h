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

#ifndef OCEANBASE_SQL_EXECUTOR_JOB_CONF_
#define OCEANBASE_SQL_EXECUTOR_JOB_CONF_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/ob_phy_table_location.h"

namespace oceanbase {
namespace sql {

typedef common::ObIArray<common::ObNewRange> RangeIArray;
typedef common::ObSEArray<common::ObNewRange, 8> RangeSEArray;

class ObJobConf {
public:
  ObJobConf();
  virtual ~ObJobConf();
  void reset();

  void set_scan_table_id(const uint64_t table_id, const uint64_t index_id);
  int get_scan_table_id(uint64_t& table_id, uint64_t& index_id);

  inline int get_task_split_type() const
  {
    return task_split_type_;
  }
  inline void set_task_split_type(int type)
  {
    task_split_type_ = type;
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObJobConf);

private:
  // Task Split Type
  int task_split_type_;
  // If a table scan Job, we need this info:
  uint64_t table_id_;
  uint64_t index_id_;

  // Shuffle Method
  // Transmit Method
  // Root Job Parameters
  // Parallel Parameters
  // etc...
  //
};

inline void ObJobConf::set_scan_table_id(const uint64_t table_id, const uint64_t index_id)
{
  table_id_ = table_id;
  index_id_ = index_id;
}

inline int ObJobConf::get_scan_table_id(uint64_t& table_id, uint64_t& index_id)
{
  table_id = table_id_;
  index_id = index_id_;
  return common::OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_JOB_CONF_ */
//// end of header file
