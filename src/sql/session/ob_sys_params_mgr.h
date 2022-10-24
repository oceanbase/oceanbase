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

#ifndef OCEANBASE_SQL_SYS_PARAMS_MGR_H_
#define OCEANBASE_SQL_SYS_PARAMS_MGR_H_

#include "lib/list/ob_list.h"
#include "sql/engine/ob_physical_plan.h"

namespace oceanbase
{
namespace sql
{
const char *STR_SYS_PAREAMS = "sys_params";
const char *STR_SORT_MEM_SIZE_LIMIT = "sort_mem_size_limit";
const char *STR_GROUP_MEM_SIZE_LIMIT = "group_mem_size_limit";
// FIX ME,
// 1. MIN value
static const int64_t MIN_SORT_MEM_SIZE_LIMIT = 10000; // 10M
static const int64_t MIN_GROUP_MEM_SIZE_LIMIT = 10000; // 10M
// 2. MAX value, -1 means no limit

class ObSysParamsMgr
{
public:
  ObSysParamsMgr();
  virtual ~ObSysParamsMgr();

  void set_sort_mem_size_limit(const int64_t size);
  void set_gorup_mem_size_limit(const int64_t size);

  int parse_from_file(const char *file_name);

  int64_t get_sort_mem_size_limit() const;
  int64_t get_group_mem_size_limit() const;

private:
  int64_t sort_mem_size_limit_;
  int64_t group_mem_size_limit_;
  DISALLOW_COPY_AND_ASSIGN(ObSysParamsMgr);
};

inline int64_t ObSysParamsMgr::get_sort_mem_size_limit() const
{
  return sort_mem_size_limit_;
}

inline int64_t ObSysParamsMgr::get_group_mem_size_limit() const
{
  return group_mem_size_limit_;
}

void ObSysParamsMgr::set_sort_mem_size_limit(const int64_t size)
{
  if (size >= MIN_SORT_MEM_SIZE_LIMIT) {
    sort_mem_size_limit_ = size;
  } else if (size < 0) {
    sort_mem_size_limit_ = -1;
  } else {
    ; // lease than MIN value, no change
  }
}

void ObSysParamsMgr::set_gorup_mem_size_limit(const int64_t size)
{
  if (size >= MIN_GROUP_MEM_SIZE_LIMIT) {
    group_mem_size_limit_ = size;
  } else if (size < 0) {
    group_mem_size_limit_ = -1;
  } else {
    ; // lease than MIN value, no change
  }
}
} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_SYS_PARAMS_MGR_H_ */



