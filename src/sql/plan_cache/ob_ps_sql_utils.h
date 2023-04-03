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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PS_SQL_UTILS_H_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PS_SQL_UTILS_H_

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "common/data_buffer.h"
#include "sql/parser/parse_node.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_bit_set.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObString;
}
namespace sql
{
class ObPsSqlUtils
{
public:
  template<typename T>
  static int alloc_new_var(common::ObIAllocator &allocator, const T &t, T *&new_t);

  static int deep_copy_str(common::ObIAllocator &allocator,
                           const common::ObString &src,
                           common::ObString &dst);
  
  // 获取某个ObPsStmtItem/ObPsStmtInfo对象占用内存
  template<typename T>
  static int get_var_mem_total(const T &t, int64_t &size);
};

template<typename T>
int ObPsSqlUtils::alloc_new_var(common::ObIAllocator &allocator, const T &t, T *&new_t)
{
  int ret = common::OB_SUCCESS;
  new_t = NULL;
  common::ObDataBuffer *data_buf = NULL;
  int64_t cv_size = 0;
  if (OB_FAIL(t.get_convert_size(cv_size))) {
    SQL_PC_LOG(WARN, "get_convert_size failed", K(ret));
  } else {
    const int64_t size = cv_size + sizeof(common::ObDataBuffer);
    char *buf = static_cast<char *>(allocator.alloc(size));
    if (OB_ISNULL(buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SQL_PC_LOG(WARN, "failed to alloc memory");
    } else {
      data_buf = new (buf + sizeof(T)) common::ObDataBuffer(buf + sizeof(T) + sizeof(common::ObDataBuffer),
                                                            size - sizeof(T) - sizeof(common::ObDataBuffer));
      new_t = new (buf) T(data_buf, &allocator);
      if (OB_FAIL(new_t->deep_copy(t))) {
        SQL_PC_LOG(WARN, "deep copy failed", K(ret), K(cv_size));
      }
    }
  }
  return ret;
}

template<typename T>
int ObPsSqlUtils::get_var_mem_total(const T &t, int64_t &size)
{
  int ret = common::OB_SUCCESS;
  size = 0;
  if (OB_FAIL(t.get_convert_size(size))) {
    SQL_PC_LOG(WARN, "get_convert_size failed", K(ret));
  } else {
    size += sizeof(common::ObDataBuffer);
  }
  return ret;
}

} //end of sql
} //end of oceanbase


#endif //OCEANBASE_SQL_PLAN_CACHE_OB_PS_SQL_UTILS_H_
