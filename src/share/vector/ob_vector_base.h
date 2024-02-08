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

#ifndef OCEANBASE_SHARE_VECTOR_OB_VECTOR_BASE_H_
#define OCEANBASE_SHARE_VECTOR_OB_VECTOR_BASE_H_

#include "share/vector/ob_i_vector.h"

namespace oceanbase
{
namespace common
{

class ObVectorBase : public ObIVector
{
public:
  ObVectorBase() : max_row_cnt_(INT64_MAX) {}

  void set_max_row_cnt(int64_t max_row_cnt) { max_row_cnt_ = max_row_cnt; }
  int64_t get_max_row_cnt() const { return max_row_cnt_; }
protected:
  int64_t max_row_cnt_;
};

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_VECTOR_BASE_H_
