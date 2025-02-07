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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_CACHE_LOOKUP_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_CACHE_LOOKUP_ITER_H_

#include "sql/das/iter/ob_das_local_lookup_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObDASCacheLookupIterParam : public ObDASLocalLookupIterParam
{
public:
  ObDASCacheLookupIterParam()
    : ObDASLocalLookupIterParam()
  {}
  virtual bool is_valid() const override
  {
    return true;
  }
};

class ObDASScanCtDef;
class ObDASScanRtDef;
class ObDASFuncLookupIter;
class ObDASCacheLookupIter : public ObDASLocalLookupIter
{
public:
  ObDASCacheLookupIter(const ObDASIterType type = ObDASIterType::DAS_ITER_LOCAL_LOOKUP)
    : ObDASLocalLookupIter(type)
  {}
  virtual ~ObDASCacheLookupIter() {}

protected:
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOOKUP_ITER_H_ */
