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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_MVI_LOOKUP_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_MVI_LOOKUP_ITER_H_

// #include "storage/access/ob_dml_param.h"
#include "sql/das/iter/ob_das_local_lookup_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

class ObDASScanCtDef;
class ObDASScanRtDef;
class ObDASMVILookupIter : public ObDASLocalLookupIter
{
public:
  ObDASMVILookupIter(): ObDASLocalLookupIter(ObDASIterType::DAS_ITER_MVI_LOOKUP) {}
  virtual ~ObDASMVILookupIter() {}

protected:
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int do_index_lookup() override;

private:
  bool check_has_rowkey();
  int save_rowkey();
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_MVI_ITER_H_ */
