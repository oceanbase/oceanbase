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

#ifndef OB_ALL_VIRTUAL_DIAG_INDEX_SCAN_H_
#define OB_ALL_VIRTUAL_DIAG_INDEX_SCAN_H_

#include "lib/stat/ob_session_stat.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualDiagIndexScan
{
typedef  common::ObSEArray<int64_t, 16> ObIndexArray;
public:
  ObAllVirtualDiagIndexScan() : index_ids_() {}
  virtual ~ObAllVirtualDiagIndexScan() { index_ids_.reset(); }
  int set_index_ids(const common::ObIArray<common::ObNewRange> &ranges);
  inline ObIndexArray &get_index_ids() { return index_ids_; }
private:
  ObIndexArray index_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDiagIndexScan);
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_DIAG_INDEX_SCAN_H_ */
