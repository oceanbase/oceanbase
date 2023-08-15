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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_bucket.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace table;

int ObTableLoadBucket::init(const ObAddr &leader_addr) {
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (!leader_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", KR(ret), K(leader_addr));
  } else {
    leader_addr_ = leader_addr;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadBucket::add_row(const ObTabletID &tablet_id,
                               const ObTableLoadObjRow &obj_row,
                               int64_t count,
                               int64_t batch_size,
                               bool &flag)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, bucket_add_row_time_us);
  int ret = OB_SUCCESS;
  ObTableLoadTabletObjRow tablet_obj_row;
  tablet_obj_row.tablet_id_ = tablet_id;
  tablet_obj_row.obj_row_ = obj_row;
  flag = false;
  if (OB_FAIL(row_array_.push_back(tablet_obj_row))) {
    LOG_WARN("fail to add row", KR(ret));
  }
  if (OB_SUCC(ret)) {
    flag = (row_array_.count() >= batch_size);
  }
  return ret;
}


}  // namespace observer
}  // namespace oceanbase
