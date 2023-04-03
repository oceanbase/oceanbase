// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

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
  OB_TABLE_LOAD_STATISTICS_TIME_COST(bucket_add_row_time_us);
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
