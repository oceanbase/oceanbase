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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_schedule_dag_func.h"
#include "lib/oblog/ob_log_module.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_tx_table_merge_task.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag_param.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace compaction
{

#define CREATE_DAG(T) \
  { \
    if (OB_FAIL(MTL(ObTenantDagScheduler*)->create_and_add_dag<T>(&param, is_emergency))) {  \
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) { \
        LOG_WARN("failed to create merge dag", K(ret), K(param)); \
      } \
    } else { \
      LOG_DEBUG("success to schedule tablet merge dag", K(ret), K(param)); \
    } \
  }

int ObScheduleDagFunc::schedule_tx_table_merge_dag(
    ObTabletMergeDagParam &param,
    const bool is_emergency)
{
  int ret = OB_SUCCESS;
  CREATE_DAG(ObTxTableMergeDag);
  return ret;
}

int ObScheduleDagFunc::schedule_tablet_merge_dag(
    ObTabletMergeDagParam &param,
    const bool is_emergency)
{
  int ret = OB_SUCCESS;
  if (is_major_merge_type(param.merge_type_)) {
    CREATE_DAG(ObTabletMajorMergeDag);
  } else if (MINI_MERGE == param.merge_type_) {
    CREATE_DAG(ObTabletMiniMergeDag);
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObScheduleDagFunc::schedule_ddl_table_merge_dag(
    ObDDLTableMergeDagParam &param,
    const bool is_emergency)
{
  int ret = OB_SUCCESS;
  CREATE_DAG(ObDDLTableMergeDag);
  return ret;
}

int ObScheduleDagFunc::schedule_mds_table_merge_dag(
    storage::mds::ObMdsTableMergeDagParam &param,
    const bool is_emergency)
{
  int ret = OB_SUCCESS;
  CREATE_DAG(storage::mds::ObMdsTableMergeDag);
  return ret;
}

} // namespace compaction
} // namespace oceanbase
