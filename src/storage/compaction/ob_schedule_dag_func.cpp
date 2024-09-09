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
#include "ob_schedule_dag_func.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/column_store/ob_co_merge_dag.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag_param.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction/ob_tablet_refresh_dag.h"
#include "storage/compaction/ob_verify_ckm_dag.h"
#include "storage/compaction/ob_update_skip_major_tablet_dag.h"
#include "storage/compaction/ob_batch_freeze_tablets_dag.h"
#include "lib/utility/ob_sort.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace share;

namespace compaction
{

#define CREATE_DAG(T)                                                          \
  if (OB_FAIL(MTL(ObTenantDagScheduler *)                                      \
                  ->create_and_add_dag<T>(&param, is_emergency))) {            \
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {                         \
      LOG_WARN("failed to create merge dag", K(ret), K(param));                \
    } else if (OB_EAGAIN == ret) {                                             \
      LOG_WARN("exists same dag, wait the dag to finish", K(ret), K(param));   \
    }                                                                          \
  } else {                                                                     \
    LOG_DEBUG("success to schedule tablet merge dag", K(ret), K(param));       \
  }

int ObScheduleDagFunc::schedule_tx_table_merge_dag(
    ObTabletMergeDagParam &param,
    const bool is_emergency)
{
  int ret = OB_SUCCESS;
  CREATE_DAG(ObTxTableMergeDag);
  return ret;
}

int ObScheduleDagFunc::schedule_tablet_co_merge_dag_net(
    ObCOMergeDagParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObTenantDagScheduler*)->create_and_add_dag_net<ObCOMergeDagNet>(&param))) {
    if (OB_TASK_EXIST != ret) {
      LOG_WARN("failed to create dag_net", K(ret), K(param));
    } else {
      ret = OB_SUCCESS; // ignore OB_TASK_EXIST
    }
  } else {
    FLOG_INFO("success to create co merge dag_net", K(ret), K(param));
  }
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

int ObScheduleDagFunc::schedule_batch_freeze_dag(
    const ObBatchFreezeTabletsParam &param)
{
  int ret = OB_SUCCESS;
  bool is_emergency = true;
  if (param.tablet_info_array_.empty()) {
    // do nothing
  } else {
    CREATE_DAG(ObBatchFreezeTabletsDag);
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObScheduleDagFunc::schedule_tablet_refresh_dag(
    ObTabletsRefreshSSTableParam &param,
    const bool is_emergency)
{
  int ret = OB_SUCCESS;
  CREATE_DAG(ObTabletsRefreshSSTableDag);
  return ret;
}

int ObScheduleDagFunc::schedule_verify_ckm_dag(ObVerifyCkmParam &param)
{
  int ret = OB_SUCCESS;
  bool is_emergency = true;
  if (param.tablet_info_array_.empty()) {
    // do nothing
  } else {
    lib::ob_sort(param.tablet_info_array_.begin(), param.tablet_info_array_.end());
    CREATE_DAG(ObVerifyCkmDag);
  }

  if (OB_FAIL(ret)) {
    ADD_SUSPECT_LS_INFO(MAJOR_MERGE,
                        ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                        param.ls_id_,
                        ObSuspectInfoType::SUSPECT_LS_SCHEDULE_DAG,
                        param.compaction_scn_,
                        (int64_t) ObDagType::DAG_TYPE_VERIFY_CKM,
                        (int64_t) ret /*error_code*/);
  }
  return ret;
}

int ObScheduleDagFunc::schedule_update_skip_major_tablet_dag(
    const ObUpdateSkipMajorParam &param)
{
  int ret = OB_SUCCESS;
  bool is_emergency = false;
  if (param.tablet_info_array_.empty()) {
    // do nothing
  } else {
    CREATE_DAG(ObUpdateSkipMajorTabletDag);
  }
  return ret;
}

#endif

} // namespace compaction
} // namespace oceanbase
