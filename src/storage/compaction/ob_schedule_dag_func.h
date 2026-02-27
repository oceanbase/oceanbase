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

#ifndef OCEANBASE_STORAGE_COMPACTION_OB_SCHEDULE_DAG_FUNC_H_
#define OCEANBASE_STORAGE_COMPACTION_OB_SCHEDULE_DAG_FUNC_H_
#include "lib/container/ob_iarray.h"
#include "storage/compaction/ob_compaction_util.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace share
{
class ObLSID;
}
namespace storage
{
namespace mds
{
class ObTabletMdsMiniMergeDagParam;
}
struct ObDDLTableMergeDagParam;
struct ObTabletSplitParam;
struct ObLobSplitParam;
class ObTabletSplitDag;
class ObTabletLobSplitDag;
class ObComplementDataDag;
class ObTablet;
}

namespace share
{
class ObTenantDagScheduler;
}
namespace compaction
{
struct ObTabletMergeDagParam;
struct ObCOMergeDagParam;
struct ObTabletSchedulePair;
struct ObBatchFreezeTabletsParam;
#ifdef OB_BUILD_SHARED_STORAGE
struct ObTabletsRefreshSSTableParam;
struct ObVerifyCkmParam;
struct ObUpdateSkipMajorParam;
struct ObTabletSSMinorMergeDagParam;
#endif

class ObScheduleDagFunc final
{
public:
  static int schedule_tablet_merge_dag(
      ObTabletMergeDagParam &param,
      const bool is_emergency = false);
  static int schedule_tx_table_merge_dag(
      ObTabletMergeDagParam &param,
      const bool is_emergency = false);
  static int schedule_ddl_table_merge_dag(
      storage::ObDDLTableMergeDagParam &param,
      const bool is_emergency = false);
  static int schedule_tablet_split_dag(
      storage::ObTabletSplitParam &param,
      const bool is_emergency = false);
  static int schedule_and_get_tablet_split_dag(
      storage::ObTabletSplitParam &param,
      storage::ObTabletSplitDag *&dag,
      const bool is_emergency = false);
  static int schedule_lob_tablet_split_dag(
      storage::ObLobSplitParam &param,
      const bool is_emergency = false);
  static int schedule_tablet_co_merge_dag_net(
      ObCOMergeDagParam &param);
  static int schedule_and_get_lob_tablet_split_dag(
      storage::ObLobSplitParam &param,
      storage::ObTabletLobSplitDag *&dag,
      const bool is_emergency = false);
  static int schedule_mds_table_merge_dag(
      storage::mds::ObTabletMdsMiniMergeDagParam &param,
      const bool is_emergency = false);
  static int schedule_batch_freeze_dag(
    const ObBatchFreezeTabletsParam &freeze_param);
#ifdef OB_BUILD_SHARED_STORAGE
  static int schedule_ss_attach_major_dag(
    ObTabletMergeDagParam &param);
#endif
};

class ObDagParamFunc final
{
public:
  static int fill_param(
    const share::ObLSID &ls_id,
    const storage::ObTablet &tablet,
    const ObMergeType merge_type,
    const int64_t &merge_snapshot_version,
    const ObExecMode exec_mode,
    const share::ObDagId *dag_net_id,
    ObCOMergeDagParam &param);
  static int fill_param(
    const share::ObLSID &ls_id,
    const storage::ObTablet &tablet,
    const ObMergeType merge_type,
    const int64_t &merge_snapshot_version,
    const ObExecMode exec_mode,
    ObTabletMergeDagParam &param);
};

}
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_OB_SCHEDULE_DAG_FUNC_H_ */
