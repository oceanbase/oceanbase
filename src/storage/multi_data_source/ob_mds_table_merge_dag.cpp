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

#include "storage/multi_data_source/ob_mds_table_merge_dag.h"
#include "storage/multi_data_source/ob_mds_table_merge_task.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag_param.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::compaction;

namespace oceanbase
{
namespace storage
{
namespace mds
{
ERRSIM_POINT_DEF(EN_SKIP_MERGE_MDS_TABEL);
ObMdsTableMergeDag::ObMdsTableMergeDag()
  : ObTabletMergeDag(ObDagType::DAG_TYPE_MDS_MINI_MERGE),
    is_inited_(false),
    flush_scn_(),
    generate_ts_(0),
    mds_construct_sequence_(-1)
{
}

int ObMdsTableMergeDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(param));
  } else {
    const ObMdsTableMergeDagParam *mds_param = static_cast<const ObMdsTableMergeDagParam*>(param);
    if (OB_UNLIKELY(!is_mds_mini_merge(mds_param->merge_type_))) {
      ret = OB_ERR_SYS;
      LOG_WARN("param type is not mds table merge type", K(ret), KPC(mds_param));
    } else if (OB_UNLIKELY(!mds_param->flush_scn_.is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("flush scn is invalid", K(ret), KPC(mds_param));
    } else if (OB_FAIL(ObTabletMergeDag::inner_init(mds_param))) {
      LOG_WARN("failed to init ObTabletMergeDag", K(ret), KPC(mds_param));
    } else if (OB_FAIL(fill_compat_mode_())) {
      LOG_WARN("failed to fill compat mode", K(ret), KPC(mds_param));
    } else {
      flush_scn_ = mds_param->flush_scn_;
      generate_ts_ = mds_param->generate_ts_;
      mds_construct_sequence_ = mds_param->mds_construct_sequence_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObMdsTableMergeDag::fill_compat_mode_()
{
  int ret = OB_SUCCESS;
  // Mds dump should not access mds data to avoid potential dead lock
  // between mds table lock on ObTabletPointer and other mds component
  // inner locks. So here use no_lock to get tablet.

  ObLSHandle tmp_ls_handle;
  ObTabletHandle tmp_tablet_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, tmp_ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(tmp_ls_handle.get_ls()->get_tablet_svr()->get_tablet(
      tablet_id_, tmp_tablet_handle, 0/*timeout_us*/, storage::ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    compat_mode_ = tmp_tablet_handle.get_obj()->get_tablet_meta().compat_mode_;
  }

  return ret;
}

int ObMdsTableMergeDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObMdsTableMergeTask *task = nullptr;
  bool need_create_task = true;
#ifdef ERRSIM
      ret = EN_SKIP_MERGE_MDS_TABEL ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        need_create_task = false;
        ret = OB_SUCCESS;
      }
#endif

  if (!need_create_task) {
    FLOG_INFO("skip create mds table merge dag first task");
  } else if (OB_FAIL(create_task(nullptr/*parent*/, task))) {
    STORAGE_LOG(WARN, "fail to alloc mds merge task", K(ret));
  }
  return ret;
}

int ObMdsTableMergeDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls basic tablet merge dag do not init", K(ret));
  } else {
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, ObIDag::get_type(),
        ls_id_.id(),
        static_cast<int64_t>(tablet_id_.id()),
        static_cast<int64_t>(flush_scn_.get_val_for_inner_table_field())))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObMdsTableMergeDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(databuff_printf(buf, buf_len, "mds table merge task, ls_id=%ld, tablet_id=%ld, flush_scn=%ld",
      ls_id_.id(), tablet_id_.id(), flush_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to fill dag key", K(ret), K_(ls_id), K_(tablet_id), K_(flush_scn));
  }

  return ret;
}
} // namespace mds
} // namespace storage
} // namespace oceanbase
