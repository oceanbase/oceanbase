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
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/multi_data_source/ob_mds_table_merge_task.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
namespace mds
{
ObMdsTableMergeDag::ObMdsTableMergeDag()
  : ObIDag(ObDagType::DAG_TYPE_MDS_TABLE_MERGE),
    is_inited_(false)
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
    const share::ObLSID &ls_id = mds_param->ls_id_;
    const common::ObTabletID &tablet_id = mds_param->tablet_id_;
    ObLSService *ls_service = MTL(ObLSService*);
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;

    if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
    } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), K(tablet_id), K(tablet_handle));
    } else {
      compat_mode_ = tablet->get_tablet_meta().compat_mode_;
      param_ = *mds_param;
      is_inited_ = true;
    }
  }

  return ret;
}

bool ObMdsTableMergeDag::operator==(const share::ObIDag &other) const
{
  bool is_same = true;

  if (this == &other) {
    is_same = true;
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMdsTableMergeDag &other_dag = static_cast<const ObMdsTableMergeDag&>(other);
    is_same = (param_.ls_id_ == other_dag.param_.ls_id_ && param_.tablet_id_ == other_dag.param_.tablet_id_);
  }

  return is_same;
}

int64_t ObMdsTableMergeDag::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&param_.ls_id_, sizeof(share::ObLSID), hash_val);
  hash_val = common::murmurhash(&param_.tablet_id_, sizeof(common::ObTabletID), hash_val);
  return hash_val;
}

int ObMdsTableMergeDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObMdsTableMergeTask *task = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K_(is_inited));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(param_))) {
    LOG_WARN("failed to ini task", K(ret), K_(param));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  }

  return ret;
}

int ObMdsTableMergeDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table merge dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                param_.ls_id_.id(),
                                static_cast<int64_t>(param_.tablet_id_.id()),
                                static_cast<int64_t>(param_.flush_scn_.get_val_for_inner_table_field())))){
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObMdsTableMergeDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(databuff_printf(buf, buf_len, "mds table merge task, ls_id=%ld, tablet_id=%ld, flush_scn=%ld",
      param_.ls_id_.id(), param_.tablet_id_.id(), param_.flush_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to fill dag key", K(ret), K_(param));
  }

  return ret;
}

bool ObMdsTableMergeDag::ignore_warning()
{
  return OB_LS_NOT_EXIST == dag_ret_
      || OB_TABLET_NOT_EXIST == dag_ret_
      || OB_CANCELED == dag_ret_;
}
} // namespace mds
} // namespace storage
} // namespace oceanbase
