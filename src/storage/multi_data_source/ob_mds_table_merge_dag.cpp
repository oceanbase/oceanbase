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
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/multi_data_source/ob_mds_table_merge_task.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag_param.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"

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
ObMdsTableMergeDag::ObMdsTableMergeDag()
  : ObTabletMergeDag(ObDagType::DAG_TYPE_MDS_TABLE_MERGE),
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
    if (OB_UNLIKELY(!is_mds_table_merge(mds_param->merge_type_))) {
      ret = OB_ERR_SYS;
      LOG_WARN("param type is not mds table merge type", K(ret), KPC(mds_param));
    } else if (OB_UNLIKELY(!mds_param->flush_scn_.is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("flush scn is invalid", K(ret), KPC(mds_param));
    } else if (OB_FAIL(ObBasicTabletMergeDag::inner_init(*mds_param))) {
      LOG_WARN("failed to init ObTabletMergeDag", K(ret), KPC(mds_param));
    } else {
      flush_scn_ = mds_param->flush_scn_;
      generate_ts_ = mds_param->generate_ts_;
      mds_construct_sequence_ = mds_param->mds_construct_sequence_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObMdsTableMergeDag::create_first_task()
{
  return ObTabletMergeDag::create_first_task<ObMdsTableMergeTask>();
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
