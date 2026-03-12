/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_profile_iter.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/ob_das_scan_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASProfileIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  ObDASProfileIterParam &profile_param = static_cast<ObDASProfileIterParam &>(param);
  scan_param_ = profile_param.get_scan_param();
  enable_profile_ = profile_param.is_enable_profile();
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", KR(ret), KP(scan_param_));
  }
  return ret;
}

int ObDASProfileIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_NOT_NULL(children_[i])) {
      if (OB_FAIL(children_[i]->reuse())) {
        LOG_WARN("failed to reuse child iter", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObDASProfileIter::inner_release()
{
  return OB_SUCCESS;
}

int ObDASProfileIter::create_partition_profile()
{
  int ret = OB_SUCCESS;
  if (!enable_profile_) {
    partition_profile_ = nullptr;
  } else if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", KR(ret), KP(scan_param_));
  } else if (OB_NOT_NULL(partition_profile_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition profile already created", KR(ret));
  } else if (OB_NOT_NULL(table_scan_profile_)) {
    common::ObOpProfile<common::ObMetric> *hybrid_profile = nullptr;
    if (OB_FAIL(table_scan_profile_->get_or_register_child(
            common::ObProfileId::HYBRID_SEARCH, hybrid_profile))) {
      LOG_WARN("failed to get or register hybrid search profile", KR(ret));
    } else if (OB_FAIL(hybrid_profile->register_child(
            common::ObProfileId::HYBRID_SEARCH_PARTITION, partition_profile_))) {
      LOG_WARN("failed to register partition profile", KR(ret));
    } else {
      common::ObProfileSwitcher switcher(partition_profile_);
      SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, scan_param_->tablet_id_.id());
    }
  }
  return ret;
}

int ObDASProfileIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  table_scan_profile_ = common::get_current_profile();
  if (OB_FAIL(create_partition_profile())) {
    LOG_WARN("failed to create partition profile", KR(ret));
  } else if (OB_ISNULL(children_) || OB_ISNULL(children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(partition_profile_);
    if (OB_FAIL(children_[0]->do_table_scan())) {
      LOG_WARN("failed to do table scan for child", KR(ret));
    }
  }
  return ret;
}

int ObDASProfileIter::rescan()
{
  int ret = OB_SUCCESS;
  partition_profile_ = nullptr;
  if (OB_FAIL(create_partition_profile())) {
    LOG_WARN("failed to create partition profile", KR(ret));
  } else if (OB_ISNULL(children_) || OB_ISNULL(children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(partition_profile_);
    if (OB_FAIL(children_[0]->rescan())) {
      LOG_WARN("failed to rescan for child", KR(ret));
    }
  }
  return ret;
}

int ObDASProfileIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(children_) || OB_ISNULL(children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(partition_profile_);
    if (OB_FAIL(children_[0]->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from child", KR(ret));
      }
    }
  }
  return ret;
}

int ObDASProfileIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(children_) || OB_ISNULL(children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(partition_profile_);
    if (OB_FAIL(children_[0]->get_next_rows(count, capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next rows from child", KR(ret));
      }
    }
  }
  return ret;
}

int ObDASProfileIter::init_runtime_profile(
    common::ObProfileId profile_id,
    common::ObOpProfile<common::ObMetric> *&profile_out)
{
  int ret = OB_SUCCESS;
  profile_out = nullptr;
  common::ObOpProfile<common::ObMetric> *parent_profile = common::get_current_profile();
  if (OB_NOT_NULL(parent_profile)) {
    if (OB_FAIL(parent_profile->register_child(profile_id, profile_out))) {
      LOG_WARN("failed to register profile", KR(ret), K(profile_id));
    }
  }
  return ret;
}

void ObDASProfileIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(children_) && OB_NOT_NULL(children_[0])) {
    children_[0]->clear_evaluated_flag();
  }
}

}  // namespace sql
}  // namespace oceanbase
