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

#define USING_LOG_PREFIX SHARE

#include "share/ls/ob_ls_table_iterator.h"
#include "share/ls/ob_ls_info.h" // ObLSInfo
#include "share/ls/ob_ls_table_operator.h" // ObLSTableOperator

namespace oceanbase
{
namespace share
{
ObLSTableIterator::ObLSTableIterator()
    : inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      inner_idx_(0),
      lst_operator_(NULL),
      inner_ls_infos_(),
      filters_(),
      inner_table_only_(false)
{
}

int ObLSTableIterator::init(
    ObLSTableOperator &lst_operator,
    const uint64_t tenant_id,
    const share::ObLSTable::Mode mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(
             share::ObLSTable::DEFAULT_MODE != mode
             && share::ObLSTable::INNER_TABLE_ONLY_MODE != mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mode", KR(ret), K(mode));
  } else {
    lst_operator_ = &lst_operator;
    tenant_id_ = tenant_id;
    inner_table_only_ = (share::ObLSTable::INNER_TABLE_ONLY_MODE == mode);
    inited_ = true;
  }
  return ret;
}

int ObLSTableIterator::next(ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(inner_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_idx_ can't be smaller than 0", KR(ret), K_(inner_idx));
  } else if (inner_ls_infos_.count() <= 0) {
    if (OB_FAIL(inner_open_())) {
      LOG_WARN("fail to open iterator", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ls_info.reset();
    if (inner_idx_ < inner_ls_infos_.count()) {
      if (OB_FAIL(ls_info.assign(inner_ls_infos_[inner_idx_]))) {
        LOG_WARN("failed to assign ls_info", KR(ret));
      } else if (OB_FAIL(ls_info.filter(filters_))) {
        LOG_WARN("fail to filter replica", KR(ret), K(ls_info));
      } else {
        ++inner_idx_;
      }
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObLSTableIterator::inner_open_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(lst_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(inner_idx_ < 0 || inner_idx_ != inner_ls_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_idx_ should be equal to the count of inner array",
        KR(ret), K_(inner_idx), "inner_ls_infos count", inner_ls_infos_.count());
  } else if (OB_FAIL(lst_operator_->get_by_tenant(
             tenant_id_, inner_table_only_, inner_ls_infos_))) {
    LOG_WARN("fail to get ls infos by tenant",
             KR(ret), K_(tenant_id), K_(inner_table_only));
  }
  return ret;
}

//////////////////ObAllLSTableIterator
int ObAllLSTableIterator::init(ObLSTableOperator &lst_operator,
           schema::ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(tenant_iter_.init(schema_service))) {
    LOG_WARN("failed to init tenant iter", KR(ret));
  } else {
    lst_operator_ = &lst_operator;
    inited_ = true;
  }
  return ret;
}
int ObAllLSTableIterator::next(ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(inner_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_idx_ can't be smaller than 0", KR(ret), K_(inner_idx));
  } else if (inner_idx_ == inner_ls_infos_.count()) {
    if (OB_FAIL(next_tenant_iter_())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next tenant", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ls_info.reset();
    bool get_ls = false;
    do {
      if (inner_idx_ < inner_ls_infos_.count()) {
        get_ls = true;
        if (OB_FAIL(ls_info.assign(inner_ls_infos_[inner_idx_]))) {
          LOG_WARN("failed to assign ls_info", KR(ret));
        } else if (OB_FAIL(ls_info.filter(filters_))) {
          LOG_WARN("fail to filter replica", KR(ret), K(ls_info));
        } else {
          ++inner_idx_;
        }
      } else if (OB_FAIL(next_tenant_iter_())) {
        LOG_WARN("fail to get next tenant", KR(ret));
      }//if all_ls_meta_table is empty in the tenant
    } while (OB_SUCC(ret) && !get_ls);
  }

  return ret;
}

int ObAllLSTableIterator::next_tenant_iter_()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(lst_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(inner_idx_ < 0 || inner_idx_ != inner_ls_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_idx_ should be equal to the count of inner array",
        KR(ret), K_(inner_idx), "inner_ls_infos count", inner_ls_infos_.count());
  } else {
    inner_idx_ = 0;
    inner_ls_infos_.reset();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_iter_.next(tenant_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next tenant", KR(ret), K(tenant_id));
        }
      } else if (is_user_tenant(tenant_id)) {
        //no need check user tenant
      } else {
        break;
      }
    }
  }
  if (OB_ITER_END == ret) {
    //all tenant end
  } else if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lst_operator_->load_all_ls_in_tenant(tenant_id, inner_ls_infos_))) {
    LOG_WARN("fail to get ls infos by tenant", KR(ret), K(tenant_id));
  }
  return ret;
}

//////////////////ObTenantLSTableIterator
int ObTenantLSTableIterator::init(ObLSTableOperator &lst_operator,
    const uint64_t meta_tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(meta_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(meta_tenant_id));
  } else if (is_user_tenant(meta_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user tenant has no meta info", KR(ret), K(meta_tenant_id));
  } else {
    lst_operator_ = &lst_operator;
    tenant_id_ = meta_tenant_id;
    inner_idx_ = 0;
    inner_ls_infos_.reset();
    filters_.reset();
    inited_ = true;
  }
  return ret;
}
int ObTenantLSTableIterator::next(ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(inner_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_idx_ can't be smaller than 0", KR(ret), K_(inner_idx));
  } else if (inner_ls_infos_.count() <= 0) {
    if (OB_ISNULL(lst_operator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lst operator is null", KR(ret));
    } else if (OB_FAIL(lst_operator_->load_all_ls_in_tenant(tenant_id_, inner_ls_infos_))) {
      LOG_WARN("fail to get ls infos by tenant", KR(ret), K(tenant_id_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (inner_idx_ < inner_ls_infos_.count()) {
    if (OB_FAIL(ls_info.assign(inner_ls_infos_[inner_idx_]))) {
      LOG_WARN("failed to assign ls_info", KR(ret));
    } else if (OB_FAIL(ls_info.filter(filters_))) {
      LOG_WARN("fail to filter replica", KR(ret), K(ls_info));
    } else {
      ++inner_idx_;
    }
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}
} // end namespace share
} // end namespace oceanbase
