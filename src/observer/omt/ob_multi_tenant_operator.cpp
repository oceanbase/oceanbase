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

#define USING_LOG_PREFIX SERVER_OMT
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
namespace omt
{

ObMultiTenantOperator::ObMultiTenantOperator() :
  inited_(false),
  tenant_idx_(-1),
  tenant_(nullptr)
{}

ObMultiTenantOperator::~ObMultiTenantOperator()
{
  reset();
}

void ObMultiTenantOperator::reset()
{
  if (inited_) {
    // 如果迭代未结束，释放残留的资源
    int ret = OB_SUCCESS;
    if (tenant_idx_ >= 0 && tenant_idx_ < tenant_ids_.size()) {
      uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
      if (tenant_ != nullptr) {
        if (tenant_id != tenant_->id()) {
          LOG_ERROR("ObMultiTenantOperator::reset", K(tenant_id), K(tenant_->id()));
          abort();
        }
        {
          share::ObTenantSwitchGuard guard(tenant_);
          release_last_tenant();
        }
        tenant_->unlock(handle_);
      }
    }
    if (OB_FAIL(ret)) {
      // 无法处理资源释放不了的问题
      abort();
    } else {
      tenant_ids_.reuse();
      tenant_idx_ = -1;
      tenant_ = nullptr;
      inited_ = false;
    }
  }
}

int ObMultiTenantOperator::init()
{
  int ret = OB_SUCCESS;
  ObMultiTenant *omt = GCTX.omt_;
  TenantIdList tenant_list;
  if (omt == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("operator init", K(ret));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("operator init", K(ret));
  } else {
    if (tenant_ids_.size() != 0 || tenant_idx_ != -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("operator init", K(ret), K(tenant_idx_), K(tenant_ids_));
    } else if (FALSE_IT(omt->get_tenant_ids(tenant_list))) {
    } else {
      for (int i=0; i < tenant_list.size(); i++) {
        uint64_t tenant_id = tenant_list.at(i);
        if (!is_virtual_tenant_id(tenant_id) && is_need_process(tenant_id)) {
          tenant_ids_.push_back(tenant_id);
        }
      }
      inited_ = true;
    }
  }
  return ret;
}

int ObMultiTenantOperator::execute(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = init();
  }
  if (OB_SUCC(ret)) {
    if (tenant_idx_ == -1) {
      tenant_idx_ = 0;
    }
    while (OB_SUCC(ret)) {
      if (tenant_idx_ >= tenant_ids_.size()) {
        // finish iter
        ret = OB_ITER_END;
      } else {
        uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
        int process_ret = OB_SUCCESS;
        if (tenant_ == nullptr) {
          if (OB_FAIL(GCTX.omt_->get_active_tenant_with_tenant_lock(tenant_id, handle_, tenant_))) {
            LOG_WARN("get_tenant_with_tenant_lock", K(ret), K(tenant_id));
          }
        } else {
          // check iter tenant
          if (tenant_->id() != tenant_id) {
            LOG_ERROR("ObMultiTenantOperator tenant mismatch", K(tenant_ids_), K(tenant_idx_), K(tenant_id), K(tenant_->id()));
            abort();
          }
        }
        if (OB_SUCC(ret)) {
          share::ObTenantSwitchGuard guard(tenant_);
          process_ret = process_curr_tenant(row);
        }
        if (OB_SUCC(ret)) {
          if (process_ret == OB_SUCCESS) {
            // succ do nothing
            break;
          } else if (process_ret == OB_ITER_END) {
            {
              // release last tenant obj
              share::ObTenantSwitchGuard guard(tenant_);
              release_last_tenant();
            }
            tenant_->unlock(handle_);
            tenant_ = nullptr;
            tenant_idx_++;
          } else {
            ret = process_ret;
            LOG_WARN("operator process", K(ret), K(lbt()));
          }
        } else if (ret == OB_TENANT_NOT_IN_SERVER) {
          ret = OB_SUCCESS;
          tenant_idx_++;
        }
      }
    }
  }
  return ret;
}

} // end omt
} // end oceanbase
