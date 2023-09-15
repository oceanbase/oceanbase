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

#include "ob_location_adapter.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ls/ob_ls_status_operator.h"

namespace oceanbase
{
namespace transaction
{

using namespace common;
using namespace share;

ObLocationAdapter::ObLocationAdapter() : is_inited_(false),
    schema_service_(NULL), location_service_(NULL)
{
  reset_statistics();
}

int ObLocationAdapter::init(share::schema::ObMultiVersionSchemaService *schema_service,
    share::ObLocationService *location_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ob location adapter inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(schema_service) || OB_ISNULL(location_service)) {
    TRANS_LOG(WARN, "invalid argument", KP(schema_service), KP(location_service));
    ret = OB_INVALID_ARGUMENT;
  } else {
    schema_service_ = schema_service;
    location_service_ = location_service;
    is_inited_ = true;
    TRANS_LOG(INFO, "ob location cache adapter inited success");
  }

  return ret;
}

void ObLocationAdapter::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    TRANS_LOG(INFO, "ob location cache adapter destroyed");
  }
}

void ObLocationAdapter::reset_statistics()
{
  renew_access_ = 0;
  total_access_ = 0;
  error_count_ = 0;
}

void ObLocationAdapter::statistics()
{
  if (REACH_TIME_INTERVAL(TRANS_ACCESS_STAT_INTERVAL)) {
    TRANS_LOG(INFO, "location adapter statistics",
        K_(renew_access), K_(total_access), K_(error_count),
        "renew_rate", static_cast<float>(renew_access_) / static_cast<float>(total_access_ + 1));
    reset_statistics();
  }
}

int ObLocationAdapter::nonblock_get_leader(const int64_t cluster_id,
                                           const int64_t tenant_id,
                                           const ObLSID &ls_id,
                                           common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  const bool is_sync = false;
#ifdef TRANS_ERROR
  int64_t random = ObRandom::rand(0, 100);
  static int64_t total_alloc_cnt = 0;
  static int64_t random_cnt = 0;
  ++total_alloc_cnt;
  if (0 == random % 50) {
    ret = OB_LS_LOCATION_NOT_EXIST;
    ++random_cnt;
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "get error for random", K(ls_id),
          K(total_alloc_cnt), K(random_cnt));
    }
    return ret;
  }
#endif

  if (!is_inited_) {
    TRANS_LOG(WARN, "ob location adapter not inited");
    ret = OB_NOT_INIT;
  } else if (!ls_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(ls_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = get_leader_(cluster_id, tenant_id, ls_id, leader, is_sync);
  }

  return ret;
}

int ObLocationAdapter::get_leader_(const int64_t cluster_id,
                                   const int64_t tenant_id,
                                   const ObLSID &ls_id,
                                   common::ObAddr &leader,
                                   const bool is_sync)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ob location adapter not inited");
    ret = OB_NOT_INIT;
  } else if (!ls_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(ls_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_sync) {
    bool force_renew = false;
    if (OB_FAIL(location_service_->get_leader(cluster_id, tenant_id, ls_id, force_renew, leader))) {
      TRANS_LOG(WARN, "get leader from locatition cache error", K(ret), K(ls_id));
      force_renew = true;
      if (OB_SUCCESS != (ret = location_service_->get_leader(cluster_id, tenant_id, ls_id, force_renew, leader))) {
        TRANS_LOG(WARN, "get leader from locatition cache error again",
            KR(ret), K(ls_id), K(force_renew));
      }
      renew_access_++;
    }
  } else {
    if (OB_FAIL(location_service_->nonblock_get_leader(cluster_id, tenant_id, ls_id, leader))) {
      TRANS_LOG(DEBUG, "nonblock get leader from locatition cache error", K(ret), K(ls_id));
      int tmp_ret = OB_SUCCESS;
      //异步获取leader失败，暂时不清除location的cache；
      if (OB_SUCCESS != (tmp_ret = location_service_->nonblock_renew(cluster_id, tenant_id, ls_id))) {
        TRANS_LOG(WARN, "nonblock renew from location cache error", "ret", tmp_ret, K(ls_id));
      }
    }
  }
  if (OB_LS_LOCATION_NOT_EXIST == ret) {
    int tmp_ret = OB_SUCCESS;
    ObLSExistState state;
    if (OB_SUCCESS != (tmp_ret = ObLocationService::check_ls_exist(tenant_id, ls_id, state))) {
      TRANS_LOG(WARN, "check if ls exist failed", K(tmp_ret), K(ls_id));
      if (OB_TENANT_NOT_EXIST == tmp_ret) {
        bool has_dropped = false;
        if (OB_TMP_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(tenant_id, has_dropped))) {
          TRANS_LOG(WARN, "failed to check tenant has been dropped", K(tmp_ret), K(tenant_id));
        } else if (has_dropped) {
          ret = OB_TENANT_HAS_BEEN_DROPPED;
        }
      }
    } else if (state.is_deleted()) {
      // rewrite ret
      ret = OB_LS_IS_DELETED;
      TRANS_LOG(INFO, "ls is deleted", K(ret), K(ls_id));
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    if (!leader.is_valid()) {
      TRANS_LOG(WARN, "invalid server", K(ls_id), K(leader));
      ret = OB_ERR_UNEXPECTED;
    }
  }
  // statistics
  ++total_access_;
  if (OB_FAIL(ret)) {
    ++error_count_;
  }
  statistics();

  return ret;
}

int ObLocationAdapter::nonblock_renew(const int64_t cluster_id, const int64_t tenant_id, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ob location adapter not inited");
    ret = OB_NOT_INIT;
  } else if (!ls_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(ls_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = location_service_->nonblock_renew(cluster_id, tenant_id, ls_id))) {
    TRANS_LOG(WARN, "nonblock renew error", KR(ret), K(ls_id));
  } else {
    ++renew_access_;
  }

  return ret;
}

int ObLocationAdapter::nonblock_get(const int64_t cluster_id,
                                    const int64_t tenant_id,
                                    const ObLSID &ls_id,
                                    ObLSLocation &location)
{
  int ret = OB_SUCCESS;
#ifdef TRANS_ERROR
  int64_t random = ObRandom::rand(0, 100);
  static int64_t total_alloc_cnt = 0;
  static int64_t random_cnt = 0;
  ++total_alloc_cnt;
  if (0 == random % 50) {
    ret = OB_LS_LOCATION_NOT_EXIST;
    ++random_cnt;
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "get error for random", K(ls_id),
          K(total_alloc_cnt), K(random_cnt));
    }
    return ret;
  }
#endif
  if (!is_inited_) {
    TRANS_LOG(WARN, "ob location adapter not inited");
    ret = OB_NOT_INIT;
  } else if (!(is_valid_cluster_id(cluster_id) && is_valid_tenant_id(tenant_id) && ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(cluster_id), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(location_service_->nonblock_get(cluster_id, tenant_id, ls_id, location))) {
    TRANS_LOG(WARN, "nonblock get failed", KR(ret), K(cluster_id), K(tenant_id), K(ls_id));
  } else {
    // do nothing
  }
  if (OB_LS_LOCATION_NOT_EXIST == ret) {
    int tmp_ret = OB_SUCCESS;
    ObLSExistState state;
    if (OB_SUCCESS != (tmp_ret = ObLocationService::check_ls_exist(tenant_id, ls_id, state))) {
      TRANS_LOG(WARN, "check if ls exist failed", K(tmp_ret), K(ls_id));
      if (OB_TENANT_NOT_EXIST == tmp_ret) {
        bool has_dropped = false;
        if (OB_TMP_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(tenant_id, has_dropped))) {
          TRANS_LOG(WARN, "failed to check tenant has been dropped", K(tmp_ret), K(tenant_id));
        } else if (has_dropped) {
          ret = OB_TENANT_HAS_BEEN_DROPPED;
        }
      }
    } else if (state.is_deleted()) {
      // rewrite ret
      ret = OB_LS_IS_DELETED;
      TRANS_LOG(INFO, "ls is deleted", K(ret), K(ls_id));
    } else {
      // do nothing
    }
  }
  return ret;
}

} // transaction
} // oceanbase
