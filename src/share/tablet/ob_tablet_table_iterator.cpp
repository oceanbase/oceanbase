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

#include "share/tablet/ob_tablet_table_iterator.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace schema;


ObTabletMetaIterator::ObTabletMetaIterator()
  : is_inited_(false),
    prefetch_tablet_idx_(0),
    tenant_id_(OB_INVALID_TENANT_ID)
{}

void ObTabletMetaIterator::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  prefetch_tablet_idx_ = -1;
  prefetched_tablets_.reset();
}

int ObTabletMetaIterator::inner_init(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSTabletMetaIterator init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id invalid", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}

int ObTabletMetaIterator::next(ObTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (prefetch_tablet_idx_ == -1) {
    ret = OB_ITER_END;
  } else {
    bool find = false;
    while (OB_SUCC(ret) && !find) {
      if (prefetch_tablet_idx_ < prefetched_tablets_.count()) {
        // directly get from prefetched tablet_info
        tablet_info.reset();
        if (OB_FAIL(tablet_info.assign(prefetched_tablets_.at(prefetch_tablet_idx_)))) {
          LOG_WARN("fail to assign tablet_info", KR(ret), K_(prefetch_tablet_idx));
        } else if (tablet_info.replica_count() > 0) {
          //
          if (OB_FAIL(tablet_info.filter(filters_))) {
            LOG_WARN("fail to filter tablet_info", KR(ret), K(tablet_info));
          } else {
            find = true;
          }
        }
        ++prefetch_tablet_idx_;
      } else if (OB_FAIL(prefetch())) { // need to prefetch a batch of tablet_info
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to prefetch", KR(ret), K_(tenant_id), K_(prefetch_tablet_idx));
        }
        prefetch_tablet_idx_ = -1;
      } else {
        prefetch_tablet_idx_ = 0;
      }
    }
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObCompactionTabletMetaIterator-------------------------------------------------------------------
 */
ObCompactionTabletMetaIterator::ObCompactionTabletMetaIterator(
  const bool first_check, const int64_t compaction_scn)
  : ObTabletMetaIterator(),
    first_check_(first_check),
    compaction_scn_(compaction_scn),
    batch_size_(TABLET_META_TABLE_RANGE_GET_SIZE),
    end_tablet_id_()
  {}

void ObCompactionTabletMetaIterator::reset()
{
  ObTabletMetaIterator::reset();
  first_check_ = false;
  compaction_scn_ = 0;
  end_tablet_id_.reset();
  batch_size_ = 0;
}

int ObCompactionTabletMetaIterator::next(ObTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  do {
    if (OB_FAIL(ObTabletMetaIterator::next(tablet_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next tablet info", KR(ret));
      }
    } else if (!tablet_info.is_valid()) {
      if (tablet_info.get_replicas().empty()) {
        // ObTabletMetaIterator::next may fillter some replica members and make tablet_info invalid, skip and fetch next one
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet_info is invalid", KR(ret), K(tablet_info));
      }
    }
  } while (OB_SUCC(ret) && !tablet_info.is_valid());
  return ret;
}

int ObCompactionTabletMetaIterator::init(
    const uint64_t tenant_id,
    const int64_t batch_size,
    share::ObIServerTrace &server_trace)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(batch_size));
  } else if (OB_FAIL(ObTabletMetaIterator::inner_init(tenant_id))) {
    LOG_WARN("failed to init", KR(ret), K(tenant_id));
  // Keep set_filter_not_exist_server before setting all the other filters,
  // otherwise the other filters may return OB_ENTRY_NOT_EXIST error code.
  } else if (OB_FAIL(filters_.set_filter_not_exist_server(server_trace))) {
    LOG_WARN("fail to set not exist server filter", KR(ret), K(tenant_id));
  } else if (OB_FAIL(filters_.set_filter_permanent_offline(server_trace))) {
    LOG_WARN("fail to set filter", KR(ret), K(tenant_id));
  } else {
    batch_size_ = batch_size;
    is_inited_ = true;
  }
  return ret;
}

int ObCompactionTabletMetaIterator::prefetch()
{
  int ret = OB_SUCCESS;
  if (prefetch_tablet_idx_ >= prefetched_tablets_.count()) {
    ObTabletID tmp_last_tablet_id;
    if (OB_FAIL(ObTabletMetaTableCompactionOperator::range_scan_for_compaction(
        tenant_id_,
        compaction_scn_,
        end_tablet_id_,
        batch_size_,
        !first_check_/*add_report_scn_filter*/,
        tmp_last_tablet_id,
        prefetched_tablets_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to range get by operator", KR(ret),
            K_(tenant_id), K_(end_tablet_id), K_(batch_size), K_(prefetched_tablets));
      } else {
        prefetch_tablet_idx_ = -1;
      }
    } else if (prefetched_tablets_.count() <= 0) {
      prefetch_tablet_idx_ = -1;
      ret = OB_ITER_END;
    } else {
      end_tablet_id_ = tmp_last_tablet_id;
      prefetch_tablet_idx_ = 0;
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////////////

ObTenantTabletMetaIterator::ObTenantTabletMetaIterator()
    : ObTabletMetaIterator(),
      first_prefetch_(true),
      sql_proxy_(nullptr),
      valid_tablet_ls_pairs_idx_(0)
{
}

ObTenantTabletMetaIterator::~ObTenantTabletMetaIterator()
{
}

int ObTenantTabletMetaIterator::init(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletMetaIterator::inner_init(tenant_id))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_table_operator_.init(sql_proxy))) {
    LOG_WARN("fail to init tablet table operator", KR(ret), K(tenant_id));
  } else {
    sql_proxy_ = &sql_proxy;
    valid_tablet_ls_pairs_.reuse();
    valid_tablet_ls_pairs_idx_ = 0;
    is_inited_ = true;
    if (OB_FAIL(prefetch())) { // need to prefetch a batch of tablet_info
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to prefetch", KR(ret), K_(tenant_id), K_(prefetch_tablet_idx));
      }
    }
  }
  return ret;
}

void ObTenantTabletMetaIterator::reset()
{
  ObTabletMetaIterator::reset();
  first_prefetch_ = true;
  sql_proxy_ = nullptr;
  valid_tablet_ls_pairs_idx_ = -1;
  valid_tablet_ls_pairs_.reset();
  tablet_table_operator_.reset();
}

int ObTenantTabletMetaIterator::prefetch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prefetch_valid_tablet_ids())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to prefetch valid tablet ids", KR(ret), K_(tenant_id),
        K_(valid_tablet_ls_pairs_idx), K_(valid_tablet_ls_pairs));
    }
  } else if (OB_FAIL(prefetch_tablets())) {
    LOG_WARN("fail to prefetch tablets", KR(ret), K_(tenant_id),
      K_(prefetch_tablet_idx), "prefetch count", prefetched_tablets_.count());
  }
  return ret;
}

int ObTenantTabletMetaIterator::prefetch_valid_tablet_ids()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(prefetch_tablet_idx_ != prefetched_tablets_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefetched valid tablet ids have not been iterated to end",
             KR(ret), K_(valid_tablet_ls_pairs_idx), "tablet ls pair count",
             valid_tablet_ls_pairs_.count());
  } else {
    // for any tenant, firstly, get its sys tables' tablet_ids, then get user
    // tables' tablet_ids if it has user tables.
    if (first_prefetch_) {
      if (OB_FAIL(prefetch_sys_table_tablet_ids())) {
        LOG_WARN("fail to prefetch sys tables' tablet_id", KR(ret), K_(tenant_id));
      } else {
        first_prefetch_ = false;
      }
    } else if (OB_FAIL(prefetch_user_table_tablet_ids())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to prefetch user tables' tablet_id", KR(ret), K_(tenant_id));
      }
    }
  }
  return ret;
}

int ObTenantTabletMetaIterator::prefetch_tablets()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(prefetch_tablet_idx_ != prefetched_tablets_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefetched tablet infos have not been iterated to end",
             KR(ret), K_(prefetch_tablet_idx),
             "prefetched tablet count", prefetched_tablets_.count());
  } else {
    prefetch_tablet_idx_ = 0;
    prefetched_tablets_.reuse();

    if (OB_FAIL(tablet_table_operator_.batch_get(tenant_id_,
                                                 valid_tablet_ls_pairs_,
                                                 prefetched_tablets_))) {
      LOG_WARN("fail to do batch_get through tablet_table_operator", KR(ret),
               K_(tenant_id), K_(valid_tablet_ls_pairs), K_(prefetched_tablets));
    }
  }
  return ret;
}

int ObTenantTabletMetaIterator::prefetch_sys_table_tablet_ids()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id_, table_schemas))) {
    LOG_WARN("fail to get tenant table schemas", KR(ret), K_(tenant_id));
  } else {
    ObTabletLSPair pair;
    for (int64_t i = 0; (i < table_schemas.count()) && OB_SUCC(ret); ++i) {
      const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
      if (OB_ISNULL(simple_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err, table schema is null", KR(ret), K_(tenant_id));
      } else {
        const uint64_t tid = simple_schema->get_table_id();
        if (is_sys_table(tid)) {
          // sys-table only has one tablet in 4.0 version
          const ObTabletID &tablet_id = simple_schema->get_tablet_id();
          pair.reset();
          if (OB_FAIL(pair.init(tablet_id, SYS_LS))) {
            LOG_WARN("fail to init tablet_ls_pair", KR(ret), K(tablet_id));
          } else if (OB_FAIL(valid_tablet_ls_pairs_.push_back(pair))) {
            LOG_WARN("fail to push back tablet_ls_pair", KR(ret), K(pair));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantTabletMetaIterator::prefetch_user_table_tablet_ids()
{
  int ret = OB_SUCCESS;
  ObTabletID start_tablet_id;
  if (valid_tablet_ls_pairs_.count() <= 0) {
    // start with default invalid ObTabletLSPair(0,-1) in the first time
  } else {
    const int64_t last_idx = valid_tablet_ls_pairs_.count() - 1;
    start_tablet_id = valid_tablet_ls_pairs_.at(last_idx).get_tablet_id();
  }
  const int64_t range_count = GCONF.tablet_meta_table_scan_batch_count;
  valid_tablet_ls_pairs_.reuse();
  if (OB_FAIL(ObTabletToLSTableOperator::range_get_tablet(
                                              *sql_proxy_,
                                              tenant_id_,
                                              start_tablet_id,
                                              range_count,
                                              valid_tablet_ls_pairs_))) {
    LOG_WARN("fail to get a range of tablet through tablet_to_ls_table_operator",
              KR(ret), K_(tenant_id), K(start_tablet_id), K(range_count),
              K_(valid_tablet_ls_pairs));
  } else if (valid_tablet_ls_pairs_.count() <= 0) {
    ret = OB_ITER_END;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////

ObTenantTabletTableIterator::ObTenantTabletTableIterator()
    : inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      inner_idx_(0),
      tt_operator_(NULL),
      inner_tablet_infos_(),
      filters_()
{
}

int ObTenantTabletTableIterator::init(ObTabletTableOperator &tt_operator, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    tt_operator_ = &tt_operator;
    tenant_id_ = tenant_id;
    inited_ = true;
  }
  return ret;
}

int ObTenantTabletTableIterator::next(ObTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(inner_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_idx_ can't be smaller than 0", KR(ret), K_(inner_idx));
  } else {
    tablet_info.reset();
    if (inner_idx_ >= inner_tablet_infos_.count()) {
      if (OB_FAIL(prefetch_())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to prfetch", KR(ret));
        }
      } else {
        inner_idx_ = 0;
      }
    }
    if (FAILEDx(tablet_info.assign(inner_tablet_infos_[inner_idx_]))) {
      LOG_WARN("failed to assign tablet_info",
          KR(ret), K_(inner_idx), K_(inner_tablet_infos));
    } else if (OB_FAIL(tablet_info.filter(filters_))) {
      LOG_WARN("fail to filter tablet info", KR(ret), K(tablet_info));
    } else {
      ++inner_idx_;
    }
  }
  return ret;
}

int ObTenantTabletTableIterator::prefetch_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is unexpected null", KR(ret), KP_(tt_operator));
  } else {
    ObTabletID last_tablet_id; // start with INVALID_TABLET_ID = 0
    if (inner_tablet_infos_.count() > 0) {
      const int64_t last_idx = inner_tablet_infos_.count() - 1;
      last_tablet_id = inner_tablet_infos_.at(last_idx).get_tablet_id();
    }
    inner_tablet_infos_.reset();
    const int64_t range_size = GCONF.tablet_meta_table_scan_batch_count;
    if (OB_FAIL(tt_operator_->range_get(
        tenant_id_,
        last_tablet_id,
        range_size,
        inner_tablet_infos_))) {
      LOG_WARN("fail to range get by operator", KR(ret),
          K_(tenant_id), K(last_tablet_id), K(range_size), K_(inner_tablet_infos));
    } else if (inner_tablet_infos_.count() <= 0) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
