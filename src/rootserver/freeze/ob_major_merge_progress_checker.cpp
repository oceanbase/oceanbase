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

#define USING_LOG_PREFIX RS

#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_table_operator.h"
#include "share/tablet/ob_tablet_table_iterator.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_all_server_tracer.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/tablet/ob_tablet_table_iterator.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObMajorMergeProgressChecker::ObMajorMergeProgressChecker()
  : is_inited_(false), tenant_id_(OB_INVALID_ID), sql_proxy_(nullptr),
    schema_service_(nullptr), zone_merge_mgr_(nullptr), lst_operator_(nullptr),
    server_trace_(nullptr)
{}

int ObMajorMergeProgressChecker::init(
    const uint64_t tenant_id,
    common::ObMySQLProxy &sql_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObZoneMergeManager &zone_merge_mgr,
    share::ObLSTableOperator &lst_operator,
    ObIServerTrace &server_trace)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &sql_proxy;
    schema_service_ = &schema_service;
    zone_merge_mgr_ = &zone_merge_mgr;
    lst_operator_ = &lst_operator;
    server_trace_ = &server_trace;
    is_inited_ = true;
  }
  return ret;
}

int ObMajorMergeProgressChecker::check_merge_progress(
    const volatile bool &stop,
    const int64_t global_broadcast_scn,
    ObAllZoneMergeProgress &all_progress)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if ((global_broadcast_scn <= 0) || all_progress.error()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(global_broadcast_scn),
              "array_error", all_progress.error());
  } else {
    HEAP_VAR(ObZoneArray, all_zones) {
      all_progress.reset();

      if (OB_FAIL(zone_merge_mgr_->get_zone(all_zones))) {
        LOG_WARN("fail to get_zone", KR(ret), K_(tenant_id));
      } else {
        // init all zone progress
        ObMergeProgress zone_progress;
        for (int64_t i = 0; (i < all_zones.count()) && OB_SUCC(ret); ++i) {
          zone_progress.zone_ = all_zones.at(i);
          zone_progress.tenant_id_ = tenant_id_;
          if (OB_FAIL(all_progress.push_back(zone_progress))) {
            LOG_WARN("fail to add zone merge progress", KR(ret), K_(tenant_id));
          }
        }
        if (OB_SUCC(ret)) {
          std::sort(all_progress.begin(), all_progress.end());
        }
      }

      if (OB_SUCC(ret)) {
        ObSchemaGetterGuard schema_guard;
        ObTenantTabletMetaIterator iter;
        hash::ObHashMap<ObTabletID, uint64_t> tablet_map;
        if (OB_FAIL(iter.init(*sql_proxy_, tenant_id_))) {
          LOG_WARN("fail to init tablet table iterator", KR(ret), K_(tenant_id));
        }
        // Keep set_filter_not_exist_server before setting all the other filters, 
        // otherwise the other filters may return OB_ENTRY_NOT_EXIST error code.
        else if (OB_FAIL(iter.get_filters().set_filter_not_exist_server(*server_trace_))) {
          LOG_WARN("fail to set not exist server filter", KR(ret), K_(tenant_id));
        } else if (OB_FAIL(iter.get_filters().set_filter_permanent_offline(*server_trace_))) {
          LOG_WARN("fail to set filter", KR(ret), K_(tenant_id));
        }
        // TODO add more filter later, rebuild replica, restore replica, etc.
        else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
          LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
        } else if (OB_FAIL(schema_guard.generate_tablet_table_map(tenant_id_, tablet_map))) {
          LOG_WARN("fail to generate tablet table map", K_(tenant_id), KR(ret));
        } else {
          ObTabletInfo tablet;
          while (!stop && OB_SUCC(ret) && OB_SUCC(iter.next(tablet))) {
            if (OB_FAIL(check_tablet(tablet, tablet_map, all_progress,
                                     global_broadcast_scn, schema_guard))) {
              LOG_WARN("fail to check tablet merge progress", KR(ret), K_(tenant_id),
                       K(stop), K(tablet));
            }
          }

          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          if (stop && OB_SUCC(ret)) {
            ret = OB_CANCELED;
            LOG_WARN("already stop to check merge progress", KR(ret), K_(tenant_id));
          }
        }
      }
    }
  }

  const int64_t cost_us = ObTimeUtility::current_time() - start_time;
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to check merge progress", K_(tenant_id), K(global_broadcast_scn), K(cost_us));
  } else {
    LOG_WARN("fail to check merge progress", KR(ret), K_(tenant_id),
             K(global_broadcast_scn), K(cost_us));
  }
  return ret;
}

int ObMajorMergeProgressChecker::check_tablet(
    const ObTabletInfo &tablet,
    const common::hash::ObHashMap<ObTabletID, uint64_t> &tablet_map,
    ObAllZoneMergeProgress &all_progress,
    const int64_t global_broadcast_scn,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;

  const ObTabletID tablet_id = tablet.get_tablet_id();
  const share::schema::ObSimpleTableSchemaV2 *table_schema = nullptr;
  bool need_check = true;
  uint64_t table_id = OB_INVALID_ID;

  if (OB_FAIL(tablet_map.get_refactored(tablet_id, table_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // table schema does not exist
    } else {
      LOG_WARN("fail to get table_id", KR(ret), K(tablet_id));
    }
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(tablet_id));
  }

  if (OB_SUCC(ret)) {
    if (nullptr == table_schema) {
      need_check = false; // ignore the table does not exist
    } else {
      // no need check not ready index
      need_check = !(table_schema->is_index_table() && !table_schema->can_read_index());
    }

    if (OB_UNLIKELY(tablet_id.is_special_merge_tablet())) {
      // special tablet should not report to tablet meta table,
      // just for defense, print error log and no need check
      need_check = false;
      LOG_ERROR("unexpected tablet id", K(tablet_id), K(need_check));
    }
  }

  if (OB_SUCC(ret) && need_check) {
    ObLSInfo ls_info;
    int64_t cluster_id = GCONF.cluster_id;
    const ObLSID &ls_id = tablet.get_ls_id();
    if (OB_FAIL(lst_operator_->get(cluster_id, tenant_id_, ls_id, ls_info))) {
      LOG_WARN("fail to get ls info", KR(ret), K_(tenant_id), K(ls_id));
    } else if (OB_FAIL(check_majority_integrated(schema_guard, tablet, ls_info))) {
      LOG_WARN("fail to check majority integrated", KR(ret));
    } else if (OB_FAIL(check_tablet_data_version(all_progress, global_broadcast_scn, tablet, ls_info))) {
      LOG_WARN("fail to check majority integrated", KR(ret));
    }
  }

  return ret;
}

int ObMajorMergeProgressChecker::check_tablet_data_version(
    ObAllZoneMergeProgress &all_progress,
    const int64_t global_broadcast_scn,
    const ObTabletInfo &tablet,
    const share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;

  const ObLSReplica *ls_r = nullptr;
  FOREACH_CNT_X(r, tablet.get_replicas(), OB_SUCCESS == ret) {
    if (OB_FAIL(ls_info.find(r->get_server(), ls_r))) {
      LOG_WARN("fail to find lfs replica", "addr", r->get_server(), KR(ret));
    } else if (OB_UNLIKELY(nullptr == ls_r)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls replica", KR(ret), KPC(r));
    } else {
      ObAllZoneMergeProgress::iterator p =
        std::lower_bound(all_progress.begin(), all_progress.end(), ls_r->get_zone());
      if ((p != all_progress.end()) && (p->zone_ == ls_r->get_zone())) {
        if ((REPLICA_TYPE_LOGONLY == ls_r->get_replica_type())
            || (REPLICA_TYPE_ENCRYPTION_LOGONLY == ls_r->get_replica_type())) {
          // logonly replica no need check
        } else {
          if ((p->smallest_snapshot_version_ <= 0)
              || (p->smallest_snapshot_version_ > r->get_snapshot_version())) {
            p->smallest_snapshot_version_ = r->get_snapshot_version();
          }
          if (r->get_snapshot_version() < global_broadcast_scn) {
            // only log the first replica not merged
            if (0 == p->unmerged_tablet_cnt_) {
              LOG_INFO("replica not merged to target version", K_(tenant_id), 
                       "current_version", r->get_snapshot_version(), K(global_broadcast_scn), 
                       "replica", *r);
            }
            ++(p->unmerged_tablet_cnt_);
            p->unmerged_data_size_ += r->get_data_size();
          } else {
            ++(p->merged_tablet_cnt_);
            p->merged_data_size_ += r->get_data_size();
          }
        }
      }
    }
  }
  return ret;
}

int ObMajorMergeProgressChecker::check_majority_integrated(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const ObTabletInfo &tablet,
    const share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  int64_t full_cnt = 0;
  int64_t majority = 0;
  int64_t all_replica_num = OB_INVALID_COUNT;
  int64_t full_replica_num = OB_INVALID_COUNT;
  int64_t paxos_replica_num = OB_INVALID_COUNT;

  if (OB_FAIL(get_associated_replica_num(schema_guard, paxos_replica_num,
          full_replica_num, all_replica_num, majority))) {
    LOG_WARN("fail to get associated replica num", KR(ret), K_(tenant_id));
  } else {
    const int64_t tablet_replica_cnt = tablet.replica_count();
    int64_t paxos_cnt = 0;
    const ObLSReplica *ls_r = nullptr;
    FOREACH_CNT_X(r, tablet.get_replicas(), OB_SUCC(ret)) {
      if (OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid replica", KR(ret), K_(tenant_id), K(r));
      } else if (OB_FAIL(ls_info.find(r->get_server(), ls_r))) {
        LOG_WARN("fail to find", "addr", r->get_server(), KR(ret));
      } else if (OB_UNLIKELY(nullptr == ls_r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls replica", KR(ret), KPC(r));
      } else if (ObReplicaStatus::REPLICA_STATUS_NORMAL != ls_r->get_replica_status()) {
        // nothing
      } else if (ObReplicaTypeCheck::is_paxos_replica_V2(ls_r->get_replica_type())) {
        paxos_cnt++;
        if (REPLICA_TYPE_FULL == ls_r->get_replica_type()) {
          full_cnt++;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if ((full_cnt <= 0) || (paxos_cnt < majority)) {
        ret = OB_ROOT_NOT_INTEGRATED;
        if (tablet_replica_cnt < majority) {
          LOG_WARN("tablet replica meta may create too late, wait next round check", KR(ret), 
            K(tablet_replica_cnt), K(majority), K(paxos_cnt), K(ls_info.get_replicas()));
        } else {
          LOG_ERROR("not integrated", K(full_cnt), K(paxos_cnt), K(majority), K(all_replica_num),
            K(full_replica_num), K(paxos_replica_num), K(tablet), K(ls_info.get_replicas()));
        }
      }
    }
  }

  return ret;
}

int ObMajorMergeProgressChecker::get_associated_replica_num(
    share::schema::ObSchemaGetterGuard &schema_guard,
    int64_t &paxos_replica_num,
    int64_t &full_replica_num,
    int64_t &all_replica_num,
    int64_t &majority)
{
  int ret = OB_SUCCESS;

  const ObTenantSchema *tenant_schema = nullptr;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant info", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(nullptr == tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_schema is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(tenant_schema->get_paxos_replica_num(schema_guard, paxos_replica_num))) {
    LOG_WARN("fail to get table paxos replica num", KR(ret), K_(tenant_id));
  } else {
    full_replica_num = tenant_schema->get_full_replica_num();
    all_replica_num = tenant_schema->get_all_replica_num();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(paxos_replica_num <= 0)
             || OB_UNLIKELY(full_replica_num <= 0)
             || OB_UNLIKELY(all_replica_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected replica num", KR(ret), K(paxos_replica_num),
             K(full_replica_num), K(all_replica_num));
  } else {
    majority = rootserver::majority(paxos_replica_num);
  }

  return ret;
}

} // namespace rootserver
} // namespace oceanbase
