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
#include "rootserver/freeze/ob_major_freeze_util.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_table_operator.h"
#include "share/tablet/ob_tablet_table_iterator.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/scn.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObMajorMergeProgressChecker::ObMajorMergeProgressChecker()
  : merge_time_statistics_(), is_inited_(false), tenant_id_(OB_INVALID_ID), sql_proxy_(nullptr),
    schema_service_(nullptr), zone_merge_mgr_(nullptr), lst_operator_(nullptr),
    server_trace_(nullptr), tablet_compaction_map_(), table_count_(0), table_ids_(),
    table_compaction_map_(), tablet_validator_(), index_validator_(), cross_cluster_validator_(),
    uncompacted_tablets_(), diagnose_rw_lock_(ObLatchIds::MAJOR_FREEZE_DIAGNOSE_LOCK),
    ls_infos_map_()
{}

int ObMajorMergeProgressChecker::init(
    const uint64_t tenant_id,
    const bool is_primary_service,
    common::ObMySQLProxy &sql_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObZoneMergeManager &zone_merge_mgr,
    share::ObLSTableOperator &lst_operator,
    ObIServerTrace &server_trace)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_MAP_BUCKET_CNT = 10000;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(tablet_compaction_map_.create(DEFAULT_MAP_BUCKET_CNT, "MFTatCompactMap",
      "MFTatCompactMap", tenant_id))) {
    LOG_WARN("fail to create tablet compaction status map", KR(ret), K(tenant_id), K(DEFAULT_MAP_BUCKET_CNT));
  } else if (OB_FAIL(table_compaction_map_.create(DEFAULT_MAP_BUCKET_CNT, "MFTbCompMap", "MFTbCompMap", tenant_id))) {
    LOG_WARN("fail to create table compaction status map", KR(ret), K(tenant_id), K(DEFAULT_MAP_BUCKET_CNT));
  } else if (OB_FAIL(ls_infos_map_.create(300, "MFLsInfoMap", "MFLsInfoMap", tenant_id))) {
    LOG_WARN("fail to create table compaction status map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_validator_.init(tenant_id, is_primary_service, sql_proxy, zone_merge_mgr))) {
    LOG_WARN("fail to init tablet validator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(index_validator_.init(tenant_id, is_primary_service, sql_proxy, zone_merge_mgr))) {
    LOG_WARN("fail to init index validator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(cross_cluster_validator_.init(tenant_id, is_primary_service, sql_proxy, zone_merge_mgr))) {
    LOG_WARN("fail to init cross cluster validator", KR(ret), K(tenant_id));
  } else {
    table_ids_.set_attr(ObMemAttr(tenant_id, "TableIds"));
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

int ObMajorMergeProgressChecker::prepare_handle()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_compaction_map_.reuse())) {
    LOG_WARN("fail to reuse tablet_compaction_map", KR(ret));
  } else if (OB_FAIL(table_compaction_map_.reuse())) {
    LOG_WARN("fail to reuse table_compaction_map", KR(ret));
  } else {
    table_count_ = 0;
  }
  return ret;
}

int ObMajorMergeProgressChecker::check_table_status(bool &exist_unverified)
{
  int ret = OB_SUCCESS;
  SMART_VARS_2((ObArray<ObTableCompactionInfo>, uncompacted_tables),
               (ObArray<ObTableCompactionInfo>, unverified_tables)) {
    hash::ObHashMap<uint64_t, ObTableCompactionInfo>::iterator iter = table_compaction_map_.begin();
    int64_t ele_count = 0;
    for (;OB_SUCC(ret) && (iter != table_compaction_map_.end()); ++iter) {
      const ObTableCompactionInfo &compaction_info = iter->second;
      if (!compaction_info.is_verified()) {
        if (!has_exist_in_array(table_ids_, compaction_info.table_id_)) {
          ++ele_count; // ignore tables whose table_id does not exist in 'table_ids_'
        } else if (compaction_info.is_uncompacted()) {
          if (OB_FAIL(uncompacted_tables.push_back(compaction_info))) {
            LOG_WARN("fail to push back", KR(ret), K(compaction_info));
          } else {
            ++ele_count; // ignore uncompacted tables, which are caused by truncate table
          }
        } else if (OB_FAIL(unverified_tables.push_back(compaction_info))) {
          LOG_WARN("fail to push back", KR(ret), K(compaction_info));
        }
      } else if (compaction_info.is_verified()) {
        ++ele_count;
      }
    }

    if (OB_SUCC(ret)) {
      if (uncompacted_tables.count() > 0) {
        // Note: uncompacted tables are caused by truncate table. mark these uncomapted tables as
        // verified. Otherwise, major compaction cannot finish forever due to uncompacted tables.
        // Moreover, if uncompacted tables are index tables, data_tables can not start to verify
        // checksum with these uncompacted index_tables.
        //
        if (OB_FAIL(mark_uncompacted_tables_as_verified(uncompacted_tables))) {
          LOG_WARN("fail to mark uncompacted tables as verified", KR(ret), K(uncompacted_tables));
        }
      }
      const int64_t unverified_cnt = unverified_tables.count();
      exist_unverified = unverified_cnt > 0;
      if (exist_unverified) {
        const int64_t print_count = (unverified_cnt > 10) ? 10 : unverified_cnt;
        FLOG_INFO("exists unverified tables", K(unverified_cnt), K(print_count), "unverified_tables",
          ObArrayWrap<ObTableCompactionInfo>(unverified_tables.get_data(), print_count));
      } else if (ele_count != table_count_) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("table_compaction_map element count should not be less than table count", KR(ret), K(ele_count),
          K_(table_count));
      }
    }
  }
  return ret;
}

int ObMajorMergeProgressChecker::handle_table_with_first_tablet_in_sys_ls(
    const volatile bool &stop,
    const bool is_primary_service,
    const share::SCN &global_broadcast_scn,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTableSchemaV2 *simple_schema = nullptr;
  ObTableCompactionInfo cur_compaction_info;
  // table_id of the table containing first tablet in sys ls
  const uint64_t major_merge_special_table_id = ObChecksumValidatorBase::MAJOR_MERGE_SPECIAL_TABLE_ID;
  // only primary major_freeze_service need to handle table with frist tablet in sys ls here
  if (!is_primary_service) {
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, major_merge_special_table_id, simple_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(major_merge_special_table_id));
  } else if (OB_ISNULL(simple_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret), K_(tenant_id), K(major_merge_special_table_id));
  } else {
    SMART_VAR(ObArray<ObTabletLSPair>, pairs) {
      FREEZE_TIME_GUARD;
      if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(tenant_id_, *simple_schema,
                                                                       *sql_proxy_, pairs))) {
        LOG_WARN("fail to get tablet_ls pairs", KR(ret), K_(tenant_id), K(major_merge_special_table_id));
      } else if (OB_UNLIKELY(pairs.count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret), K_(tenant_id),
                 K(major_merge_special_table_id));
      } else if (OB_FAIL(table_compaction_map_.get_refactored(major_merge_special_table_id, cur_compaction_info))) {
        LOG_WARN("fail to get refactored", KR(ret), K(major_merge_special_table_id));
      } else if (OB_FAIL(cross_cluster_validator_.write_tablet_checksum_at_table_level(stop, pairs,
                   global_broadcast_scn, cur_compaction_info, major_merge_special_table_id, expected_epoch))) {
        if (OB_ITEM_NOT_MATCH == ret) {
          bool is_exist = false;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(ObTabletReplicaChecksumOperator::is_higher_ver_tablet_rep_ckm_exist(
              tenant_id_, global_broadcast_scn, major_merge_special_table_id, *sql_proxy_, is_exist))) {
            LOG_WARN("fail to check is higher version tablet replica checksum exist", KR(tmp_ret),
              K_(tenant_id), K(global_broadcast_scn), K(major_merge_special_table_id));
          } else if (is_exist) {
            // 1. one restore standby tenant switchover to primary tenant, launch one lower version
            // of major compaction, tablet replica checksum is overwritten by higher version.
            // 2. one lower version of major compaction is not finished, another higher version of
            // medium compaction is launched, leading to tablet replica checksum is overwritten by
            // higher version.
            LOG_ERROR("already exist higher version tablet checksum of first table", KR(ret),
              K(global_broadcast_scn), K(major_merge_special_table_id), K(expected_epoch));
            ret = OB_SUCCESS; // ignore ret, so as to let this round of major freeze finish
          } else {
            LOG_ERROR("no higher version tablet checksum of first table exist", KR(ret),
              K(global_broadcast_scn), K(major_merge_special_table_id), K(expected_epoch));
          }
        } else {
          LOG_WARN("fail to write tablet checksum at table level", KR(ret), K_(tenant_id), K(pairs));
        }
      } else if (OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_report_scn(
                   tenant_id_, global_broadcast_scn.get_val_for_tx(),
                   pairs, ObTabletReplica::ScnStatus::SCN_STATUS_ERROR, expected_epoch))) {
        LOG_WARN("fail to batch update report_scn", KR(ret), K_(tenant_id), K(pairs));
      }
    }
  }
  return ret;
}

int ObMajorMergeProgressChecker::check_merge_progress(
    const volatile bool &stop,
    const SCN &global_broadcast_scn,
    ObAllZoneMergeProgress &all_progress,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if ((!global_broadcast_scn.is_valid()) || all_progress.error()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(global_broadcast_scn),
              "array_error", all_progress.error());
  } else {
    HEAP_VAR(ObZoneArray, all_zones) {
      all_progress.reset();
      // reset uncompacted_tablets in the beginning of each round of check_merge_progress
      reset_uncompacted_tablets();

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
        } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id_, table_ids_))) {
          LOG_WARN("fail to get table ids in tenant", KR(ret), K_(tenant_id));
        } else if (OB_FAIL(refresh_ls_infos())) {
          LOG_WARN("fail to refresh ls infos", KR(ret), K_(tenant_id));
        } else {
          ObTabletInfo tablet_info;
          int64_t last_epoch_check_us = ObTimeUtil::fast_current_time();
          while (!stop && OB_SUCC(ret)) {
            {
              FREEZE_TIME_GUARD;
              if (OB_FAIL(iter.next(tablet_info))) {
                if (OB_ITER_END != ret) {
                  LOG_WARN("fail to get next tablet_info", KR(ret), K_(tenant_id), K(stop));
                }
              } else if (OB_FAIL(ObMajorFreezeUtil::check_epoch_periodically(*sql_proxy_, tenant_id_,
                                 expected_epoch, last_epoch_check_us))) {
                LOG_WARN("fail to check freeze service epoch", KR(ret), K_(tenant_id), K(stop));
              }
            }
            if (OB_FAIL(ret)) {
            } else if (!tablet_info.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("iterate invalid tablet info", KR(ret), K(tablet_info));
            } else if (OB_FAIL(check_tablet(tablet_info, tablet_map, all_progress, global_broadcast_scn,
                schema_guard))) {
              LOG_WARN("fail to check tablet", KR(ret), K_(tenant_id), K(stop), K(tablet_info));
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
  merge_time_statistics_.update_merge_status_us_.check_merge_progress_us_ = cost_us;
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to check merge progress", K_(tenant_id), K(global_broadcast_scn), K(cost_us));
  } else {
    LOG_WARN("fail to check merge progress", KR(ret), K_(tenant_id),
             K(global_broadcast_scn), K(cost_us));
  }
  return ret;
}

void ObMajorMergeProgressChecker::set_major_merge_start_time(
     const int64_t major_merge_start_us)
{
  cross_cluster_validator_.set_major_merge_start_time(major_merge_start_us);
}

int ObMajorMergeProgressChecker::get_uncompacted_tablets(
    ObArray<ObTabletReplica> &uncompacted_tablets) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    SpinRLockGuard r_guard(diagnose_rw_lock_);
    if (OB_FAIL(uncompacted_tablets.assign(uncompacted_tablets_))) {
      LOG_WARN("fail to assign uncompacted_tablets", KR(ret), K_(tenant_id), K_(uncompacted_tablets));
    }
  }
  return ret;
}

int ObMajorMergeProgressChecker::check_tablet(
    const ObTabletInfo &tablet_info,
    const common::hash::ObHashMap<ObTabletID, uint64_t> &tablet_map,
    ObAllZoneMergeProgress &all_progress,
    const SCN &global_broadcast_scn,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;

  const ObTabletID tablet_id(tablet_info.get_tablet_id());
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
      if (!need_check) {
        // not ready index, no need to check tablet compaction_scn and validate index column checksum
        ObTabletLSPair pair(tablet_info.get_tablet_id(), tablet_info.get_ls_id());
        if (OB_FAIL(tablet_compaction_map_.set_refactored(pair, ObTabletCompactionStatus::CAN_SKIP_VERIFYING, true))) {
          LOG_WARN("fail to set refactored", KR(ret), K(tablet_info));
        }
      }
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
    const ObLSID &ls_id = tablet_info.get_ls_id();
    if (OB_FAIL(ls_infos_map_.get_refactored(ls_id, ls_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // ls_info does not exist, ignore this tablet
        ret = OB_SUCCESS;
        if (TC_REACH_TIME_INTERVAL(30 * 1000 * 1000)) { // 30s
          LOG_WARN("ls_info does not exist", K_(tenant_id), K(ls_id), K(tablet_info));
        }
      } else {
        LOG_WARN("fail to get ls_info from ls_info_map", KR(ret), K(ls_id), K_(tenant_id));
      }
    } else if (OB_FAIL(check_tablet_compaction_scn(all_progress, global_broadcast_scn, tablet_info, ls_info))) {
      LOG_WARN("fail to check tablet compaction_scn", KR(ret), K(tablet_info), K(ls_info));
    }
  }

  return ret;
}

int ObMajorMergeProgressChecker::check_tablet_compaction_scn(
    ObAllZoneMergeProgress &all_progress,
    const SCN &global_broadcast_scn,
    const ObTabletInfo &tablet_info,
    const share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ret)) {
  } else {
    bool is_tablet_compacted = true;
    bool tablet_need_verify = true;
    const ObLSReplica *ls_r = nullptr;
    FOREACH_CNT_X(r, tablet_info.get_replicas(), OB_SUCCESS == ret) {
      if (OB_FAIL(ls_info.find(r->get_server(), ls_r))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // Ignore tablet replicas that are not in ls_info. E.g., after ls replica migration,
          // source ls meta has been deleted, but source tablet meta has not been deleted yet.
          ret = OB_SUCCESS;  // ignore ret
          LOG_INFO("ignore this tablet replica, sicne it is not in ls_info", K_(tenant_id),
                   KPC(r), K(ls_info));
        } else {
          LOG_WARN("fail to find ls replica", KR(ret), "addr", r->get_server());
	      }
      } else if (OB_UNLIKELY(nullptr == ls_r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls replica", KR(ret), KPC(r));
      } else {
        ObAllZoneMergeProgress::iterator p =
          std::lower_bound(all_progress.begin(), all_progress.end(), ls_r->get_zone());
        if ((p != all_progress.end()) && (p->zone_ == ls_r->get_zone())) {
          SCN replica_snapshot_scn;
          SCN replica_report_scn;
          if (OB_FAIL(replica_snapshot_scn.convert_for_tx(r->get_snapshot_version()))) {
            LOG_WARN("fail to convert val to SCN", KR(ret), "snapshot_version", r->get_snapshot_version());
          } else if (replica_report_scn.convert_for_tx(r->get_report_scn())) {
            LOG_WARN("fail to convert val to SCN", KR(ret), "report_scn", r->get_report_scn());
          } else if (replica_report_scn > replica_snapshot_scn) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected report_scn and snapshot_scn", KR(ret), "report_scn",
                     r->get_report_scn(), "snapshot_scn", r->get_snapshot_version());
          } else if ((REPLICA_TYPE_LOGONLY == ls_r->get_replica_type())
                    || (REPLICA_TYPE_ENCRYPTION_LOGONLY == ls_r->get_replica_type())) {
            // logonly replica no need check
          } else {
            if ((p->smallest_snapshot_scn_ <= SCN::min_scn())
                || (p->smallest_snapshot_scn_ > replica_snapshot_scn)) {
              p->smallest_snapshot_scn_ = replica_snapshot_scn;
            }
            if (replica_snapshot_scn >= global_broadcast_scn) {
              if (replica_snapshot_scn > global_broadcast_scn) { // launched another medium compaction
                tablet_need_verify = false; // this tablet does not need to execute checksum verification
              } else {  // replica_snapshot_scn == global_broadcast_scn
                if (replica_report_scn == global_broadcast_scn) { // finished verification on the old leader
                  tablet_need_verify = false; // this tablet does not need to execute checksum verification
                } else { // replica_report_scn < global_broadcast_scn
                  // check tablet replica status when replica_snapshot_scn = global_broadcast_scn
                  // and replica_report_scn < global_broadcast_scn, so as to find out checksum error
                  // occured before this round of major freeze. do not check tablet replica status
                  // when replica_snapshot_scn > global_broadcast_scn or replica_report_scn =
                  // global_broadcast_scn, since the checksum error detected here may be caused by
                  // medium compaction after this round of major freeze.
                  if (ObTabletReplica::ScnStatus::SCN_STATUS_ERROR == r->get_status()) {
                    ret = OB_CHECKSUM_ERROR;
                    LOG_ERROR("ERROR! ERROR! ERROR! find error status tablet replica", KR(ret), K(tablet_info));
                    if (TC_REACH_TIME_INTERVAL(6 * 3600 * 1000 * 1000)) {  // record every 6h
                      ROOTSERVICE_EVENT_ADD("daily_merge", "checksum_error", "tenant_id", r->get_tenant_id(),
                        "ls_id", r->get_ls_id().id(), "tablet_id", r->get_tablet_id().id());
                    }
                  }
                }
              }
              ++(p->merged_tablet_cnt_);
              p->merged_data_size_ += r->get_data_size();
            } else {
              // only log the first replica not merged
              if (0 == p->unmerged_tablet_cnt_) {
                LOG_INFO("replica not merged to target version or status not match", K_(tenant_id),
                        "current_version", r->get_snapshot_version(), K(global_broadcast_scn),
                        "current_status", r->get_status(), "compaction_replica", *r);
              }
              if (p->unmerged_tablet_cnt_ < 3) {
                SpinWLockGuard w_guard(diagnose_rw_lock_);
                if (OB_FAIL(uncompacted_tablets_.push_back(*r))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("fail to push_back", KR(ret), K_(tenant_id), K(global_broadcast_scn), KPC(r));
                }
              }
              ++(p->unmerged_tablet_cnt_);
              p->unmerged_data_size_ += r->get_data_size();
              is_tablet_compacted = false;
            }
          }
        }
      }
    } // end foreach

    if (OB_SUCC(ret) && is_tablet_compacted) {
      ObTabletLSPair pair(tablet_info.get_tablet_id(), tablet_info.get_ls_id());
      if (tablet_need_verify) {
        if (OB_FAIL(tablet_compaction_map_.set_refactored(pair, ObTabletCompactionStatus::COMPACTED, true))) {
          LOG_WARN("fail to set refactored", KR(ret), K(tablet_info));
        }
      } else {
        if (OB_FAIL(tablet_compaction_map_.set_refactored(pair, ObTabletCompactionStatus::CAN_SKIP_VERIFYING, true))) {
          LOG_WARN("fail to set refactored", KR(ret), K(tablet_info));
        }
      }
    }
  }
  return ret;
}

int ObMajorMergeProgressChecker::check_verification(
    const volatile bool &stop,
    const bool is_primary_service,
    const share::SCN &global_broadcast_scn,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  }
  // check and set need_validate for index_validator and cross_cluster_validator. tablet_validator
  // always need validate, thus there is no need to check and set need_validate for tablet_validator.
  else if (FALSE_IT(index_validator_.check_and_set_validate(is_primary_service))) {
  } else if (OB_FAIL(cross_cluster_validator_.check_and_set_validate(is_primary_service, global_broadcast_scn))) {
    LOG_WARN("fail to check and set validate for cross_cluster_validator", KR(ret), K(global_broadcast_scn));
  } else if (FALSE_IT(index_validator_.set_need_val_cross_cluster_ckm(cross_cluster_validator_.need_validate()))) {
  } else if (!tablet_compaction_map_.empty()) {
    if (OB_FAIL(tablet_validator_.validate_checksum(stop, global_broadcast_scn, tablet_compaction_map_,
      table_count_, table_compaction_map_, table_ids_, merge_time_statistics_, expected_epoch))) {
      LOG_WARN("fail to validate checksum of tablet validator", KR(ret), K(global_broadcast_scn));
    } else if (OB_FAIL(index_validator_.validate_checksum(stop, global_broadcast_scn,
        tablet_compaction_map_, table_count_, table_compaction_map_, table_ids_, merge_time_statistics_, expected_epoch))) {
      LOG_WARN("fail to validate checksum of index validator", KR(ret), K(global_broadcast_scn));
    } else if (OB_FAIL(cross_cluster_validator_.validate_checksum(stop, global_broadcast_scn,
        tablet_compaction_map_, table_count_, table_compaction_map_, table_ids_, merge_time_statistics_, expected_epoch))) {
      LOG_WARN("fail to validate checksum of cross cluster validator", KR(ret), K(global_broadcast_scn));
    }
  } else {
    LOG_INFO("none tablet finished compaction, no need to check verification", K(global_broadcast_scn));
  }
  return ret;
}

int ObMajorMergeProgressChecker::mark_uncompacted_tables_as_verified(
    const ObIArray<ObTableCompactionInfo> &uncompacted_tables)
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(table, uncompacted_tables, OB_SUCCESS == ret) {
    ObTableCompactionInfo table_compaction_info;
    const uint64_t table_id = table->table_id_;
    table_compaction_info.table_id_ = table_id;
    table_compaction_info.set_verified();
    if (OB_FAIL(table_compaction_map_.set_refactored(table_id, table_compaction_info, true/*overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(table_compaction_info));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to mark uncompacted tables as verified", K(uncompacted_tables));
  }
  return ret;
}

void ObMajorMergeProgressChecker::reset_uncompacted_tablets()
{
  SpinWLockGuard w_guard(diagnose_rw_lock_);
  uncompacted_tablets_.reset();
}

int ObMajorMergeProgressChecker::refresh_ls_infos()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    FREEZE_TIME_GUARD;
    // 1. clear ls_infos cached in memory
    ls_infos_map_.reuse();
    SMART_VAR(ObArray<ObLSInfo>, ls_infos) {
      // 2. load ls_infos from __all_ls_meta_table
      const bool inner_table_only = false;
      if (OB_ISNULL(lst_operator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lst_operator is null", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(lst_operator_->get_by_tenant(tenant_id_, inner_table_only, ls_infos))) {
        LOG_WARN("fail to get ls infos", KR(ret), K_(tenant_id));
      } else {
        // 3. update ls_infos cached in memory
        const int64_t ls_infos_cnt = ls_infos.count();
        for (int64_t i = 0; (i < ls_infos_cnt) && OB_SUCC(ret); ++i) {
          const ObLSID &ls_id = ls_infos.at(i).get_ls_id();
          const ObLSInfo &ls_info = ls_infos.at(i);
          if (OB_FAIL(ls_infos_map_.set_refactored(ls_id, ls_info, true/*overwrite*/))) {
            LOG_WARN("fail to set refactored", KR(ret), K(ls_id), K(ls_info));
          }
        }
      }
      LOG_INFO("finish to refresh ls infos", KR(ret), K(ls_infos));
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
