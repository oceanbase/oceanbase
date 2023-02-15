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

#include "rootserver/freeze/ob_checksum_validator.h"
#include "rootserver/freeze/ob_freeze_info_manager.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "rootserver/freeze/ob_major_freeze_util.h"
#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/ob_root_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/time/ob_time_utility.h"
#include "share/backup/ob_backup_manager.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_tablet_replica_checksum_operator.h"
#include "share/ob_tablet_checksum_operator.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

int ObMergeErrorCallback::init(
    const uint64_t tenant_id,
    ObZoneMergeManager &zone_merge_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    zone_merge_mgr_ = &zone_merge_mgr;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObMergeErrorCallback::handle_merge_error(
    const int64_t error_type,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    if (OB_FAIL(zone_merge_mgr_->set_merge_error(error_type, expected_epoch))) {
      LOG_WARN("fail to set merge error", KR(ret), K_(tenant_id), K(error_type), K(expected_epoch));
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////

int ObChecksumValidatorBase::init(
    const uint64_t tenant_id,
    const bool is_primary_service,
    ObMySQLProxy &sql_proxy,
    ObZoneMergeManager &zone_merge_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(merge_err_cb_.init(tenant_id, zone_merge_mgr))) {
    LOG_WARN("fail to init merge error callback", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_primary_service_ = is_primary_service;
    need_validate_ = false;
    sql_proxy_ = &sql_proxy;
    zone_merge_mgr_ = &zone_merge_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObChecksumValidatorBase::validate_checksum(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    int64_t &table_count,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if ((!frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn));
  } else if (OB_FAIL(check_all_table_verification_finished(stop, frozen_scn, tablet_compaction_map,
                     table_count, table_compaction_map, merge_time_statistics, expected_epoch))) {
    LOG_WARN("fail to check all table verification finished", KR(ret), K_(tenant_id), K(frozen_scn));
  }
  return ret;
}

bool ObChecksumValidatorBase::exist_in_table_array(
    const uint64_t table_id,
    const ObIArray<uint64_t> &table_ids) const
{
  bool exist = false;
  for (int64_t i = 0; (i < table_ids.count()) && !exist; ++i) {
    if (table_id == table_ids.at(i)) {
      exist = true;
    }
  }
  return exist;
}

int ObChecksumValidatorBase::get_table_compaction_info(
    const ObTableSchema &table_schema,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObTableCompactionInfo &table_compaction_info)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = UINT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema));
  } else if (FALSE_IT(table_id = table_schema.get_table_id())) {
  } else if (OB_FAIL(table_compaction_map.get_refactored(table_id, table_compaction_info))) {
    if (OB_HASH_NOT_EXIST == ret) {  // first initialization
      ret = OB_SUCCESS;
      table_compaction_info.table_id_ = table_id;
      if (OB_FAIL(table_compaction_map.set_refactored(table_id, table_compaction_info))) {
        LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(table_compaction_info));
      }
    } else {
      LOG_WARN("fail to get val from hashmap", KR(ret), K(table_id));
    }
  }
  return ret;
}

int ObChecksumValidatorBase::remove_not_exist_table(
    const ObArray<uint64_t> &table_ids,
    hash::ObHashMap<uint64_t, share::ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    ObArray<uint64_t> removed_table_ids;  // record the table_id which will be removed
    hash::ObHashMap<uint64_t, ObTableCompactionInfo>::iterator iter = table_compaction_map.begin();
    for (;OB_SUCC(ret) && (iter != table_compaction_map.end()); ++iter) {
      const uint64_t cur_table_id = iter->first;
      if (!exist_in_table_array(cur_table_id, table_ids)) {
        if (OB_FAIL(removed_table_ids.push_back(cur_table_id))) {
          LOG_WARN("fail to push back", KR(ret), K(cur_table_id));
        }
      }
    } /*end for iter*/
    for (int64_t i = 0; (OB_SUCC(ret) && (i < removed_table_ids.count())); ++i) {
      const uint64_t table_id = removed_table_ids.at(i);
      if (OB_FAIL(table_compaction_map.erase_refactored(table_id))) {
        LOG_WARN("fail to erase refactored", KR(ret), K(i), K(table_id));
      }
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////
int ObTabletChecksumValidator::check_all_table_verification_finished(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    int64_t &table_count,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  UNUSED(expected_epoch);

  const int64_t start_time_us = ObTimeUtil::current_time();
  if (OB_UNLIKELY(!frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn));
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else {
    table_count = 0;
    ObSchemaGetterGuard schema_guard;
    SMART_VAR(ObArray<const ObSimpleTableSchemaV2 *>, table_schemas) {
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
                  tenant_id_, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id_, table_schemas))) {
        LOG_WARN("fail to get tenant table schemas", KR(ret), K_(tenant_id));
      } else {
        table_count = table_schemas.count();
        for (int64_t i = 0; (i < table_count) && OB_SUCC(ret) && !stop; ++i) {
          const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
          if (OB_ISNULL(simple_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
          } else {
            const uint64_t table_id = simple_schema->get_table_id();
            const ObTableSchema *table_schema = nullptr;
            if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
              LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(table_id));
            } else if (OB_ISNULL(table_schema)) {
            }
            // check whether all tablets of this table finished compaction or not, and
            // execute tablet replica checksum verification if this table has tablet.
            else if (OB_FAIL(check_table_compaction_finished(*table_schema, frozen_scn,
                              tablet_compaction_map, table_compaction_map))) {
              LOG_WARN("fail to check table compaction finished", KR(ret), K(frozen_scn),
                      KPC(table_schema));
            }
            if (OB_CHECKSUM_ERROR == ret) {
              check_ret = ret;
            }
            ret = OB_SUCCESS;  // ignore ret, and continue check next table_schema
          }
        }  // end for loop
      }
    }
  }

  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = check_ret;
  }
  const int64_t cost_time_us = ObTimeUtil::current_time() - start_time_us;
  merge_time_statistics.update_merge_status_us_.tablet_validator_us_ = cost_time_us;

  return ret;
}

// check all tablets of this table finished compaction or not.
// note that, when one table finished compaction, we need to execute tablet_replica
// checksum verification if this table has tablet.
int ObTabletChecksumValidator::check_table_compaction_finished(
    const ObTableSchema &table_schema,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = UINT64_MAX;
  ObTableCompactionInfo latest_compaction_info;
  if (OB_FAIL(get_table_compaction_info(table_schema, table_compaction_map, latest_compaction_info))) {
    LOG_WARN("fail to get table compaction info", KR(ret), K(table_schema));
  } else if (FALSE_IT(table_id = latest_compaction_info.table_id_)) {
  } else if (latest_compaction_info.is_uncompacted()) {
    SMART_VAR(ObArray<ObTabletID>, tablet_ids) {
      SMART_VAR(ObArray<ObTabletLSPair>, pairs) {
        if (table_schema.has_tablet()) {
          FREEZE_TIME_GUARD;
          if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
            LOG_WARN("fail to get tablet_ids from table schema", KR(ret), K(table_schema));
          } else if (OB_UNLIKELY(tablet_ids.count() < 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get tablet_ids of current table schema", KR(ret), K_(tenant_id), K(table_schema));
          } else if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(tenant_id_,
                     table_id, *sql_proxy_, tablet_ids, pairs))) {
            LOG_WARN("fail to get tablet_ls pairs", KR(ret), K_(tenant_id), K(table_id));
          } else if (OB_UNLIKELY(pairs.count() < 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret),
                     K_(tenant_id), K(table_id), K(tablet_ids));
          } else {
            // iterate all tablets to check 'compacted/finished status' or not.
            const int64_t tablet_cnt = tablet_ids.count();
            int64_t idx = 0;
            bool exist_skip_verifying_tablet = false;
            for (; OB_SUCC(ret) && (idx < tablet_cnt); ++idx) {
              ObTabletCompactionStatus tablet_status = ObTabletCompactionStatus::INITIAL;
              if (OB_FAIL(tablet_compaction_map.get_refactored(pairs.at(idx), tablet_status))) {
                // if tablet not finish compaction, it won't be added into this map
                if (OB_HASH_NOT_EXIST == ret) {
                  ret = OB_SUCCESS;
                  break;
                } else {
                  LOG_WARN("fail to get tablet compaction status from map", KR(ret), K(idx),
                           "pair", pairs.at(idx));
                }
              } else if ((tablet_status != ObTabletCompactionStatus::COMPACTED)
                        && (tablet_status != ObTabletCompactionStatus::CAN_SKIP_VERIFYING)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected tablet status", KR(ret), K(tablet_status), K(frozen_scn), K(table_id));
              } else if (tablet_status == ObTabletCompactionStatus::CAN_SKIP_VERIFYING) {
                exist_skip_verifying_tablet = true;
              }
            } /*end outer for loop*/
            if (OB_SUCC(ret) && (idx == tablet_cnt)) {
              latest_compaction_info.tablet_cnt_ = tablet_ids.count();
              if (exist_skip_verifying_tablet) {
                latest_compaction_info.set_can_skip_verifying();
              } else {
                latest_compaction_info.set_compacted();
              }
            }
            // if current table 'has tablet' & 'finished compaction' & 'not skip verifying',
            // verify tablet replica checksum
            if (OB_SUCC(ret) && latest_compaction_info.is_compacted()) {
              FREEZE_TIME_GUARD;
              if (OB_FAIL(ObTabletReplicaChecksumOperator::check_tablet_replica_checksum(tenant_id_,
                          pairs, frozen_scn, *sql_proxy_))) {
                if (OB_CHECKSUM_ERROR == ret) {
                  LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in major tablet_replica_checksum",
                            KR(ret), K_(tenant_id), K(frozen_scn), "pair_cnt", pairs.count());
                } else {
                  LOG_WARN("fail to check major tablet_replica checksum", KR(ret), K_(tenant_id),
                           K(frozen_scn), K(table_schema));
                }
              }
            }
            // final, set this table as COMPACTED/CAN_SKIP_VERIFYING
            if (FAILEDx(table_compaction_map.set_refactored(table_id, latest_compaction_info, true/*overwrite*/))) {
              LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(latest_compaction_info));
            }
          }
        } else { // like VIEW, it does not have tablet, treat it as compaction finished and can skip verifying
          latest_compaction_info.tablet_cnt_ = 0;
          latest_compaction_info.set_can_skip_verifying();
          if (OB_FAIL(table_compaction_map.set_refactored(table_id, latest_compaction_info, true/*overwrite*/))) {
            LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(latest_compaction_info));
          }
        }
      }
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////
ObCrossClusterTabletChecksumValidator::ObCrossClusterTabletChecksumValidator()
  : major_merge_start_us_(-1), special_table_id_(OB_INVALID_ID)
{
}

int ObCrossClusterTabletChecksumValidator::check_need_validate(
    const bool is_primary_service,
    const SCN &frozen_scn,
    bool &need_validate) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_UNLIKELY(!frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(is_primary_service), K(frozen_scn));
  } else if (is_primary_service) {
    FREEZE_TIME_GUARD;
    if (OB_FAIL(ObTabletChecksumOperator::is_first_tablet_in_sys_ls_exist(*sql_proxy_,
                      tenant_id_, frozen_scn, is_exist))) {
      LOG_WARN("fail to check is first tablet in first ls exist", KR(ret), K_(tenant_id));
    } else if (is_exist) {
      // need to check cross-cluster checksum on primary tenant when all tablet checksum exist
      need_validate = true;
    } else {
      // no need to check cross-cluster checksum on primary tenant when not all tablet checksum exist
      need_validate = false;
    }
  } else {
    // need to check cross-cluster checksum on standby tenant
    need_validate = true;
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::check_and_set_validate(
    const bool is_primary_service,
    const share::SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  bool need_validate = false;
  if (OB_UNLIKELY(!frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(is_primary_service), K(frozen_scn));
  } else if (OB_FAIL(check_need_validate(is_primary_service, frozen_scn, need_validate))) {
    LOG_WARN("fail to check need validate", KR(ret), K_(tenant_id), K(is_primary_service), K(frozen_scn));
  } else {
    set_need_validate(need_validate);
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::check_all_table_verification_finished(
    const volatile bool &stop,
    const share::SCN &frozen_scn,
    const hash::ObHashMap<share::ObTabletLSPair, share::ObTabletCompactionStatus> &tablet_compaction_map,
    int64_t &table_count,
    hash::ObHashMap<uint64_t, share::ObTableCompactionInfo> &table_compaction_map,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;

  const int64_t start_time_us = ObTimeUtil::current_time();
  if (OB_UNLIKELY(!frozen_scn.is_valid() || tablet_compaction_map.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn),
             "is_tablet_compaction_map_empty", tablet_compaction_map.empty());
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else {
    table_count = 0;
    ObSchemaGetterGuard schema_guard;
    SMART_VARS_2((ObArray<const ObSimpleTableSchemaV2 *>, table_schemas),
                (ObArray<uint64_t>, table_ids)) {
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
                  tenant_id_, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id_, table_schemas))) {
        LOG_WARN("fail to get tenant table schemas", KR(ret), K_(tenant_id));
      } else {
        table_count = table_schemas.count();
        for (int64_t i = 0; (i < table_count) && OB_SUCC(ret) && !stop; ++i) {
          const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
          if (OB_ISNULL(simple_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
          } else {
            const uint64_t table_id = simple_schema->get_table_id();
            const ObTableSchema *table_schema = nullptr;
            ObTableCompactionInfo cur_compaction_info;
            if (OB_FAIL(table_ids.push_back(table_id))) {
              LOG_WARN("fail to push back", KR(ret), K(table_id));
            } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
              LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(table_id));
            } else if (OB_ISNULL(table_schema)) {
            } else if (OB_FAIL(get_table_compaction_info(*table_schema, table_compaction_map, cur_compaction_info))) {
              LOG_WARN("fail to get table compaction info", KR(ret), K(frozen_scn), KPC(table_schema));
            } else if (cur_compaction_info.is_verified()) { // already finished verification, skip it!
            } else if (table_schema->has_tablet()) {
              if (cur_compaction_info.is_index_ckm_verified()) {
                if (need_validate()) {  // need to validate cross-cluster checksum
                  // check whether waiting all tablet checksum has timed out
                  bool is_wait_tablet_checksum_timeout = check_waiting_tablet_checksum_timeout();
                  if (OB_UNLIKELY(is_wait_tablet_checksum_timeout)) {
                    LOG_ERROR("waiting all tablet checksum has timed out, validate cross-cluster"
                      " checksum with available tablet checksum" , K_(tenant_id), K(frozen_scn),
                      K_(major_merge_start_us), "current_time_us",
                      ObTimeUtil::current_time());
                  }
                  // check whether all tablet checksum has already exist
                  bool is_exist = false;
                  FREEZE_TIME_GUARD;
                  if (OB_FAIL(ObTabletChecksumOperator::is_first_tablet_in_sys_ls_exist(*sql_proxy_,
                        tenant_id_, frozen_scn, is_exist))) {
                    LOG_WARN("fail to check is first tablet in first ls exist", KR(ret), K_(tenant_id), K(frozen_scn));
                  } else if (is_exist || is_wait_tablet_checksum_timeout) { // all tablet checksum exist or timeout
                    if (OB_FAIL(check_cross_cluster_checksum(*table_schema, frozen_scn))) {
                      if (OB_CHECKSUM_ERROR == ret) {
                        LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in cross-cluster checksum", KR(ret),
                                  K_(tenant_id), K(frozen_scn), KPC(table_schema));
                      } else {
                        LOG_WARN("fail to check cross-cluster checksum", KR(ret), K_(tenant_id),
                                K(frozen_scn), KPC(table_schema));
                      }
                    } else if (OB_FAIL(handle_table_verification_finished(stop, table_schema, frozen_scn,
                                        table_compaction_map, merge_time_statistics, expected_epoch))) {
                      LOG_WARN("fail to handle table verification finished", KR(ret), K_(tenant_id),
                              K(frozen_scn), KPC(table_schema));
                    }
                  } else if (TC_REACH_TIME_INTERVAL(10 * 60 * 1000 * 1000)) {  // 10 min
                    LOG_WARN("can not check cross-cluster checksum now, please wait until first tablet"
                      "in sys ls exists", K_(tenant_id), K(frozen_scn), KPC(table_schema),
                      K_(major_merge_start_us), "current_time_us", ObTimeUtil::current_time());
                  }
                } else {  // no need to validate cross-cluster checksum
                  if (OB_FAIL(handle_table_verification_finished(stop, table_schema, frozen_scn,
                                table_compaction_map, merge_time_statistics, expected_epoch))) {
                    LOG_WARN("fail to handle table verification finished", KR(ret), K_(tenant_id),
                            K(frozen_scn), KPC(table_schema));
                  }
                }
              }
            } else { // like VIEW that has no tablet, update report_scn for this table and mark it as VERIFIED
              if (cur_compaction_info.is_index_ckm_verified()) {
                if (OB_FAIL(handle_table_verification_finished(stop, table_schema, frozen_scn,
                              table_compaction_map, merge_time_statistics, expected_epoch))) {
                  LOG_WARN("fail to handle table verification finished", KR(ret), K_(tenant_id),
                          K(frozen_scn), KPC(table_schema));
                }
              }
            }
            if (OB_CHECKSUM_ERROR == ret) {
              check_ret = ret;
            }
            ret = OB_SUCCESS;  // ignore ret, and continue check next table_schema
          }
        }  // end for loop

        if (OB_SUCC(ret) && (OB_SUCCESS == check_ret)) {
          if (OB_FAIL(remove_not_exist_table(table_ids, table_compaction_map))) {
            LOG_WARN("fail to remove not exist table", KR(ret), K_(tenant_id), K(frozen_scn));
          }
        }
      }
    }
  }

  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = check_ret;
  }
  const int64_t cost_time_us = ObTimeUtil::current_time() - start_time_us;
  merge_time_statistics.update_merge_status_us_.cross_cluster_validator_us_ = cost_time_us;

  return ret;
}

int ObCrossClusterTabletChecksumValidator::check_cross_cluster_checksum(
    const ObTableSchema &table_schema,
    const SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_schema.has_tablet() || !frozen_scn.is_valid() || frozen_scn < SCN::min_scn())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema), K(frozen_scn));
  } else {
    SMART_VARS_2((ObArray<ObTabletID>, tablet_ids), (ObArray<ObTabletLSPair>, pairs)) {
      FREEZE_TIME_GUARD;
      if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
        LOG_WARN("fail to get tablet_ids from table schema", KR(ret), K(table_schema));
      } else if (OB_UNLIKELY(tablet_ids.count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablet_ids of current table schema", KR(ret), K_(tenant_id), K(table_schema));
      } else if (FALSE_IT(sort_tablet_ids(tablet_ids))) {  // tablet_ids should be in order
      } else if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(tenant_id_,
                  table_schema.get_table_id(), *sql_proxy_, tablet_ids, pairs))) {
        LOG_WARN("fail to get tablet_ls pairs", KR(ret), K_(tenant_id), "table_id",
                  table_schema.get_table_id());
      } else if (OB_UNLIKELY(pairs.count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret),
                  K_(tenant_id), "table_id", table_schema.get_table_id(), K(tablet_ids));
      } else {
        SMART_VARS_2((ObArray<ObTabletReplicaChecksumItem>, tablet_replica_checksum_items),
                      (ObArray<ObTabletChecksumItem>, tablet_checksum_items)) {
          FREEZE_TIME_GUARD;
          if (OB_FAIL(ObTabletReplicaChecksumOperator::batch_get(tenant_id_, pairs, frozen_scn,
                      *sql_proxy_, tablet_replica_checksum_items))) {
            LOG_WARN("fail to batch get tablet replica checksum items", KR(ret), K_(tenant_id), K(frozen_scn));
          } else if (OB_FAIL(ObTabletChecksumOperator::load_tablet_checksum_items(*sql_proxy_,
                              pairs, tenant_id_, frozen_scn, tablet_checksum_items))) {
            LOG_WARN("fail to batch get tablet checksum items", KR(ret), K_(tenant_id), K(frozen_scn));
          } else if (OB_FAIL(check_column_checksum(tablet_replica_checksum_items, tablet_checksum_items))) {
            if (OB_CHECKSUM_ERROR == ret) {
              LOG_ERROR("ERROR! ERROR! ERROR! checksum error in cross-cluster checksum", KR(ret),
                        K_(tenant_id), K(frozen_scn), K(table_schema));
            } else {
              LOG_WARN("fail to check cross-cluster checksum", KR(ret), K_(tenant_id),
                       K(frozen_scn), K(table_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}

void ObCrossClusterTabletChecksumValidator::sort_tablet_ids(ObArray<ObTabletID> &tablet_ids)
{
  std::sort(tablet_ids.begin(), tablet_ids.end());
}

int ObCrossClusterTabletChecksumValidator::check_column_checksum(
    const ObArray<ObTabletReplicaChecksumItem> &tablet_replica_checksum_items,
    const ObArray<ObTabletChecksumItem> &tablet_checksum_items)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  int cmp_ret = 0;
  ObTabletChecksumItem tablet_checksum_item;
  ObTabletReplicaChecksumItem tablet_replica_checksum_item;
  int64_t i = 0;
  int64_t j = 0;
  int64_t tablet_checksum_item_cnt = tablet_checksum_items.count();
  int64_t tablet_replica_checksum_item_cnt = tablet_replica_checksum_items.count();
  while (OB_SUCC(ret) && (i < tablet_checksum_item_cnt) && (j < tablet_replica_checksum_item_cnt)) {
    cmp_ret = 0;
    tablet_checksum_item.reset();
    if (OB_FAIL(tablet_checksum_item.assign(tablet_checksum_items.at(i)))) {
      LOG_WARN("fail to assign tablet checksum item", KR(ret), K_(tenant_id), K(i));
    } else {
      do {
        if (cmp_ret >= 0) { // iterator all tablet replica checksum util next different tablet.
          tablet_replica_checksum_item.reset();
          if (OB_FAIL(tablet_replica_checksum_item.assign(tablet_replica_checksum_items.at(j)))) {
            LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K_(tenant_id), K(j));
          } else if (0 == (cmp_ret = tablet_checksum_item.compare_tablet(tablet_replica_checksum_item))) {
            if (OB_FAIL(tablet_checksum_item.verify_tablet_column_checksum(tablet_replica_checksum_item))) {
              if (OB_CHECKSUM_ERROR == ret) {
                LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in cross-cluster checksum",
                  K(tablet_checksum_item), K(tablet_replica_checksum_item));
                check_ret = OB_CHECKSUM_ERROR;
                ret = OB_SUCCESS; // continue checking next checksum
              } else {
                LOG_WARN("unexpected error in cross-cluster checksum", KR(ret),
                  K(tablet_checksum_item), K(tablet_replica_checksum_item));
              }
            }
          }
        }
        if (cmp_ret >= 0) {
          ++j;
        }
      } while ((cmp_ret >= 0) && (j < tablet_replica_checksum_item_cnt) && OB_SUCC(ret));
    }
    ++i;
  }
  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = OB_CHECKSUM_ERROR;
  }
  return ret;
}

bool ObCrossClusterTabletChecksumValidator::is_first_tablet_in_sys_ls(const ObTabletReplicaChecksumItem &item) const
{
  // mark tablet_id=1 && ls_id=1 as end flag
  return (item.ls_id_.is_sys_ls()) && (item.tablet_id_.id() == ObTabletID::MIN_VALID_TABLET_ID);
}

bool ObCrossClusterTabletChecksumValidator::check_waiting_tablet_checksum_timeout() const
{
  const int64_t MAX_TABLET_CHECKSUM_WAIT_TIME_US = 36 * 3600 * 1000 * 1000L;  // 36 hours
  const int64_t total_wait_time_us = (ObTimeUtil::current_time() - major_merge_start_us_);
  return (total_wait_time_us > MAX_TABLET_CHECKSUM_WAIT_TIME_US);
}

// If one table finished cross-cluster checksum verification, update report_scn and then mark it as VERIFIED
int ObCrossClusterTabletChecksumValidator::handle_table_verification_finished(
    const volatile bool &stop,
    const ObTableSchema *table_schema,
    const SCN &frozen_scn,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  bool is_containing = false;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    const uint64_t table_id = table_schema->get_table_id();
    ObTableCompactionInfo cur_compaction_info;
    if (OB_FAIL(table_compaction_map.get_refactored(table_id, cur_compaction_info))) {
      LOG_WARN("fail to get refactored", KR(ret), K(table_id));
    } else if (cur_compaction_info.is_verified()) { // skip if finished verification
    } else if (!cur_compaction_info.is_index_ckm_verified()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table must finish index checksum valiation when arriving here", KR(ret),
               K(table_id), K(cur_compaction_info));
    } else {
      if (table_schema->has_tablet()) {
        SMART_VAR(ObArray<ObTabletLSPair>, pairs) {
          FREEZE_TIME_GUARD;
          if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(tenant_id_, *table_schema, *sql_proxy_, pairs))) {
            LOG_WARN("fail to get tablet_ls pairs", KR(ret), K_(tenant_id), K(table_id));
          } else if (OB_UNLIKELY(pairs.count() < 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret), K_(tenant_id), K(table_id));
          } else {
            bool need_udpate_report_scn = true;
            if (is_primary_service_) { // only primary major_freeze_service need to write tablet checksum
              const int64_t write_start_time_us = ObTimeUtil::current_time();
              if (OB_FAIL(contains_first_tablet_in_sys_ls(pairs, is_containing))) {
                LOG_WARN("fail to check if contains first tablet in sys ls", KR(ret), K_(tenant_id), K(pairs));
              } else if (is_containing) {
                // do not write tablet checksum and update report_scn of this table here.
                // instead, just record the table_id of this table here. write tablet checksum
                // and update report_scn of this table in the end of this round of major freeze.
                special_table_id_ = table_id;
                need_udpate_report_scn = false;
                LOG_INFO("this table contains first tablet in sys ls, write tablet checksum and update"
                        " report_scn of this table later", K(table_id), K(pairs));
              } else if (OB_FAIL(write_tablet_checksum_at_table_level(stop, pairs, frozen_scn,
                                  cur_compaction_info, table_id, expected_epoch))) {
                LOG_WARN("fail to write tablet checksum at table level", KR(ret), K_(tenant_id), K(pairs));
              }
              const int64_t write_cost_time_us = ObTimeUtil::current_time() - write_start_time_us;
              merge_time_statistics.update_merge_status_us_.write_tablet_checksum_us_ += write_cost_time_us;
            }
            if (need_udpate_report_scn) {
              const int64_t update_start_time_us = ObTimeUtil::current_time();
              if (FAILEDx(ObTabletMetaTableCompactionOperator::batch_update_report_scn(
                            tenant_id_, frozen_scn.get_val_for_tx(),
                            pairs, ObTabletReplica::ScnStatus::SCN_STATUS_ERROR))) {
                LOG_WARN("fail to batch update report_scn", KR(ret), K_(tenant_id), K(pairs));
              }
              const int64_t update_cost_time_us = ObTimeUtil::current_time() - update_start_time_us;
              merge_time_statistics.update_merge_status_us_.update_report_scn_us_ += update_cost_time_us;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        cur_compaction_info.set_verified();
        if (OB_FAIL(table_compaction_map.set_refactored(table_id, cur_compaction_info, true/*overwrite*/))) {
          LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(cur_compaction_info));
        }
      }
    }
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::write_tablet_checksum_at_table_level(
    const volatile bool &stop,
    const ObArray<ObTabletLSPair> &pairs,
    const share::SCN &frozen_scn,
    const share::ObTableCompactionInfo &table_compaction_info,
    const uint64_t table_id,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  FREEZE_TIME_GUARD;
  if (OB_UNLIKELY(pairs.empty()
                  || (!table_compaction_info.is_index_ckm_verified() && (table_id != special_table_id_))
                  || (!table_compaction_info.is_verified() && (table_id == special_table_id_)
                      && (OB_INVALID_ID != special_table_id_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pairs), K(frozen_scn), K(table_compaction_info),
             K(table_id), K_(special_table_id));
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if (!is_primary_service_) { // only primary major_freeze_service need to write tablet checksum
  } else if (OB_FAIL(ObTabletChecksumOperator::is_first_tablet_in_sys_ls_exist(*sql_proxy_,
                                               tenant_id_, frozen_scn, is_exist))) {
    LOG_WARN("fail to check is first tablet in first ls exist", KR(ret), K_(tenant_id));
  } else if (!is_exist) {
    if (table_compaction_info.can_skip_verifying()) {
      // do not write tablet checksum items for tables that can skip verifying,
      // since tablet checksum items of these tables must have already been written
    } else if ((table_compaction_info.is_index_ckm_verified() && (table_id != special_table_id_))
               || (table_compaction_info.is_verified() && (table_id == special_table_id_)
                      && (OB_INVALID_ID != special_table_id_))) {
      const int64_t IMMEDIATE_RETRY_CNT = 5;
      int64_t fail_count = 0;
      int64_t sleep_time_s = 1L;  // 1s
      while (!stop
             && (fail_count < IMMEDIATE_RETRY_CNT)
             && OB_FAIL(try_update_tablet_checksum_items(stop, pairs, frozen_scn, expected_epoch))) {
        if (OB_FREEZE_SERVICE_EPOCH_MISMATCH == ret) {
          LOG_WARN("freeze_service_epoch mismatch, no need to write tablet checksum items", KR(ret), K_(tenant_id));
          break;
        } else {
          ++fail_count;
          LOG_WARN("fail to write tablet checksum items", KR(ret), K_(tenant_id), K(fail_count), K(sleep_time_s));
          sleep(sleep_time_s);
          sleep_time_s *= 2;
        }
      }
    }
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::try_update_tablet_checksum_items(
    const volatile bool &stop,
    const ObArray<ObTabletLSPair> &pairs,
    const share::SCN &frozen_scn,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  bool is_match = true;
  bool contain_first_tablet_in_sys_ls = false;
  const int64_t MAX_BATCH_INSERT_COUNT = 100;
  FREEZE_TIME_GUARD;
  if (OB_UNLIKELY(pairs.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pairs), K(frozen_scn));
  }
  // Can not select freeze_service_epoch for update and update tablet_checksum_items in one same
  // transaction, since __all_service_epoch and __all_tablet_checksum are not in one same tenant.
  // Therefore, just get and check freeze_service_epoch here. However, this does not impact the
  // correctness of updating tablet_checksum_items.
  else if (OB_FAIL(ObServiceEpochProxy::check_service_epoch(*sql_proxy_, tenant_id_,
              ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, expected_epoch, is_match))) {
    LOG_WARN("fail to check freeze service epoch", KR(ret), K_(tenant_id), K(expected_epoch));
  } else if (!is_match) {
    ret = OB_FREEZE_SERVICE_EPOCH_MISMATCH;
    LOG_WARN("no need to update tablet checksum items, cuz freeze_service_epoch mismatch",
             K_(tenant_id), K(expected_epoch));
  } else {
    SMART_VAR(ObArray<ObTabletReplicaChecksumItem>, items) {
      FREEZE_TIME_GUARD;
      if (OB_FAIL(ObTabletReplicaChecksumOperator::batch_get(tenant_id_, pairs, frozen_scn, *sql_proxy_, items))) {
        LOG_WARN("fail to batch get tablet replica checksum items", KR(ret), K_(tenant_id), K(frozen_scn));
      } else {
        ObTabletReplicaChecksumItem curr_replica_item;
        ObTabletReplicaChecksumItem prev_replica_item;
        ObTabletChecksumItem tmp_checksum_item;
        // mark_end_item is the 'first_ls & first_tablet' checksum item. If we get this checksum item,
        // we need to insert it into __all_tablet_checksum table at last. In this case, if we get this
        // tablet's checksum item in table, we can ensure all checksum items have already been inserted.
        ObTabletChecksumItem mark_end_item;
        ObArray<ObTabletChecksumItem> tablet_checksum_items;
        const int64_t item_cnt = items.count();
        for (int64_t i = 0; !stop && OB_SUCC(ret) && (i < item_cnt); ++i) {
          curr_replica_item.reset();
          if (OB_FAIL(curr_replica_item.assign(items.at(i)))) {
            LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K(i), "item", items.at(i));
          } else if (!curr_replica_item.is_key_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet replica checksum is not valid", KR(ret), K(curr_replica_item));
          } else {
            if (curr_replica_item.is_same_tablet(prev_replica_item)) { // write one checksum_item per tablet
            } else {
              if (is_first_tablet_in_sys_ls(curr_replica_item)) {
                contain_first_tablet_in_sys_ls = true;
                if (OB_FAIL(mark_end_item.assign(curr_replica_item))) {
                  LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K(curr_replica_item));
                }
              } else {
                if (OB_FAIL(tmp_checksum_item.assign(curr_replica_item))) {
                  LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K(curr_replica_item));
                } else if (OB_FAIL(tablet_checksum_items.push_back(tmp_checksum_item))) {
                  LOG_WARN("fail to push back tablet checksum item", KR(ret), K(tmp_checksum_item));
                }
              }
              if (FAILEDx(prev_replica_item.assign_key(curr_replica_item))) {
                LOG_WARN("fail to assign key of tablet replica checksum item", KR(ret), K(curr_replica_item));
              }
            }
          }

          if ((item_cnt - 1) == i) {  // already iterate all tablet checksum items
            if (contain_first_tablet_in_sys_ls) {
              if (OB_UNLIKELY(!mark_end_item.is_valid())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected err about mark_end_item", KR(ret), K(mark_end_item));
              } else if (FAILEDx(tablet_checksum_items.push_back(mark_end_item))) {
                LOG_WARN("fail to push back tablet checksum item", KR(ret), K(mark_end_item));
              }
            }
            if (tablet_checksum_items.count() > 0) {
              FREEZE_TIME_GUARD;
              if (FAILEDx(ObTabletChecksumOperator::update_tablet_checksum_items(*sql_proxy_,
                          tenant_id_, tablet_checksum_items))) {
                LOG_WARN("fail to try update tablet checksum items", KR(ret), K_(tenant_id));
              }
            }
          } else if (tablet_checksum_items.count() >= MAX_BATCH_INSERT_COUNT) {
            FREEZE_TIME_GUARD;
            if (FAILEDx(ObTabletChecksumOperator::update_tablet_checksum_items(*sql_proxy_,
                        tenant_id_, tablet_checksum_items))) {
              LOG_WARN("fail to try update tablet checksum items", KR(ret), K_(tenant_id));
            } else {
              tablet_checksum_items.reuse();
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::contains_first_tablet_in_sys_ls(
    const ObArray<ObTabletLSPair> &pairs,
    bool &is_containing) const
{
  int ret = OB_SUCCESS;
  is_containing = false;
  ObTabletLSPair tmp_pair;
  if (OB_UNLIKELY(pairs.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(pairs));
  } else {
    const int64_t item_cnt = pairs.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < item_cnt); ++i) {
      if (OB_FAIL(tmp_pair.assign(pairs.at(i)))) {
        LOG_WARN("fail to assign tablet ls pair", KR(ret), K(i), "pair", pairs.at(i));
      } else if ((tmp_pair.get_tablet_id().id() == ObTabletID::MIN_VALID_TABLET_ID)
                && tmp_pair.get_ls_id().is_sys_ls()) {
        is_containing = true;
        break;
      }
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////
void ObIndexChecksumValidator::check_need_validate(
     const bool is_primary_service,
     bool &need_validate) const
{
  if (is_primary_service) {
    // need to check index checksum on primary tenant
    need_validate = true;
  } else {
    // no need to check index checksum on standby tenant
    need_validate = false;
  }
}

void ObIndexChecksumValidator::check_and_set_validate(const bool is_primary_service)
{
  bool need_validate = false;
  check_need_validate(is_primary_service, need_validate);
  set_need_validate(need_validate);
}

int ObIndexChecksumValidator::check_all_table_verification_finished(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    int64_t &table_count,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;

  const int64_t start_time_us = ObTimeUtil::current_time();
  if (OB_UNLIKELY(!frozen_scn.is_valid() || tablet_compaction_map.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn),
             "is_tablet_compaction_map_empty", tablet_compaction_map.empty());
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else {
    table_count = 0;
    ObSchemaGetterGuard schema_guard;
    SMART_VARS_2((ObArray<const ObSimpleTableSchemaV2 *>, table_schemas),
                (ObArray<uint64_t>, table_ids)) {
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id_, table_schemas))) {
        LOG_WARN("fail to get tenant table schemas", KR(ret), K_(tenant_id));
      } else {
        table_count = table_schemas.count();
        for (int64_t i = 0; (i < table_count) && OB_SUCC(ret) && !stop; ++i) {
          const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
          if (OB_ISNULL(simple_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
          } else {
            const uint64_t table_id = simple_schema->get_table_id();
            const ObTableSchema *table_schema = nullptr;
            ObTableCompactionInfo cur_compaction_info;
            if (OB_FAIL(table_ids.push_back(table_id))) {
              LOG_WARN("fail to push back", KR(ret), K(table_id));
            } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
              LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(table_id));
            } else if (OB_ISNULL(table_schema)) {
            } else if (OB_FAIL(get_table_compaction_info(*table_schema, table_compaction_map, cur_compaction_info))) {
              LOG_WARN("fail to get table compaction info", KR(ret), K(frozen_scn), KPC(table_schema));
            } else if (cur_compaction_info.is_index_ckm_verified()
                      || cur_compaction_info.is_verified()) { // already finished verification, skip it!
            } else if (is_index_table(*simple_schema)) { // for index table, may need to check column checksum
              const uint64_t data_table_id = simple_schema->get_data_table_id();
              const ObTableSchema *data_table_schema = nullptr;
              if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, data_table_schema))) {
                LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(data_table_id));
              } else if (OB_ISNULL(data_table_schema)) {
                ret = OB_TABLE_NOT_EXIST;
                LOG_WARN("fail to get data table schema", KR(ret), K_(tenant_id), K(table_id), K(data_table_id));
              } else {
                ObTableCompactionInfo data_compaction_info;
                if (OB_FAIL(get_table_compaction_info(*data_table_schema, table_compaction_map, data_compaction_info))) {
                  LOG_WARN("fail to get table compaction info", KR(ret), K(frozen_scn), KPC(data_table_schema));
                } else if (data_compaction_info.is_index_ckm_verified()
                          || data_compaction_info.is_verified()) {
                  // if a data table finished verification, then create index on this data table.
                  // we should skip verification for this index table, cuz the data table may already
                  // launched another medium compaction.
                  LOG_INFO("index table is not verified while data table is already verified, skip"
                    " verification for this index table", K(table_id), K(data_table_id),
                    K(cur_compaction_info), K(data_compaction_info));
                  if (cur_compaction_info.finish_compaction()) {
                    if (OB_FAIL(handle_table_verification_finished(table_id, frozen_scn, table_compaction_map))) {
                      LOG_WARN("fail to handle index table compaction finished", KR(ret), K(table_id), K(frozen_scn));
                    }
                  }
                } else if (table_schema->has_tablet()) {
                  if (!cur_compaction_info.finish_compaction() || !data_compaction_info.finish_compaction()) {
                  } else if (cur_compaction_info.is_compacted() && data_compaction_info.is_compacted()) {
                    #ifdef ERRSIM
                        ret = OB_E(EventTable::EN_MEDIUM_VERIFY_GROUP_SKIP_SET_VERIFY) OB_SUCCESS;
                        if (OB_FAIL(ret)) {
                          if (!is_inner_table(table_id)) {
                            ret = OB_EAGAIN;
                            STORAGE_LOG(INFO, "ERRSIM EN_MEDIUM_VERIFY_GROUP_SKIP_SET_VERIFY failed", K(ret));
                          } else {
                            ret = OB_SUCCESS;
                          }
                        }
                    #endif
                    // both tables' all tablets finished compaction, validate column checksum if need_validate()
                    if (need_validate()) {
                      FREEZE_TIME_GUARD;
                      if (FAILEDx(ObTabletReplicaChecksumOperator::check_column_checksum(tenant_id_,
                            *data_table_schema, *table_schema, frozen_scn, *sql_proxy_, expected_epoch))) {
                        if (OB_CHECKSUM_ERROR == ret) {
                          LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in index checksum", KR(ret), KPC(data_table_schema),
                            K_(tenant_id), K(frozen_scn), KPC(table_schema));
                        } else {
                          LOG_WARN("fail to check index column checksum", KR(ret), K_(tenant_id), KPC(data_table_schema),
                            KPC(table_schema));
                        }
                      }
                    }
                    // after index checksum verification, mark it as INDEX_CKM_VERIFIED
                    if (FAILEDx(handle_table_verification_finished(table_id, frozen_scn, table_compaction_map))) {
                      LOG_WARN("fail to handle table verification finished", KR(ret), K(table_id), K(frozen_scn));
                    }
                  } else if (cur_compaction_info.can_skip_verifying() || data_compaction_info.can_skip_verifying()) {
                    // if one of them can skip verifying, that means we don't need to execute index checksum verification.
                    // Mark index table as INDEX_CKM_VERIFIED directly.
                    if (OB_FAIL(handle_table_verification_finished(table_id, frozen_scn, table_compaction_map))) {
                      LOG_WARN("fail to handle index table verification finished", KR(ret), K(table_id), K(frozen_scn));
                    }
                  }
                  if (OB_SUCC(ret)) {
                    if (OB_FAIL(table_compaction_map.set_refactored(data_table_id, data_compaction_info, true/*overwrite*/))) {
                      LOG_WARN("fail to set refactored", KR(ret), K(data_table_id), K(data_compaction_info));
                    }
                  }
                } else { // virtual index table has no tablet, no need to execute index checksum verification.
                  if (cur_compaction_info.finish_compaction() && data_compaction_info.finish_compaction()) {
                    if (OB_FAIL(handle_table_verification_finished(table_id, frozen_scn, table_compaction_map))) {
                      LOG_WARN("fail to handle index table verification finished", KR(ret), K(table_id), K(frozen_scn));
                    }
                  }
                }
              }
            } else {
              if (table_schema->get_index_tid_count() < 1) { // handle data table, meanwhile not have relative index table
                if (cur_compaction_info.finish_compaction()) {
                  if (OB_FAIL(handle_table_verification_finished(table_id, frozen_scn, table_compaction_map))) {
                    LOG_WARN("fail to handle table verification finished", KR(ret), K(table_id), K(frozen_scn));
                  }
                }
              }
            }

            if (OB_CHECKSUM_ERROR == ret) {
              check_ret = ret;
            }
            if (OB_FREEZE_SERVICE_EPOCH_MISMATCH == ret) {
              // do not ignore ret, therefore not continue to check next table_schema
            } else {
              ret = OB_SUCCESS; // ignore ret, and continue to check next table_schema
            }
          }
        } // end for loop

        if (OB_SUCC(ret) && (OB_SUCCESS == check_ret)) {
          // for data table with index, if all its index tables finished verification,
          // then mark it as INDEX_CKM_VERIFIED.
          if (OB_FAIL(handle_data_table_with_index(stop, frozen_scn, table_ids, table_schemas, table_compaction_map))) {
            LOG_WARN("fail to handle data table with index", KR(ret), K_(tenant_id), K(stop), K(frozen_scn));
          } else if (OB_FAIL(remove_not_exist_table(table_ids, table_compaction_map))) {
            LOG_WARN("fail to remove not exist table", KR(ret), K_(tenant_id), K(frozen_scn));
          }
        }
      }
    }
  }

  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = check_ret;
  }
  const int64_t cost_time_us = ObTimeUtil::current_time() - start_time_us;
  merge_time_statistics.update_merge_status_us_.index_validator_us_ = cost_time_us;

  return ret;
}

int ObIndexChecksumValidator::update_data_table_verified(
    const uint64_t data_table_id,
    const ObTableCompactionInfo &data_table_compaction,
    const SCN &frozen_scn,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  if (data_table_compaction.is_index_ckm_verified()
      || data_table_compaction.is_verified()) { // skip if already finished verification
  } else if (data_table_compaction.finish_compaction()) {
    if (OB_FAIL(handle_table_verification_finished(data_table_id, frozen_scn, table_compaction_map))) {
      LOG_WARN("fail to handle table compaction finished", KR(ret), K(data_table_id), K(frozen_scn));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table must finish compaction when its all index table finished verifying", KR(ret), K(data_table_id),
      K(frozen_scn), K(data_table_compaction));
  }
  return ret;
}

// If one table finished compaction (and finished index checksum verification if needed),
// mark it as INDEX_CKM_VERIFIED
int ObIndexChecksumValidator::handle_table_verification_finished(
    const uint64_t table_id,
    const SCN &frozen_scn,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  ObTableCompactionInfo cur_compaction_info;
  if (OB_FAIL(table_compaction_map.get_refactored(table_id, cur_compaction_info))) {
    LOG_WARN("fail to get refactored", KR(ret), K(table_id));
  } else if (cur_compaction_info.is_index_ckm_verified()
              || cur_compaction_info.is_verified()) { // skip if finished verification
  } else if (!cur_compaction_info.finish_compaction()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table must finish compaction when arriving here", KR(ret), K(table_id), K(cur_compaction_info));
  } else {
    cur_compaction_info.set_index_ckm_verified();
    if (OB_FAIL(table_compaction_map.set_refactored(table_id, cur_compaction_info, true/*overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(cur_compaction_info));
    }
  }
  return ret;
}

bool ObIndexChecksumValidator::is_index_table(
     const ObSimpleTableSchemaV2 &simple_schema)
{
  return (simple_schema.is_index_table()
         && simple_schema.can_read_index());
}

int ObIndexChecksumValidator::handle_data_table_with_index(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const ObIArray<uint64_t> &table_ids,
    const ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObArray<uint64_t>, data_tables_to_update) {
    // check data tables with index, return those need to be marked as INDEX_CKM_VERIFIED
    if (OB_FAIL(check_data_table_with_index(table_schemas, table_compaction_map, data_tables_to_update))) {
      LOG_WARN("fail to check data table with index", KR(ret), K_(tenant_id), K(frozen_scn));
    }
    // mark data tables whose all index tables finished verification as INDEX_CKM_VERIFIED
    hash::ObHashMap<uint64_t, ObTableCompactionInfo>::iterator iter = table_compaction_map.begin();
    for (; !stop && OB_SUCC(ret) && (iter != table_compaction_map.end()); ++iter) {
      const uint64_t cur_table_id = iter->first;
      const ObTableCompactionInfo &table_compaction_info = iter->second;
      if (exist_in_table_array(cur_table_id, table_ids)
          && exist_in_table_array(cur_table_id, data_tables_to_update)) {
        if (OB_FAIL(update_data_table_verified(cur_table_id, table_compaction_info,
                                               frozen_scn, table_compaction_map))) {
          LOG_WARN("fail to update data table to verified status", KR(ret), K(cur_table_id),
                  K(frozen_scn), K(table_compaction_info));
        }
      }
    }
  }
  return ret;
}

int ObIndexChecksumValidator::check_data_table_with_index(
    const ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObIArray<uint64_t> &data_tables_to_update)
{
  int ret = OB_SUCCESS;
  data_tables_to_update.reset();
  int64_t table_count = table_schemas.count();
  // push_back data tables with index
  for (int64_t i = 0; (i < table_count) && OB_SUCC(ret); ++i) {
    const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
    if (OB_ISNULL(simple_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
    } else if (is_index_table(*simple_schema)) {
      const uint64_t data_table_id = simple_schema->get_data_table_id();
      if (!has_exist_in_array(data_tables_to_update, data_table_id)) {
        data_tables_to_update.push_back(data_table_id);
      }
    }
  }
  // remove data tables whose index tables do not finish verification
  for (int64_t i = 0; (i < table_count) && OB_SUCC(ret); ++i) {
    const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
    if (OB_ISNULL(simple_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
    } else if (is_index_table(*simple_schema)) {
      const uint64_t index_table_id = simple_schema->get_table_id();
      const uint64_t data_table_id = simple_schema->get_data_table_id();
      ObTableCompactionInfo index_table_compaction_info;
      if (OB_FAIL(table_compaction_map.get_refactored(index_table_id, index_table_compaction_info))) {
        LOG_WARN("fail to get refactored", KR(ret), K(index_table_id));
      } else if (!index_table_compaction_info.is_index_ckm_verified()
                 && !index_table_compaction_info.is_verified()) { // index_table is not verified
        int64_t idx = -1;
        if (has_exist_in_array(data_tables_to_update, data_table_id, &idx)) {
          if (OB_FAIL(data_tables_to_update.remove(idx))) {
            LOG_WARN("fail to remove", KR(ret), K(data_tables_to_update), K(idx));
          }
        }
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
