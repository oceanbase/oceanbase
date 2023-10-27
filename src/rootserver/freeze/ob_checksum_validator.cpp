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
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/time/ob_time_utility.h"
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
    const ObIArray<uint64_t> &ori_table_ids,
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
                     table_count, table_compaction_map, ori_table_ids, merge_time_statistics, expected_epoch))) {
    LOG_WARN("fail to check all table verification finished", KR(ret), K_(tenant_id), K(frozen_scn));
  }
  return ret;
}

int ObChecksumValidatorBase::check_exist_in_table_set(
    const uint64_t table_id,
    const hash::ObHashSet<uint64_t> &table_id_set,
    bool &is_exist) const
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_FAIL(table_id_set.exist_refactored(table_id))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      is_exist = true;
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_exist = false;
    } else {
      LOG_WARN("fail to check exist", KR(ret), K(table_id));
    }
  }
  return ret;
}

int ObChecksumValidatorBase::get_table_compaction_info(
    const ObSimpleTableSchemaV2 &simple_schema,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObTableCompactionInfo &table_compaction_info)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = UINT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (!simple_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(simple_schema));
  } else if (FALSE_IT(table_id = simple_schema.get_table_id())) {
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

int ObChecksumValidatorBase::convert_array_to_set(
    const ObIArray<uint64_t> &table_ids,
    hash::ObHashSet<uint64_t> &table_id_set)
{
  int ret = OB_SUCCESS;
  const int64_t table_id_cnt = table_ids.count();
  for (int64_t i = 0; (i < table_id_cnt) && OB_SUCC(ret); ++i) {
    if (OB_FAIL(table_id_set.set_refactored(table_ids.at(i), 0/*overwrite*/))) {
      LOG_WARN("fail to set_refactored", KR(ret), "table_id", table_ids.at(i));
    }
  }
  return ret;
}

int ObChecksumValidatorBase::remove_not_exist_table(
    const ObArray<uint64_t> &table_ids,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    SMART_VAR(ObArray<uint64_t>, removed_table_ids) { // record the table_id which will be removed
      hash::ObHashSet<uint64_t> table_id_set;
      if (OB_FAIL(table_id_set.create(10000, "MFTabIdSet", "MFTabIdSet", tenant_id_))) {
        LOG_WARN("fail to create table id set", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(convert_array_to_set(table_ids, table_id_set))) {
        LOG_WARN("fail to convert array to set", KR(ret));
      }
      hash::ObHashMap<uint64_t, ObTableCompactionInfo>::iterator iter = table_compaction_map.begin();
      for (;OB_SUCC(ret) && (iter != table_compaction_map.end()); ++iter) {
        const uint64_t cur_table_id = iter->first;
        bool is_exist = false;
        if (OB_FAIL(check_exist_in_table_set(cur_table_id, table_id_set, is_exist))) {
          LOG_WARN("fail to check if exist in table set", KR(ret), K(cur_table_id));
        } else if (!is_exist) {
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
  }
  return ret;
}

int ObChecksumValidatorBase::handle_table_can_not_verify(
    const uint64_t table_id,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  ObTableCompactionInfo table_compaction_info;
  table_compaction_info.table_id_ = table_id;
  table_compaction_info.set_verified();
  if (OB_FAIL(table_compaction_map.set_refactored(table_id, table_compaction_info, true/*overwrite*/))) {
    LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(table_compaction_info));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to handle table can not verify", K(table_id));
  } else {
    LOG_INFO("fail to handle table can not verify", K(table_id));
  }
  return ret;
}

int ObChecksumValidatorBase::write_ckm_and_update_report_scn(
    const volatile bool &stop,
    const ObSimpleTableSchemaV2 *simple_schema,
    const SCN &frozen_scn,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_containing = false;
  if (OB_ISNULL(simple_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    const uint64_t table_id = simple_schema->get_table_id();
    ObTableCompactionInfo cur_compaction_info;
    if (OB_FAIL(table_compaction_map.get_refactored(table_id, cur_compaction_info))) {
      LOG_WARN("fail to get refactored", KR(ret), K(table_id));
    } else if (cur_compaction_info.is_verified()) { // skip if finished verification
    } else if (!cur_compaction_info.is_index_ckm_verified()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table must finish index checksum valiation when arriving here", KR(ret),
               K(table_id), K(cur_compaction_info));
    } else {
      // set it to false, if succ to handle_table_can_not_verify
      bool need_update_map = true;
      if (simple_schema->has_tablet()) {
        SMART_VAR(ObArray<ObTabletLSPair>, pairs) {
          FREEZE_TIME_GUARD;
          if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(tenant_id_, *simple_schema, *sql_proxy_, pairs))) {
            if (OB_LIKELY(OB_ITEM_NOT_MATCH == ret)) {
              if (OB_TMP_FAIL(handle_table_can_not_verify(table_id, table_compaction_map))) {
                LOG_WARN("fail to handle table can not verify", KR(tmp_ret), K(table_id));
              } else {
                ret = OB_SUCCESS; // ignore ret
                need_update_map = false;
              }
            } else {
              LOG_WARN("fail to get tablet_ls pairs", KR(ret), K_(tenant_id), K(table_id));
            }
          } else if (OB_UNLIKELY(pairs.count() < 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret), K_(tenant_id), K(table_id));
          } else {
            bool need_update_report_scn = true;
            if (is_primary_service_) { // only primary major_freeze_service need to write tablet checksum
              const int64_t write_start_time_us = ObTimeUtil::current_time();
              if (OB_FAIL(contains_first_tablet_in_sys_ls(pairs, is_containing))) {
                LOG_WARN("fail to check if contains first tablet in sys ls", KR(ret), K_(tenant_id), K(pairs));
              } else if (is_containing) {
                // do not write tablet checksum and update report_scn of this table here.
                // instead, write tablet checksum and update report_scn of this table in the end
                // of this round of major freeze.
                if (MAJOR_MERGE_SPECIAL_TABLE_ID != table_id) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("table_id of the table containing first tablet in sys ls does not equal"
                           " to 1", KR(ret), K_(tenant_id), K(table_id));
                }
                need_update_report_scn = false;
                LOG_INFO("this table contains first tablet in sys ls, write tablet checksum and update"
                        " report_scn of this table later", K(table_id), K(pairs));
              } else if (OB_FAIL(write_tablet_checksum_at_table_level(stop, pairs, frozen_scn,
                                  cur_compaction_info, table_id, expected_epoch))) {
                if (OB_LIKELY(OB_ITEM_NOT_MATCH == ret)) {
                  if (OB_TMP_FAIL(handle_table_can_not_verify(table_id, table_compaction_map))) {
                    LOG_WARN("fail to handle table can not verify", KR(tmp_ret), K(table_id));
                  } else {
                    ret = OB_SUCCESS; // ignore ret
                    need_update_map = false;
                    need_update_report_scn = false;
                  }
                } else {
                  LOG_WARN("fail to write tablet checksum at table level", KR(ret), K_(tenant_id), K(pairs));
                }
              }
              const int64_t write_cost_time_us = ObTimeUtil::current_time() - write_start_time_us;
              merge_time_statistics.update_merge_status_us_.write_tablet_checksum_us_ += write_cost_time_us;
            }
            if (OB_SUCC(ret) && need_update_report_scn) {
              const int64_t update_start_time_us = ObTimeUtil::current_time();
              if (OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_report_scn(
                            tenant_id_, frozen_scn.get_val_for_tx(),
                            pairs, ObTabletReplica::ScnStatus::SCN_STATUS_ERROR, expected_epoch))) {
                LOG_WARN("fail to batch update report_scn", KR(ret), K_(tenant_id), K(pairs));
              }
              const int64_t update_cost_time_us = ObTimeUtil::current_time() - update_start_time_us;
              merge_time_statistics.update_merge_status_us_.update_report_scn_us_ += update_cost_time_us;
            }
          }
        }
      }

      if (OB_SUCC(ret) && need_update_map) {
        cur_compaction_info.set_verified();
        if (OB_FAIL(table_compaction_map.set_refactored(table_id, cur_compaction_info, true/*overwrite*/))) {
          LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(cur_compaction_info));
        }
      }
    }
  }
  return ret;
}

int ObChecksumValidatorBase::contains_first_tablet_in_sys_ls(
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

int ObChecksumValidatorBase::write_tablet_checksum_at_table_level(
    const volatile bool &stop,
    const ObArray<ObTabletLSPair> &pairs,
    const share::SCN &frozen_scn,
    const share::ObTableCompactionInfo &table_compaction_info,
    const uint64_t table_id,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  FREEZE_TIME_GUARD;
  if (OB_UNLIKELY(pairs.empty()
                  || (!table_compaction_info.is_index_ckm_verified() && (MAJOR_MERGE_SPECIAL_TABLE_ID != table_id))
                  || (!table_compaction_info.is_verified() && (MAJOR_MERGE_SPECIAL_TABLE_ID == table_id)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pairs), K(frozen_scn), K(table_compaction_info),
             K(table_id));
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if (!is_primary_service_) { // only primary major_freeze_service need to write tablet checksum
  } else {
    if ((table_compaction_info.is_index_ckm_verified() && (MAJOR_MERGE_SPECIAL_TABLE_ID != table_id))
        || (table_compaction_info.is_verified() && (MAJOR_MERGE_SPECIAL_TABLE_ID == table_id))) {
      const int64_t IMMEDIATE_RETRY_CNT = 5;
      int64_t fail_count = 0;
      int64_t sleep_time_us = 200 * 1000; // 200 ms
      while (!stop
             && (fail_count < IMMEDIATE_RETRY_CNT)
             && OB_FAIL(try_update_tablet_checksum_items(stop, pairs, frozen_scn, expected_epoch))) {
        if (OB_FREEZE_SERVICE_EPOCH_MISMATCH == ret) {
          LOG_WARN("freeze_service_epoch mismatch, no need to write tablet checksum items", KR(ret), K_(tenant_id));
          break;
        } else if (OB_ITEM_NOT_MATCH == ret) {
          LOG_INFO("tablet replica checksum item is empty, no need to write tablet checksum items", KR(ret), K_(tenant_id));
          break;
        } else {
          ++fail_count;
          LOG_WARN("fail to write tablet checksum items", KR(ret), K_(tenant_id), K(fail_count), K(sleep_time_us));
          USLEEP(sleep_time_us);
          sleep_time_us *= 2;
        }
      }
    }
  }
  return ret;
}

int ObChecksumValidatorBase::try_update_tablet_checksum_items(
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
      } else if (0 == items.count()) {
        ret = OB_ITEM_NOT_MATCH;
        LOG_WARN("fail to get tablet replica checksum items", KR(ret), K_(tenant_id), K(frozen_scn), K(pairs));
      } else {
        ObTabletReplicaChecksumItem curr_replica_item;
        ObTabletReplicaChecksumItem prev_replica_item;
        ObTabletChecksumItem tmp_checksum_item;
        // mark_end_item is the 'first_ls & first_tablet' checksum item. If we get this checksum item,
        // we need to insert it into __all_tablet_checksum table at last. In this case, if we get this
        // tablet's checksum item in table, we can ensure all checksum items have already been inserted.
        ObTabletChecksumItem mark_end_item;
        SMART_VAR(ObArray<ObTabletChecksumItem>, tablet_checksum_items) {
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
  }
  return ret;
}

bool ObChecksumValidatorBase::is_first_tablet_in_sys_ls(const ObTabletReplicaChecksumItem &item) const
{
  // mark tablet_id=1 && ls_id=1 as end flag
  return (item.ls_id_.is_sys_ls()) && (item.tablet_id_.id() == ObTabletID::MIN_VALID_TABLET_ID);
}


///////////////////////////////////////////////////////////////////////////////
int ObTabletChecksumValidator::check_all_table_verification_finished(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    int64_t &table_count,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    const ObIArray<uint64_t> &ori_table_ids,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  UNUSED(ori_table_ids);

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
        int64_t last_epoch_check_us = ObTimeUtil::fast_current_time();
        for (int64_t i = 0; (i < table_count) && OB_SUCC(ret) && !stop; ++i) {
          const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
          if (OB_ISNULL(simple_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
          } else if (OB_FAIL(ObMajorFreezeUtil::check_epoch_periodically(*sql_proxy_, tenant_id_,
                             expected_epoch, last_epoch_check_us))) {
            LOG_WARN("fail to check freeze service epoch", KR(ret), K_(tenant_id), K(stop));
          } else {
            const uint64_t table_id = simple_schema->get_table_id();
            // check whether all tablets of this table finished compaction or not, and
            // execute tablet replica checksum verification if this table has tablet.
            if (OB_FAIL(check_table_compaction_and_validate_checksum(*simple_schema, frozen_scn,
                             tablet_compaction_map, table_compaction_map))) {
              LOG_WARN("fail to check table compaction finished", KR(ret), K(frozen_scn),
                      KPC(simple_schema));
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
        }  // end for loop
      }
    }
  }

  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = check_ret;
  }
  const int64_t cost_time_us = ObTimeUtil::current_time() - start_time_us;
  merge_time_statistics.update_merge_status_us_.tablet_validator_us_ = cost_time_us;
  LOG_INFO("finish to check all table verification finished", KR(ret), K_(tenant_id), K(frozen_scn),
           K(expected_epoch), K(stop), K(cost_time_us));
  return ret;
}

// check all tablets of this table finished compaction or not.
// note that, when one table finished compaction, we need to execute tablet_replica
// checksum verification if this table has tablet.
int ObTabletChecksumValidator::check_table_compaction_and_validate_checksum(
    const ObSimpleTableSchemaV2 &simple_schema,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t table_id = UINT64_MAX;
  ObTableCompactionInfo latest_compaction_info;
  if (OB_FAIL(get_table_compaction_info(simple_schema, table_compaction_map, latest_compaction_info))) {
    LOG_WARN("fail to get table compaction info", KR(ret), K(simple_schema));
  } else if (FALSE_IT(table_id = latest_compaction_info.table_id_)) {
  } else if (latest_compaction_info.is_uncompacted()) {
    SMART_VAR(ObArray<ObTabletID>, tablet_ids) {
      SMART_VAR(ObArray<ObTabletLSPair>, pairs) {
        if (simple_schema.has_tablet()) {
          FREEZE_TIME_GUARD;
          if (OB_FAIL(simple_schema.get_tablet_ids(tablet_ids))) {
            LOG_WARN("fail to get tablet_ids from table schema", KR(ret), K(simple_schema));
          } else if (OB_UNLIKELY(tablet_ids.count() < 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get tablet_ids of current table schema", KR(ret), K_(tenant_id), K(simple_schema));
          } else if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(tenant_id_,
                     table_id, *sql_proxy_, tablet_ids, pairs))) {
            if (OB_LIKELY(OB_ITEM_NOT_MATCH == ret)) {
              if (OB_TMP_FAIL(handle_table_can_not_verify(table_id, table_compaction_map))) {
                LOG_WARN("fail to handle table can not verify", KR(tmp_ret), K(table_id));
              } else {
                ret = OB_SUCCESS; // ignore ret
              }
            } else {
              LOG_WARN("fail to get tablet_ls pairs", KR(ret), K_(tenant_id), K(table_id));
            }
          } else if (OB_UNLIKELY(pairs.count() < 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret),
                     K_(tenant_id), K(table_id), K(tablet_ids));
          } else {
            if (OB_FAIL(check_table_compaction_info(tablet_ids, pairs, tablet_compaction_map, latest_compaction_info))) {
              LOG_WARN("fail to check compaction_scn", KR(ret), K_(tenant_id), K(table_id), K(tablet_ids), K(pairs));
            }
            // set it to false, if succ to handle_table_can_not_verify
            bool need_update_map = true;
            // if current table 'has tablet' & 'finished compaction' & 'not skip verifying',
            // verify tablet replica checksum
            if (OB_SUCC(ret) && latest_compaction_info.is_compacted()) {
              if (OB_FAIL(validate_tablet_replica_checksum(frozen_scn, table_id, pairs,
                          table_compaction_map, need_update_map))) {
                LOG_WARN("fail to validate tablet replica checksum", KR(ret), K(frozen_scn), K(table_id));
              }
            }
            // set this table as COMPACTED/VERIFIED if needed
            if (OB_SUCC(ret) && need_update_map) {
              if (OB_FAIL(table_compaction_map.set_refactored(table_id, latest_compaction_info, true/*overwrite*/))) {
                LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(latest_compaction_info));
              }
            }
          }
        } else { // like VIEW, it does not have tablet, treat it as compaction finished and VERIFIED
          latest_compaction_info.tablet_cnt_ = 0;
          latest_compaction_info.set_verified();
          if (OB_FAIL(table_compaction_map.set_refactored(table_id, latest_compaction_info, true/*overwrite*/))) {
            LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(latest_compaction_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletChecksumValidator::check_table_compaction_info(
    const ObArray<ObTabletID> &tablet_ids,
    const ObArray<ObTabletLSPair> &pairs,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    ObTableCompactionInfo &latest_compaction_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((tablet_ids.count() < 1) || (pairs.count() < 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_ids), K(pairs));
  } else {
    // iterate all tablets to check 'COMPACTED/CAN_SKIP_VERIFYING status' or not.
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
          LOG_WARN("fail to get tablet compaction status from map", KR(ret), K(idx), "pair", pairs.at(idx));
        }
      } else if ((tablet_status != ObTabletCompactionStatus::COMPACTED)
                 && (tablet_status != ObTabletCompactionStatus::CAN_SKIP_VERIFYING)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet status", KR(ret), K(tablet_status), K(tablet_ids.at(idx)));
      } else if (tablet_status == ObTabletCompactionStatus::CAN_SKIP_VERIFYING) {
        exist_skip_verifying_tablet = true;
      }
    } /*end for loop*/
    if (OB_SUCC(ret) && (idx == tablet_cnt)) {
      latest_compaction_info.tablet_cnt_ = tablet_ids.count();
      if (exist_skip_verifying_tablet) {
        // for table that exists CAN_SKIP_VERIFYING tablet, direct mark this table as VERIFIED
        latest_compaction_info.set_verified();
      } else {
        latest_compaction_info.set_compacted();
      }
    }
  }
  return ret;
}

int ObTabletChecksumValidator::validate_tablet_replica_checksum(
    const SCN &frozen_scn,
    const uint64_t table_id,
    const ObArray<ObTabletLSPair> &pairs,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    bool &need_update_map)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!frozen_scn.is_valid() || pairs.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(frozen_scn), K(pairs));
  } else {
    FREEZE_TIME_GUARD;
    if (OB_FAIL(ObTabletReplicaChecksumOperator::check_tablet_replica_checksum(tenant_id_,
                pairs, frozen_scn, *sql_proxy_))) {
      if (OB_ITEM_NOT_MATCH == ret) {
        if (OB_TMP_FAIL(handle_table_can_not_verify(table_id, table_compaction_map))) {
          LOG_WARN("fail to handle table can not verify", KR(tmp_ret), K(table_id));
        } else {
          ret = OB_SUCCESS; // ignore ret
          need_update_map = false;
        }
      } else if (OB_CHECKSUM_ERROR == ret) {
        LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in major "
          "tablet_replica_checksum", KR(ret), K_(tenant_id), K(frozen_scn), "pair_cnt", pairs.count());
        if (TC_REACH_TIME_INTERVAL(6 * 3600 * 1000 * 1000)) {  // record every 6h
          ROOTSERVICE_EVENT_ADD("daily_merge", "checksum_error", K_(tenant_id), K(table_id));
        }
      } else {
        LOG_WARN("fail to check major tablet_replica checksum", KR(ret), K_(tenant_id),
                 K(frozen_scn), K(table_id));
      }
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////
ObCrossClusterTabletChecksumValidator::ObCrossClusterTabletChecksumValidator()
  : major_merge_start_us_(-1), is_all_tablet_checksum_exist_(false)
{
}

int ObCrossClusterTabletChecksumValidator::check_and_set_validate(
    const bool is_primary_service,
    const share::SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  is_all_tablet_checksum_exist_ = false; // reset is_all_tablet_checksum_exist_
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
      need_validate_ = true;
      is_all_tablet_checksum_exist_ = true;
    } else {
      // no need to check cross-cluster checksum on primary tenant when not all tablet checksum exist
      need_validate_ = false;
    }
  } else {
    // need to check cross-cluster checksum on standby tenant
    need_validate_ = true;
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::check_all_table_verification_finished(
    const volatile bool &stop,
    const share::SCN &frozen_scn,
    const hash::ObHashMap<share::ObTabletLSPair, share::ObTabletCompactionStatus> &tablet_compaction_map,
    int64_t &table_count,
    hash::ObHashMap<uint64_t, share::ObTableCompactionInfo> &table_compaction_map,
    const ObIArray<uint64_t> &ori_table_ids,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  UNUSED(ori_table_ids);

  const int64_t start_time_us = ObTimeUtil::current_time();
  if (OB_UNLIKELY(!frozen_scn.is_valid() || tablet_compaction_map.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn),
             "is_tablet_compaction_map_empty", tablet_compaction_map.empty());
  } else if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if (need_validate()) {  // need to validate cross-cluster checksum
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
            ObTableCompactionInfo cur_compaction_info;
            if (OB_FAIL(table_ids.push_back(table_id))) {
              LOG_WARN("fail to push back", KR(ret), K(table_id));
            } else if (OB_FAIL(get_table_compaction_info(*simple_schema, table_compaction_map, cur_compaction_info))) {
              LOG_WARN("fail to get table compaction info", KR(ret), K(frozen_scn), KPC(simple_schema));
            } else if (cur_compaction_info.is_verified()) { // already finished verification, skip it!
            } else if (simple_schema->has_tablet()) {
              if (cur_compaction_info.is_index_ckm_verified()) {
                if (OB_FAIL(validate_cross_cluster_checksum(stop, frozen_scn, expected_epoch,
                            simple_schema, table_compaction_map, merge_time_statistics))) {
                  LOG_WARN("fail to validate cross-cluster checksum", KR(ret), K(stop),
                            K(frozen_scn), K(expected_epoch), K(table_id));
                }
              }
            } else { // like VIEW that has no tablet, no need to validate cross-cluster checksum
              // do nothing. should has been marked as VERIFIED by ObTabletChecksumValidator
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
        }  // end for loop

        if (OB_SUCC(ret) && (OB_SUCCESS == check_ret)) {
          if (OB_FAIL(remove_not_exist_table(table_ids, table_compaction_map))) {
            LOG_WARN("fail to remove not exist table", KR(ret), K_(tenant_id), K(frozen_scn));
          }
        }
      }
    }
  } else {  // no need to validate cross-cluster checksum
    // do nothing. index validator should already wrote ckm and updated report_scn
  }

  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = check_ret;
  }
  const int64_t cost_time_us = ObTimeUtil::current_time() - start_time_us;
  merge_time_statistics.update_merge_status_us_.cross_cluster_validator_us_ = cost_time_us;
  LOG_INFO("finish to check all table verification finished", KR(ret), K_(tenant_id), K(frozen_scn),
           K(expected_epoch), K(stop), K(cost_time_us));
  return ret;
}

int ObCrossClusterTabletChecksumValidator::validate_cross_cluster_checksum(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const int64_t expected_epoch,
    const ObSimpleTableSchemaV2 *simple_schema,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObMergeTimeStatistics &merge_time_statistics)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!frozen_scn.is_valid() || expected_epoch < 0 || OB_ISNULL(simple_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(frozen_scn), K(expected_epoch), KP(simple_schema));
  } else {
    FREEZE_TIME_GUARD;
    bool is_wait_tablet_checksum_timeout = false;
    // check whether all tablet checksum has already exist
    if (OB_FAIL(check_if_all_tablet_checksum_exist(frozen_scn))) {
      LOG_WARN("fail to check if all tablet checksum exist", KR(ret), K_(tenant_id), K(frozen_scn));
    } else if (!is_all_tablet_checksum_exist_) {
      // check whether waiting all tablet checksum has timed out
      is_wait_tablet_checksum_timeout = check_waiting_tablet_checksum_timeout();
      if (OB_UNLIKELY(is_wait_tablet_checksum_timeout)) {
        bool is_match = true;
        if (OB_FAIL(ObServiceEpochProxy::check_service_epoch(*sql_proxy_, tenant_id_,
                    ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, expected_epoch, is_match))) {
          LOG_WARN("fail to check freeze service epoch", KR(ret), K_(tenant_id), K(expected_epoch));
        } else if (!is_match) {
          ret = OB_FREEZE_SERVICE_EPOCH_MISMATCH;
          LOG_WARN("no need to validate cross-cluster checksum, cuz freeze_service_epoch mismatch",
                  K_(tenant_id), K(frozen_scn), K(expected_epoch));
        } else {
          // only LOG_ERROR when freeze_service_epoch match.
          //
          LOG_ERROR("waiting all tablet checksum has timed out, validate cross-cluster"
            " checksum with available tablet checksum" , K_(tenant_id), K(frozen_scn),
            K(expected_epoch), K_(major_merge_start_us), "current_time_us",
            ObTimeUtil::current_time());
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_all_tablet_checksum_exist_ || is_wait_tablet_checksum_timeout) { // all tablet checksum exist or timeout
      if (OB_FAIL(check_cross_cluster_checksum(*simple_schema, frozen_scn))) {
        if (OB_ITEM_NOT_MATCH == ret) {
          if (OB_TMP_FAIL(handle_table_can_not_verify(simple_schema->get_table_id(), table_compaction_map))) {
            LOG_WARN("fail to handle table can not verify", KR(ret), "table_id", simple_schema->get_table_id());
          } else {
            ret = OB_SUCCESS; // ignore ret
          }
        } else if (OB_CHECKSUM_ERROR == ret) {
          LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in "
            "cross-cluster checksum", KR(ret), K_(tenant_id), K(frozen_scn), KPC(simple_schema));
        } else {
          LOG_WARN("fail to check cross-cluster checksum", KR(ret), K_(tenant_id),
                  K(frozen_scn), KPC(simple_schema));
        }
      } else if (OB_FAIL(write_ckm_and_update_report_scn(stop, simple_schema, frozen_scn,
                         table_compaction_map, merge_time_statistics, expected_epoch))) {
        LOG_WARN("fail to write tablet checksum and update report_scn", KR(ret), K_(tenant_id),
                K(frozen_scn), KPC(simple_schema), K(expected_epoch));
      }
    } else if (TC_REACH_TIME_INTERVAL(10 * 60 * 1000 * 1000)) {  // 10 min
      LOG_WARN("can not check cross-cluster checksum now, please wait until first tablet "
               "in sys ls exists", K_(tenant_id), K(frozen_scn), KPC(simple_schema),
               K_(major_merge_start_us), "current_time_us", ObTimeUtil::current_time());
    }
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::check_cross_cluster_checksum(
    const ObSimpleTableSchemaV2 &simple_schema,
    const SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!simple_schema.has_tablet() || !frozen_scn.is_valid() || frozen_scn < SCN::min_scn())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(simple_schema), K(frozen_scn));
  } else {
    SMART_VARS_2((ObArray<ObTabletID>, tablet_ids), (ObArray<ObTabletLSPair>, pairs)) {
      FREEZE_TIME_GUARD;
      if (OB_FAIL(simple_schema.get_tablet_ids(tablet_ids))) {
        LOG_WARN("fail to get tablet_ids from table schema", KR(ret), K(simple_schema));
      } else if (OB_UNLIKELY(tablet_ids.count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablet_ids of current table schema", KR(ret), K_(tenant_id), K(simple_schema));
      } else if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(tenant_id_,
                  simple_schema.get_table_id(), *sql_proxy_, tablet_ids, pairs))) {
        LOG_WARN("fail to get tablet_ls pairs", KR(ret), K_(tenant_id), "table_id",
                  simple_schema.get_table_id());
      } else if (OB_UNLIKELY(pairs.count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret),
                  K_(tenant_id), "table_id", simple_schema.get_table_id(), K(tablet_ids));
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
          } else if ((0 == tablet_replica_checksum_items.count())
                     || (0 == tablet_checksum_items.count())) {
            ret = OB_ITEM_NOT_MATCH;
            LOG_WARN("fail to get checksum items", KR(ret), K_(tenant_id), K(frozen_scn),
                     K(tablet_replica_checksum_items), K(tablet_checksum_items));
          } else if (OB_FAIL(check_column_checksum(tablet_replica_checksum_items, tablet_checksum_items))) {
            if (OB_CHECKSUM_ERROR == ret) {
              LOG_ERROR("ERROR! ERROR! ERROR! checksum error in cross-cluster checksum", KR(ret),
                        K_(tenant_id), K(frozen_scn), K(simple_schema));
            } else {
              LOG_WARN("fail to check cross-cluster checksum", KR(ret), K_(tenant_id),
                       K(frozen_scn), K(simple_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::check_column_checksum(
    const ObArray<ObTabletReplicaChecksumItem> &tablet_replica_checksum_items,
    const ObArray<ObTabletChecksumItem> &tablet_checksum_items)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  hash::ObHashMap<ObTabletLSPair, ObTabletChecksumItem> tablet_checksum_items_map;
  if (OB_FAIL(tablet_checksum_items_map.create(500, "MFTatCkmMap", "MFTatCkmMap", tenant_id_))) {
    LOG_WARN("fail to create tablet checksum items map", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(convert_array_to_map(tablet_checksum_items, tablet_checksum_items_map))) {
    LOG_WARN("fail to convert array to map", KR(ret));
  } else {
    const int64_t tablet_replica_checksum_item_cnt = tablet_replica_checksum_items.count();
    for (int64_t i = 0; (i < tablet_replica_checksum_item_cnt) && OB_SUCC(ret); ++i) {
      const ObTabletReplicaChecksumItem &tablet_replica_checksum_item = tablet_replica_checksum_items.at(i);
      ObTabletLSPair pair(tablet_replica_checksum_item.tablet_id_, tablet_replica_checksum_item.ls_id_);
      ObTabletChecksumItem tablet_checksum_item;
      // ObHashMap::get_refactored calls assign function of key_type and value_type.
      // ObTabletChecksumItem::assign should ensure the item is valid, thus construct one valid item
      if (OB_FAIL(construct_valid_tablet_checksum_item(tablet_checksum_item))) {
        LOG_WARN("fail to construct valid tablet checksum item", KR(ret));
      } else if (OB_FAIL(tablet_checksum_items_map.get_refactored(pair, tablet_checksum_item))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // ignore ret and skip this tablet checksum. this may be caused by timeouted wait of tablet checksum
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get_refactored", KR(ret), K(pair));
        }
      } else if (OB_FAIL(tablet_checksum_item.verify_tablet_column_checksum(tablet_replica_checksum_item))) {
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
  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = OB_CHECKSUM_ERROR;
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::convert_array_to_map(
    const ObArray<ObTabletChecksumItem> &tablet_checksum_items,
    hash::ObHashMap<ObTabletLSPair, ObTabletChecksumItem> &tablet_checksum_items_map)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_checksum_item_cnt = tablet_checksum_items.count();
  for (int64_t i = 0; (i < tablet_checksum_item_cnt) && OB_SUCC(ret); ++i) {
    const ObTabletChecksumItem &item = tablet_checksum_items.at(i);
    ObTabletLSPair pair(item.tablet_id_, item.ls_id_);
    if (OB_FAIL(tablet_checksum_items_map.set_refactored(pair, item, false/*overwrite*/))) {
      LOG_WARN("fail to set_refactored", KR(ret), K(pair), K(item));
    }
  }
  return ret;
}

int ObCrossClusterTabletChecksumValidator::construct_valid_tablet_checksum_item(
    ObTabletChecksumItem &tablet_checksum_item)
{
  int ret = OB_SUCCESS;
  ObTabletID fake_tablet_id(1);
  ObLSID fake_ls_id(1);
  ObTabletReplicaReportColumnMeta fake_column_meta;
  tablet_checksum_item.tablet_id_ = fake_tablet_id;
  tablet_checksum_item.ls_id_ = fake_ls_id;
  ObArray<int64_t> fake_column_checksums;
  if (OB_FAIL(fake_column_checksums.push_back(0))) {
    LOG_WARN("fail to push back column checksums", KR(ret));
  } else if (OB_FAIL(fake_column_meta.init(fake_column_checksums))) {
    LOG_WARN("fail to init column meta", KR(ret));
  } else if (OB_FAIL(tablet_checksum_item.column_meta_.assign(fake_column_meta))) {
    LOG_WARN("fail to assign column meta", KR(ret), K(fake_column_meta));
  }
  return ret;
}

// 1. is_all_tablet_checksum_exist_ = true: do nothing
// 2. is_all_tablet_checksum_exist_ = false: check and update is_all_tablet_checksum_exist_
int ObCrossClusterTabletChecksumValidator::check_if_all_tablet_checksum_exist(
    const SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  // check only once every 10 seconds
  if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {  // 10s
    bool is_sync = false;
    ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
    ObArray<uint64_t> frozen_scn_vals;
    if (is_all_tablet_checksum_exist_) {
      // do nothing
    } else if (OB_FAIL(freeze_info_proxy.get_frozen_scn_larger_or_equal_than(
                      *sql_proxy_, frozen_scn, frozen_scn_vals))) {
      LOG_WARN("fail to get frozen scn", KR(ret), K_(tenant_id), K(frozen_scn));
    } else if (OB_UNLIKELY(frozen_scn_vals.count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no larger frozen scn exists", KR(ret), K_(tenant_id), K(frozen_scn));
    } else if (OB_FAIL(ObTabletChecksumOperator::is_all_tablet_checksum_sync(*sql_proxy_,
                      tenant_id_, frozen_scn_vals, is_sync))) {
      LOG_WARN("fail to check is first tablet in first ls exist", KR(ret), K_(tenant_id), K(frozen_scn));
    } else {
      // update is_all_tablet_checksum_exist_ according to the result of
      // ObTabletChecksumOperator::is_all_tablet_checksum_sync
      is_all_tablet_checksum_exist_ = is_sync;
      LOG_INFO("succ to check if all tablet checksum exist", K_(tenant_id), K(frozen_scn),
               K_(is_all_tablet_checksum_exist));
    }
  }
  return ret;
}

bool ObCrossClusterTabletChecksumValidator::check_waiting_tablet_checksum_timeout() const
{
  const int64_t MAX_TABLET_CHECKSUM_WAIT_TIME_US = 36 * 3600 * 1000 * 1000L;  // 36 hours
  const int64_t total_wait_time_us = (ObTimeUtil::current_time() - major_merge_start_us_);
  return (total_wait_time_us > MAX_TABLET_CHECKSUM_WAIT_TIME_US);
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

void ObIndexChecksumValidator::set_need_val_cross_cluster_ckm(
     const bool is_need_val_cross_cluster_ckm)
{
  is_need_val_cross_cluster_ckm_ = is_need_val_cross_cluster_ckm;
}

int ObIndexChecksumValidator::check_all_table_verification_finished(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    int64_t &table_count,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    const ObIArray<uint64_t> &ori_table_ids,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  bool already_print = false;

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
        int64_t last_epoch_check_us = ObTimeUtil::fast_current_time();
        for (int64_t i = 0; (i < table_count) && OB_SUCC(ret) && !stop; ++i) {
          const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
          if (OB_ISNULL(simple_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
          } else if (OB_FAIL(ObMajorFreezeUtil::check_epoch_periodically(*sql_proxy_, tenant_id_,
                             expected_epoch, last_epoch_check_us))) {
            LOG_WARN("fail to check freeze service epoch", KR(ret), K_(tenant_id), K(stop));
          } else {
            const uint64_t table_id = simple_schema->get_table_id();
            ObTableCompactionInfo cur_compaction_info;
            if (OB_FAIL(table_ids.push_back(table_id))) {
              LOG_WARN("fail to push back", KR(ret), K(table_id));
            } else if (OB_FAIL(get_table_compaction_info(*simple_schema, table_compaction_map, cur_compaction_info))) {
              LOG_WARN("fail to get table compaction info", KR(ret), K(frozen_scn), KPC(simple_schema));
            } else if (cur_compaction_info.is_index_ckm_verified()
                      || cur_compaction_info.is_verified()) { // already finished verification, skip it!
            } else if (simple_schema->is_index_table()) {
              if (simple_schema->can_read_index()) {
                // 1. for index table can read, may need to check column checksum
                if (OB_FAIL(handle_index_table(frozen_scn, cur_compaction_info, simple_schema,
                    schema_guard, table_compaction_map, stop, merge_time_statistics, expected_epoch))) {
                  LOG_WARN("fail to handle index table", KR(ret), K(frozen_scn), K(simple_schema));
                }
              } else { // !simple_schema->can_read_index()
                // 2. for index table can not read, directly mark it as VERIFIED
                // do not check compaction_scn and validate checksum of can not read index's tablets.
                // although update_all_tablets_report_scn will update its report_scn. the storage
                // layer may schedule major compaction and increase compaction_scn of this index's
                // tablets later.
                if (OB_FAIL(handle_table_can_not_verify(table_id, table_compaction_map))) {
                  LOG_WARN("fail to handle table can not verify", KR(ret));
                }
              }
            }
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(try_print_first_unverified_info(simple_schema, table_schemas, table_compaction_map, already_print))) {
              LOG_WARN("fail to try print first unverified info", KR(tmp_ret), K(table_id));
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
          // 1. data table with index: if all its index tables finished verification, mark it as INDEX_CKM_VERIFIED.
          // 2. data table without index: if it finished compaction, directly mark it as INDEX_CKM_VERIFIED.
          // 3. other types of tables that are not index: if it finished compaction, directly mark it as INDEX_CKM_VERIFIED.
          if (OB_FAIL(handle_data_table(stop, frozen_scn, table_ids, table_schemas, ori_table_ids,
                      table_compaction_map, merge_time_statistics, schema_guard, expected_epoch))) {
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
  LOG_INFO("finish to check all table verification finished", KR(ret), K_(tenant_id), K(frozen_scn),
           K(expected_epoch), K(stop), K(cost_time_us));
  return ret;
}

int ObIndexChecksumValidator::update_data_table_verified(
    const uint64_t data_table_id,
    const ObTableCompactionInfo &data_table_compaction,
    const SCN &frozen_scn,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    const volatile bool &stop,
    const ObSimpleTableSchemaV2 *simple_schema,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (data_table_compaction.is_index_ckm_verified()
      || data_table_compaction.is_verified()) { // skip if already finished verification
  } else if (data_table_compaction.is_compacted()) {
    if (OB_FAIL(handle_table_verification_finished(data_table_id, frozen_scn, table_compaction_map,
                stop, simple_schema, merge_time_statistics, expected_epoch))) {
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
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    const volatile bool &stop,
    const ObSimpleTableSchemaV2 *simple_schema,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObTableCompactionInfo cur_compaction_info;
  if (OB_FAIL(table_compaction_map.get_refactored(table_id, cur_compaction_info))) {
    LOG_WARN("fail to get refactored", KR(ret), K(table_id));
  } else if (cur_compaction_info.is_index_ckm_verified()
              || cur_compaction_info.is_verified()) { // skip if finished verification
  } else if (!cur_compaction_info.is_compacted()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table must finish compaction when arriving here", KR(ret), K(table_id), K(cur_compaction_info));
  } else {
    cur_compaction_info.set_index_ckm_verified();
    if (OB_FAIL(table_compaction_map.set_refactored(table_id, cur_compaction_info, true/*overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(cur_compaction_info));
    } else if (!is_need_val_cross_cluster_ckm_) {
      // if no need to validate cross cluster checksum, directly write tablet checksum and update
      // report_scn after validate index checksum. So as to avoid repeated checksum verification
      // as much as possible in case of switch leader.
      if (OB_FAIL(write_ckm_and_update_report_scn(stop, simple_schema, frozen_scn,
                         table_compaction_map, merge_time_statistics, expected_epoch))) {
        LOG_WARN("fail to write tablet checksum and update report_scn", KR(ret), K_(tenant_id),
                 K(frozen_scn), KPC(simple_schema), K(expected_epoch));
        // revert status to COMPACTED, so as to retry write_ckm_and_update_report_scn of this table
        int tmp_ret = OB_SUCCESS;
        cur_compaction_info.set_compacted();
        if (OB_TMP_FAIL(table_compaction_map.set_refactored(table_id, cur_compaction_info, true/*overwrite*/))) {
          LOG_WARN("fail to set refactored", KR(tmp_ret), K(table_id), K(cur_compaction_info));
        }
      }
    }
  }
  return ret;
}

int ObIndexChecksumValidator::handle_data_table(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const ObIArray<uint64_t> &table_ids,
    const ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas,
    const ObIArray<uint64_t> &ori_table_ids,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObMergeTimeStatistics &merge_time_statistics,
    ObSchemaGetterGuard &schema_guard,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<uint64_t> table_id_set;
  hash::ObHashSet<uint64_t> data_tables_to_update_set;
  SMART_VAR(ObArray<uint64_t>, filtered_tables_to_update) {
    if (OB_FAIL(table_id_set.create(10000, "MFTabIdSet", "MFTabIdSet", tenant_id_))) {
      LOG_WARN("fail to create table id set", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(convert_array_to_set(table_ids, table_id_set))) {
      LOG_WARN("fail to convert array to set", KR(ret));
    } else if (OB_FAIL(data_tables_to_update_set.create(10000, "MFTabUpdSet", "MFTabUpdSet", tenant_id_))) {
      LOG_WARN("fail to create data tables to update set", KR(ret), K_(tenant_id));
    }
    // check tables that are not index table, return those need to be marked as INDEX_CKM_VERIFIED
    else if (OB_FAIL(check_data_table(table_schemas, table_compaction_map, ori_table_ids, data_tables_to_update_set))) {
      LOG_WARN("fail to check data table with index", KR(ret), K_(tenant_id), K(frozen_scn));
    }
    // filter table_ids exist in @data_tables_to_update_set and @table_ids from table_compaction_map
    hash::ObHashMap<uint64_t, ObTableCompactionInfo>::iterator iter = table_compaction_map.begin();
    for (; !stop && OB_SUCC(ret) && (iter != table_compaction_map.end()); ++iter) {
      const uint64_t cur_table_id = iter->first;
      bool is_exist_in_table_id_set = false;
      bool is_exist_in_update_set = false;
      if (OB_FAIL(check_exist_in_table_set(cur_table_id, table_id_set, is_exist_in_table_id_set))) {
        LOG_WARN("fail to check if exist in table set", KR(ret), K(cur_table_id));
      } else if (OB_FAIL(check_exist_in_table_set(cur_table_id, data_tables_to_update_set, is_exist_in_update_set))) {
        LOG_WARN("fail to check if exist in table set", KR(ret), K(cur_table_id));
      } else if (is_exist_in_table_id_set && is_exist_in_update_set) {
        if (OB_FAIL(filtered_tables_to_update.push_back(cur_table_id))) {
          LOG_WARN("fail to push back", KR(ret), K(cur_table_id));
        }
      }
    }
    // mark these table as INDEX_CKM_VERIFIED
    const int64_t filtered_tables_to_update_cnt = filtered_tables_to_update.count();
    for (int64_t i = 0; !stop && OB_SUCC(ret) && (i < filtered_tables_to_update_cnt); ++i) {
      const uint64_t cur_table_id = filtered_tables_to_update.at(i);
      ObTableCompactionInfo cur_table_compaction_info;
      const ObSimpleTableSchemaV2 *simple_schema = nullptr;
      if (OB_FAIL(table_compaction_map.get_refactored(cur_table_id, cur_table_compaction_info))) {
        LOG_WARN("fail to get refactored", KR(ret), K(cur_table_id));
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, cur_table_id, simple_schema))) {
        LOG_WARN("fail to get simple table schema", KR(ret), K_(tenant_id), K(cur_table_id));
      } else if (OB_ISNULL(simple_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("simple schema is null", KR(ret), K_(tenant_id), K(cur_table_id));
      } else if (OB_FAIL(update_data_table_verified(cur_table_id, cur_table_compaction_info,
          frozen_scn, table_compaction_map, stop, simple_schema, merge_time_statistics, expected_epoch))) {
        LOG_WARN("fail to update data table to verified status", KR(ret), K(cur_table_id),
                 K(frozen_scn), K(cur_table_compaction_info));
      }
    }
  }
  return ret;
}

int ObIndexChecksumValidator::check_data_table(
    const ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    const ObIArray<uint64_t> &ori_table_ids,
    hash::ObHashSet<uint64_t> &data_tables_to_update_set)
{
  int ret = OB_SUCCESS;
  int64_t table_count = table_schemas.count();
  // push_back data tables with index, data tables without index, and other types of non-index
  // tables that finished compaction into @data_tables_to_update_set
  for (int64_t i = 0; (i < table_count) && OB_SUCC(ret); ++i) {
    const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
    if (OB_ISNULL(simple_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
    } else if (!simple_schema->is_index_table()) { // data table
      const uint64_t table_id = simple_schema->get_table_id();
      ObTableCompactionInfo table_compaction_info;
      if (OB_FAIL(table_compaction_map.get_refactored(table_id, table_compaction_info))) {
        LOG_WARN("fail to get refactored", KR(ret), K(table_id));
      } else if (table_compaction_info.is_compacted()) {
        if (OB_FAIL(data_tables_to_update_set.set_refactored(table_id, 0/*overwrite*/))) {
          LOG_WARN("fail to push back", KR(ret), K(table_id));
        }
      }
    }
  }
  hash::ObHashSet<uint64_t> ori_table_id_set;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ori_table_id_set.create(10000, "MFTabIdSet", "MFTabIdSet", tenant_id_))) {
      LOG_WARN("fail to create ori table id set", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(convert_array_to_set(ori_table_ids, ori_table_id_set))) {
      LOG_WARN("fail to convert array to set", KR(ret));
    }
  }
  // remove data tables whose index tables do not finish verification from @data_tables_to_update_set
  for (int64_t i = 0; (i < table_count) && OB_SUCC(ret); ++i) {
    const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
    if (OB_ISNULL(simple_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
    } else if (simple_schema->is_index_table()) {
      const uint64_t index_table_id = simple_schema->get_table_id();
      const uint64_t data_table_id = simple_schema->get_data_table_id();
      bool is_exist = false;
      if (OB_FAIL(check_exist_in_table_set(index_table_id, ori_table_id_set, is_exist))) {
        LOG_WARN("fail to check exist in table set", KR(ret), K(index_table_id));
      } else if (simple_schema->can_read_index() && is_exist) {
        ObTableCompactionInfo index_table_compaction_info;
        if (OB_FAIL(table_compaction_map.get_refactored(index_table_id, index_table_compaction_info))) {
          LOG_WARN("fail to get refactored", KR(ret), K(index_table_id));
        } else if (!index_table_compaction_info.is_index_ckm_verified()
                  && !index_table_compaction_info.is_verified()) { // index_table is not verified
          int64_t idx = -1;
          if (OB_FAIL(data_tables_to_update_set.exist_refactored(data_table_id))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS; // ignore ret
              if (OB_FAIL(data_tables_to_update_set.erase_refactored(data_table_id))) {
                LOG_WARN("fail to erase_refactored", KR(ret), K(data_table_id));
              }
            } else if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS; // ignore ret
            } else {
              LOG_WARN("fail to check exist", KR(ret), K(data_table_id));
            }
          }
        }
      } else { // !simple_schema->can_read_index() || index_table_id not exist in ori_table_id_set)
        // ignore index table that can not read or does not exist in ori_table_ids
      }
    }
  }
  return ret;
}

int ObIndexChecksumValidator::handle_index_table(
    const SCN &frozen_scn,
    const ObTableCompactionInfo &index_compaction_info,
    const ObSimpleTableSchemaV2 *index_simple_schema,
    ObSchemaGetterGuard &schema_guard,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    const volatile bool &stop,
    ObMergeTimeStatistics &merge_time_statistics,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!frozen_scn.is_valid() || OB_ISNULL(index_simple_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn), KP(index_simple_schema));
  } else {
    const uint64_t index_table_id = index_simple_schema->get_table_id();
    const uint64_t data_table_id = index_simple_schema->get_data_table_id();
    const ObSimpleTableSchemaV2 *data_simple_schema = nullptr;
    if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, data_table_id, data_simple_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(data_table_id));
    } else if (OB_ISNULL(data_simple_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("fail to get data table schema", KR(ret), K_(tenant_id), K(index_table_id), K(data_table_id));
    } else {
      ObTableCompactionInfo data_compaction_info;
      if (OB_FAIL(get_table_compaction_info(*data_simple_schema, table_compaction_map, data_compaction_info))) {
        LOG_WARN("fail to get table compaction info", KR(ret), K(frozen_scn), KPC(data_simple_schema));
      } else if (data_compaction_info.is_index_ckm_verified() || data_compaction_info.is_verified()) {
        // if a data table finished verification, then create index on this data table.
        // we should skip verification for this index table, cuz the data table may already
        // launched another medium compaction.
        LOG_INFO("index table is not verified while data table is already verified, skip"
                 " verification for this index table", K(index_table_id), K(data_table_id),
                 K(index_compaction_info), K(data_compaction_info));
        if (index_compaction_info.is_compacted()) {
          if (OB_FAIL(handle_table_verification_finished(index_table_id, frozen_scn, table_compaction_map,
                      stop, index_simple_schema, merge_time_statistics, expected_epoch))) {
            LOG_WARN("fail to handle index table compaction finished", KR(ret), K(index_table_id), K(frozen_scn));
          }
        }
      } else if (index_simple_schema->has_tablet()) {
        if (!index_compaction_info.is_compacted() || !data_compaction_info.is_compacted()) {
        } else if (index_compaction_info.is_compacted() && data_compaction_info.is_compacted()) {
          #ifdef ERRSIM
              ret = OB_E(EventTable::EN_MEDIUM_VERIFY_GROUP_SKIP_SET_VERIFY) OB_SUCCESS;
              if (OB_FAIL(ret)) {
                if (!is_inner_table(index_table_id)) {
                  ret = OB_EAGAIN;
                  STORAGE_LOG(INFO, "ERRSIM EN_MEDIUM_VERIFY_GROUP_SKIP_SET_VERIFY failed", K(ret));
                } else {
                  ret = OB_SUCCESS;
                }
              }
          #endif
          // set it to false, if succ to handle_table_can_not_verify
          bool need_update_map = true;
          // both tables' all tablets finished compaction, validate column checksum if need_validate()
          if (need_validate()) {
            FREEZE_TIME_GUARD;
            if (FAILEDx(ObTabletReplicaChecksumOperator::check_column_checksum(tenant_id_,
                  *data_simple_schema, *index_simple_schema, frozen_scn, *sql_proxy_))) {
              if ((OB_ITEM_NOT_MATCH == ret) || (OB_TABLE_NOT_EXIST == ret)) {
                if (OB_TMP_FAIL(handle_table_can_not_verify(index_table_id, table_compaction_map))) {
                  LOG_WARN("fail to handle table can not verify", KR(tmp_ret), K(index_table_id));
                } else {
                  ret = OB_SUCCESS;
                  need_update_map = false;
                }
              } else if (OB_CHECKSUM_ERROR == ret) {
                LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in index checksum",
                  KR(ret), KPC(data_simple_schema), K_(tenant_id), K(frozen_scn), KPC(index_simple_schema));
              } else {
                LOG_WARN("fail to check index column checksum", KR(ret), K_(tenant_id), KPC(data_simple_schema),
                  KPC(index_simple_schema));
              }
            }
          }
          // after index checksum verification, mark it as INDEX_CKM_VERIFIED
          if (OB_SUCC(ret) && need_update_map) {
            if (OB_FAIL(handle_table_verification_finished(index_table_id, frozen_scn, table_compaction_map,
                        stop, index_simple_schema, merge_time_statistics, expected_epoch))) {
              LOG_WARN("fail to handle table verification finished", KR(ret), K(index_table_id), K(frozen_scn));
            }
          }
        }
      } else { // virtual index table has no tablet, no need to execute index checksum verification.
        // do nothing. should has been marked as VERIFIED by ObTabletChecksumValidator
      }
    }
  }
  return ret;
}

int ObIndexChecksumValidator::try_print_first_unverified_info(
    const ObSimpleTableSchemaV2 *simple_schema,
    const ObArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas,
    const hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    bool &already_print)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(simple_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(simple_schema));
  } else if (already_print) { // do nothing. only print infos about the first unverified table
  } else {
    const uint64_t table_id = simple_schema->get_table_id();
    ObTableCompactionInfo cur_compaction_info;
    if (OB_FAIL(table_compaction_map.get_refactored(table_id, cur_compaction_info))) {
      LOG_WARN("fail to get table_compaction_info from table_compaction_map", KR(ret), K(table_id));
    } else if (cur_compaction_info.is_index_ckm_verified() || cur_compaction_info.is_verified()) {
      // 1. already finished verification, no need to print table compaction info
    } else {
      // 2. has not finished verification, need to print table compaction info
      if (simple_schema->is_index_table()) {
        // 2.1 index table, print index table compaction info and data table compaction info
        ObTableCompactionInfo data_compaction_info;
        const uint64_t data_table_id = simple_schema->get_data_table_id();
        if (OB_FAIL(table_compaction_map.get_refactored(data_table_id, data_compaction_info))) {
          LOG_WARN("fail to get table_compaction_info from table_compaction_map", KR(ret), K(data_table_id));
        } else {
          LOG_INFO("index table is unverified", K(cur_compaction_info), K(data_compaction_info));
        }
      } else {
        // 2.2 data table, print data table compaction info and its index table compaction infos
        SMART_VAR(ObArray<ObTableCompactionInfo>, index_table_compaction_infos) {
          const int64_t table_count = table_schemas.count();
          // traverse table_schemas to find all indexs of this data table
          for (int64_t i = 0; OB_SUCC(ret) && (i < table_count); ++i) {
            const ObSimpleTableSchemaV2 *cur_simple_schema = table_schemas.at(i);
            if (OB_ISNULL(cur_simple_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
            } else if (cur_simple_schema->is_index_table()) {
              const uint64_t data_table_id = cur_simple_schema->get_data_table_id();
              if (data_table_id == table_id) {
                const uint64_t index_table_id = cur_simple_schema->get_table_id();
                ObTableCompactionInfo index_compaction_info;
                if (OB_FAIL(table_compaction_map.get_refactored(index_table_id, index_compaction_info))) {
                  LOG_WARN("fail to get table_compaction_info from table_compaction_map", KR(ret), K(index_table_id));
                } else if (OB_FAIL(index_table_compaction_infos.push_back(index_compaction_info))) {
                  LOG_WARN("fail to push back", KR(ret), K(index_compaction_info));
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            LOG_INFO("data table is unverified", K(cur_compaction_info), K(index_table_compaction_infos));
          }
        }
      }
      if (OB_SUCC(ret)) {
        already_print = true;
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
