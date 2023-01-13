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
#include "rootserver/ob_root_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/backup/ob_backup_manager.h"
#include "share/ob_tablet_replica_checksum_operator.h"
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
    sql_proxy_ = &sql_proxy;
    zone_merge_mgr_ = &zone_merge_mgr;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

bool ObChecksumValidatorBase::is_primary_cluster() const
{
  bool is_primary_cluster = true;
  if (PRIMARY_CLUSTER != ObClusterInfoGetter::get_cluster_role_v2()) {
    is_primary_cluster = false;
  }
  return is_primary_cluster;
}

bool ObChecksumValidatorBase::is_standby_cluster() const
{
  bool is_standby_cluster = true;
  if (STANDBY_CLUSTER != ObClusterInfoGetter::get_cluster_role_v2()) {
    is_standby_cluster = false;
  }
  return is_standby_cluster;
}

///////////////////////////////////////////////////////////////////////////////

bool ObCrossClusterTableteChecksumValidator::need_validate() const
{
  bool need_validate = false;
  if (is_inited_ && is_standby_cluster()) {
    need_validate = true;
  }
  return need_validate;
}

int ObCrossClusterTableteChecksumValidator::validate_checksum(const SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  if (!frozen_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (!is_standby_cluster()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("can only check cross cluster checksum in standby cluster", KR(ret));
  } else if (OB_FAIL(check_cross_cluster_checksum(frozen_scn))) {
    LOG_WARN("fail to check cross cluster checksum", KR(ret), K_(tenant_id), K(frozen_scn));
  }
  return ret;
}

int ObCrossClusterTableteChecksumValidator::check_cross_cluster_checksum(
    const SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;

  int64_t check_cnt = 0;
  HEAP_VAR(ObTabletReplicaChecksumIterator, tablet_replica_checksum_iter) {
    HEAP_VAR(ObTabletChecksumIterator, tablet_checksum_iter) {
      if (OB_FAIL(tablet_replica_checksum_iter.init(tenant_id_, sql_proxy_))) {
        LOG_WARN("fail to init tablet_replica_checksum iter", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(tablet_checksum_iter.init(tenant_id_, sql_proxy_))) {
        LOG_WARN("fail to init tablet checksum iterator", KR(ret), K_(tenant_id));
      } else {
        tablet_checksum_iter.set_compaction_scn(frozen_scn);
        tablet_replica_checksum_iter.set_compaction_scn(frozen_scn);

        int cmp_ret = 0;
        ObTabletChecksumItem tablet_checksum_item;
        ObTabletReplicaChecksumItem tablet_replica_checksum_item;
        while (OB_SUCC(ret)) {
          tablet_checksum_item.reset();
          if (OB_FAIL(tablet_checksum_iter.next(tablet_checksum_item))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to iter next tablet checksum item", KR(ret), K_(tenant_id));
            }
          } else {
            do {
              if (cmp_ret >= 0) { // iterator all tablet replica checksum util next different tablet.
                tablet_replica_checksum_item.reset();
                if (OB_FAIL(tablet_replica_checksum_iter.next(tablet_replica_checksum_item))) {
                  if (OB_ITER_END != ret) {
                    LOG_WARN("fail to iter next tablet checksum item", KR(ret), K_(tenant_id));
                  }
                } 
              }

              if (OB_FAIL(ret)) {
              } else if (0 == (cmp_ret = tablet_checksum_item.compare_tablet(tablet_replica_checksum_item))) {
                if (OB_FAIL(tablet_checksum_item.verify_tablet_checksum(tablet_replica_checksum_item))) {
                  ++check_cnt;
                  if (OB_CHECKSUM_ERROR == ret) {
                    LOG_ERROR("ERROR! ERROR! ERROR! checksum error in cross-cluster checksum", 
                      K(tablet_checksum_item), K(tablet_replica_checksum_item));
                    check_ret = OB_CHECKSUM_ERROR;
                    ret = OB_SUCCESS; // continue checking next checksum
                  } else {
                    LOG_WARN("unexpected error in cross-cluster checksum", KR(ret), 
                      K(tablet_checksum_item), K(tablet_replica_checksum_item));
                  }
                }
              }
            } while ((cmp_ret >= 0) && OB_SUCC(ret));
          }
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = OB_CHECKSUM_ERROR;
  }
  LOG_INFO("finish verifying cross-cluster checksum", KR(ret), KR(check_ret), K_(tenant_id), 
    K(frozen_scn), K(check_cnt));
  return ret;
}

int ObCrossClusterTableteChecksumValidator::write_tablet_checksum_item()
{
  int ret = OB_SUCCESS;

  HEAP_VAR(ObTabletReplicaChecksumIterator, sync_checksum_iter) {
    if (OB_FAIL(sync_checksum_iter.init(tenant_id_, sql_proxy_))) {
      LOG_WARN("fail to init tablet replica checksum iterator for sync", KR(ret), K_(tenant_id));
    } else {
      ObTabletReplicaChecksumItem curr_replica_item;
      ObTabletReplicaChecksumItem prev_replica_item;
      ObTabletChecksumItem tmp_checksum_item;
      // mark_end_item is the 'first_ls & first_tablet' checksum item. If we get this checksum item,
      // we need to insert it into __all_tablet_checksum table at last. In this case, if we get this
      // tablet's checksum item in table, we can ensure all checksum items have already been inserted.
      ObTabletChecksumItem mark_end_item; 
      ObArray<ObTabletChecksumItem> tablet_checksum_items;
      while (OB_SUCC(ret)) {
        curr_replica_item.reset();
        if (OB_FAIL(sync_checksum_iter.next(curr_replica_item))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to iter next tablet checksum item", KR(ret), K_(tenant_id));
          }
        } else if (!curr_replica_item.is_key_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet replica checksum is not valid", KR(ret), K(curr_replica_item));
        } else {
          if (curr_replica_item.is_same_tablet(prev_replica_item)) {
            continue;
          } else {
            if (is_first_tablet_in_sys_ls(curr_replica_item)) {
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

        if (OB_ITER_END == ret) { // already iter all checksum item
          ret = OB_SUCCESS;
          if (OB_UNLIKELY(!mark_end_item.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected err about mark_end_item", KR(ret), K(mark_end_item));
          } else if (OB_FAIL(tablet_checksum_items.push_back(mark_end_item))) {
            LOG_WARN("fail to push back tablet checksum item", KR(ret), K(mark_end_item));
          }
          if (tablet_checksum_items.count() > 0) {
            if (FAILEDx(ObTabletChecksumOperator::insert_tablet_checksum_items(*sql_proxy_, tenant_id_, 
                tablet_checksum_items))) {
              LOG_WARN("fail to insert tablet checksum items", KR(ret), K_(tenant_id));
            }
          }
          break;
        } else if (tablet_checksum_items.count() >= MAX_BATCH_INSERT_COUNT) { // insert part checksum items once
          if (FAILEDx(ObTabletChecksumOperator::insert_tablet_checksum_items(*sql_proxy_, tenant_id_, 
              tablet_checksum_items))) {
            LOG_WARN("fail to insert tablet checksum items", KR(ret), K_(tenant_id));
          } else {
            tablet_checksum_items.reuse();
          }
        }
      }
    }
  }
  
  return ret;
}

bool ObCrossClusterTableteChecksumValidator::is_first_tablet_in_sys_ls(const ObTabletReplicaChecksumItem &item) const
{
  // mark tablet_id=1 && ls_id=1 as end flag
  return (item.ls_id_.is_sys_ls()) && (item.tablet_id_.id() == ObTabletID::MIN_VALID_TABLET_ID);
}

///////////////////////////////////////////////////////////////////////////////

bool ObIndexChecksumValidator::need_validate() const
{
  bool need_validate = false;
  if (is_inited_ && is_primary_cluster()) {
    need_validate = true;
  }
  return need_validate;
}

int ObIndexChecksumValidator::validate_checksum(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    int64_t &table_count,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (stop) {
    ret = OB_CANCELED;
    LOG_WARN("already stop", KR(ret), K_(tenant_id));
  } else if ((!frozen_scn.is_valid()) || (tablet_compaction_map.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (!is_primary_cluster()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("can only check index column checksum in primary cluster", KR(ret));
  } else if (OB_FAIL(check_all_table_verification_finished(stop, frozen_scn, tablet_compaction_map,
               table_count, table_compaction_map, expected_epoch))) {
    LOG_WARN("fail to check all table verification finished", KR(ret), K_(tenant_id), K(frozen_scn));
  }
  return ret;
}

int ObIndexChecksumValidator::check_all_table_verification_finished(
    const volatile bool &stop,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    int64_t &table_count,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;

  table_count = 0;
  ObSchemaGetterGuard schema_guard;
  SMART_VARS_2((ObArray<const ObSimpleTableSchemaV2 *>, table_schemas),
               (ObArray<uint64_t>, table_ids)) {
    if (stop) {
      ret = OB_CANCELED;
      LOG_WARN("already stop", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id_, table_schemas))) {
      LOG_WARN("fail to get tenant table schemas", KR(ret), K_(tenant_id));
    } else {
      table_count = table_schemas.count();
      for (int64_t i = 0; (i < table_schemas.count()) && OB_SUCC(ret) && !stop; ++i) {
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
          } else if (OB_FAIL(check_table_compaction_finished(*table_schema, frozen_scn, tablet_compaction_map,
              table_compaction_map, cur_compaction_info))) {
            LOG_WARN("fail to check table compaction finished", KR(ret), K(frozen_scn), KPC(table_schema));
          }
          if (OB_NOT_SUPPORTED == ret) {
            // tablets of old table_schema cannot be found in __all_tablet_to_ls, which results in
            // ret = OB_NOT_SUPPORTED. ignore ret and go on verification.
            // this logic will be removed after log4100 branch is merged into master branch.
            ret = OB_SUCCESS;
          }
          if (OB_FAIL(ret)) {
          } else if (cur_compaction_info.is_verified()) { // already finished verification, skip it!
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

              if (OB_FAIL(check_table_compaction_finished(*data_table_schema, frozen_scn, tablet_compaction_map,
                  table_compaction_map, data_compaction_info))) {
                LOG_WARN("fail to check table compaction finished", KR(ret), K(frozen_scn), KPC(data_table_schema));
              }
              if (OB_NOT_SUPPORTED == ret) {
                // tablets of old table_schema cannot be found in __all_tablet_to_ls, which results in
                // ret = OB_NOT_SUPPORTED. ignore ret and go on verification.
                // this logic will be removed after log4100 branch is merged into master branch.
                ret = OB_SUCCESS;
              }
              if (OB_FAIL(ret)) {
              } else if (data_compaction_info.is_verified()) {
                // if a data table finished verification, then create index on this data table.
                // we should skip verification for this index table, cuz the data table may already
                // launched another medium compaction.
                LOG_INFO("index table is not verified while data table is already verified, skip"
                  " verification for this index table", K(table_id), K(data_table_id),
                  K(cur_compaction_info), K(data_compaction_info));
                if (cur_compaction_info.finish_compaction()) {
                  if (OB_FAIL(handle_table_compaction_finished(table_schema, frozen_scn, table_compaction_map))) {
                    LOG_WARN("fail to handle index table compaction finished", KR(ret), K(table_id), K(frozen_scn));
                  }
                }
              } else if (table_schema->has_tablet()) {
                data_compaction_info.is_valid_data_table_ = true;
                if (!cur_compaction_info.finish_compaction() || !data_compaction_info.finish_compaction()) {
                  // data table or index table does not finish compaction.
                  data_compaction_info.all_index_verified_ = false;
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
                  // both tables' all tablets finished compaction, we should validate column checksum.
                  FREEZE_TIME_GUARD;
                  if (OB_FAIL(ObTabletReplicaChecksumOperator::check_column_checksum(tenant_id_,
                      *data_table_schema, *table_schema, frozen_scn, *sql_proxy_, expected_epoch))) {
                    if (OB_CHECKSUM_ERROR == ret) {
                      LOG_ERROR("ERROR! ERROR! ERROR! checksum error in index checksum", KR(ret), K(*data_table_schema),
                        K_(tenant_id), K(frozen_scn), K(*table_schema));
                    } else {
                      LOG_WARN("fail to check index column checksum", KR(ret), K_(tenant_id), K(*data_table_schema),
                        K(*table_schema));
                    }
                  // after index checksum verification, we should execute tablet_replica checksum verification on the
                  // index table, then mark it as VERIFIED
                  } else if (OB_FAIL(handle_table_compaction_finished(table_schema, frozen_scn, table_compaction_map))) {
                    LOG_WARN("fail to handle table compaction finished", KR(ret), K(table_id), K(frozen_scn));
                  }
                } else if (cur_compaction_info.can_skip_verifying() || data_compaction_info.can_skip_verifying()) {
                  // if one of them can skip verifying, that means we don't need to execute index checksum verification.
                  // Mark index table as VERIFIED directly.
                  if (OB_FAIL(handle_table_compaction_finished(table_schema, frozen_scn, table_compaction_map))) {
                    LOG_WARN("fail to handle index table compaction finished", KR(ret), K(table_id), K(frozen_scn));
                  }
                }
                if (OB_SUCC(ret)) {
                  if (OB_FAIL(table_compaction_map.set_refactored(data_table_id, data_compaction_info, true/*overwrite*/))) {
                    LOG_WARN("fail to set refactored", KR(ret), K(data_table_id), K(data_compaction_info));
                  }
                }
              } else { // virtual index table has no tablet, not need to index checksum verification.
                if (cur_compaction_info.finish_compaction() && data_compaction_info.finish_compaction()) {
                  if (OB_FAIL(handle_table_compaction_finished(table_schema, frozen_scn, table_compaction_map))) {
                    LOG_WARN("fail to handle index table compaction finished", KR(ret), K(table_id), K(frozen_scn));
                  } else if (OB_FAIL(handle_table_compaction_finished(data_table_schema, frozen_scn, table_compaction_map))) {
                    LOG_WARN("fail to handle data table compaction finished", KR(ret), K(data_table_id), K(frozen_scn));
                  }
                }
              }
            }
          } else {
            if (table_schema->get_index_tid_count() < 1) { // handle data table, meanwhile not have relative index table
              if (cur_compaction_info.finish_compaction()) {
                if OB_FAIL(handle_table_compaction_finished(table_schema, frozen_scn, table_compaction_map)) {
                  LOG_WARN("fail to handle table compaction finished", KR(ret), K(table_id), K(frozen_scn));
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

      if (FAILEDx(check_valid_and_verified_for_data_table(table_schemas, table_compaction_map))) {
        LOG_WARN("fail to check valid and verified for data table", KR(ret), K_(tenant_id), K(frozen_scn));
      }

      // for valid data table, if its all index table finished verification, we can handle it here
      // and make it as verified
      //
      // and we also need to do that, compare 'table_compaction_map' with 'table_ids', remove those
      // whose table_id not exists in 'table_ids' from 'table_compaction_map'
      if (OB_SUCC(ret) && (OB_SUCCESS == check_ret)) {
        ObArray<uint64_t> removed_table_ids; // record the table_id which will be removed
        hash::ObHashMap<uint64_t, ObTableCompactionInfo>::iterator iter = table_compaction_map.begin();
        for (;!stop && OB_SUCC(ret) && (iter != table_compaction_map.end()); ++iter) {
          const uint64_t cur_table_id = iter->first;
          if (exist_in_table_array(cur_table_id, table_ids)) {
            const ObTableCompactionInfo &compaction_info = iter->second;
            if (compaction_info.is_valid_data_table_) {
              if (OB_FAIL(update_data_table_verified(iter->first, compaction_info, frozen_scn, table_compaction_map))) {
                if (OB_CHECKSUM_ERROR == ret) {
                  check_ret = OB_CHECKSUM_ERROR;
                }
                LOG_WARN("fail to update data table to verified status", KR(ret), "data_table_id", iter->first,
                  K(frozen_scn), K(compaction_info));
              }
            }
          } else if (OB_FAIL(removed_table_ids.push_back(cur_table_id))) {
            LOG_WARN("fail to push back", KR(ret), K(cur_table_id));
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
  }

  if (check_ret == OB_CHECKSUM_ERROR) {
    ret = check_ret;
  }
  return ret;
}

int ObIndexChecksumValidator::update_data_table_verified(
    const int64_t data_table_id,
    const ObTableCompactionInfo &data_table_compaction,
    const SCN &frozen_scn,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  if (!data_table_compaction.is_valid_data_table_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_table_compaction));
  } else {
    if (data_table_compaction.is_verified()) { // skip if already finished verification
    } else if (data_table_compaction.all_index_verified_) {
      if (data_table_compaction.finish_compaction()) {
        ObSchemaGetterGuard schema_guard;
        const ObTableSchema *data_table_schema = nullptr;
        if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(tenant_id_, schema_guard))) {
          LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
        } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, data_table_schema))) {
          LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(data_table_id));
        } else if (OB_ISNULL(data_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("fail to get data table schema", KR(ret), K_(tenant_id), K(data_table_id));
        } else if (OB_FAIL(handle_table_compaction_finished(data_table_schema, frozen_scn, table_compaction_map))) {
          LOG_WARN("fail to handle table compaction finished", KR(ret), K(data_table_id), K(frozen_scn));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data table must finish compaction when its all index table finished verifying", KR(ret), K(data_table_id),
          K(frozen_scn), K(data_table_compaction));
      }
    } else {
      ObTableCompactionInfo new_compaction_info = data_table_compaction;
      // why mark it as false: the next time to re-scan all table, we should re-check this data
      // table's 'is_valid_data_table_', since this data table's index may be dropped
      new_compaction_info.is_valid_data_table_ = false;
      //why mark it as true: cuz we can re-scan all table, to check this data table's 'all_index_verified_' again.
      new_compaction_info.all_index_verified_ = true;
      if (OB_FAIL(table_compaction_map.set_refactored(data_table_id, new_compaction_info, true/*overwrite*/))) {
        LOG_WARN("fail to set refactored", KR(ret), K(data_table_id), K(new_compaction_info));
      }
    }
  }
  return ret;
}

// If one table finished compaction (and finished index checksum verification if needed), we can handle it,
// and then mark it as VERIFIED.
int ObIndexChecksumValidator::handle_table_compaction_finished(
    const ObTableSchema *table_schema,
    const SCN &frozen_scn,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    const uint64_t table_id = table_schema->get_table_id();
    ObTableCompactionInfo cur_compaction_info;
    if (OB_FAIL(table_compaction_map.get_refactored(table_id, cur_compaction_info))) {
      LOG_WARN("fail to get refactored", KR(ret), K(table_id));
    } else if (cur_compaction_info.is_verified()) { // skip if finished verification
    } else if (!cur_compaction_info.finish_compaction()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table must finish compaction when arriving here", KR(ret), K(table_id), K(cur_compaction_info));
    } else {
      if (table_schema->has_tablet()) {
        SMART_VAR(ObArray<ObTabletLSPair>, pairs) {
          FREEZE_TIME_GUARD;
          if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(tenant_id_, *table_schema, *sql_proxy_, pairs))) {
            LOG_WARN("fail to get tablet_ls pairs", KR(ret), K_(tenant_id), K(table_id));
          } else if (pairs.count() < 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret), K_(tenant_id), K(table_id));
          } else if (OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_report_scn(
              tenant_id_, frozen_scn.get_val_for_tx(),
              pairs, ObTabletReplica::ScnStatus::SCN_STATUS_ERROR))) {
            LOG_WARN("fail to batch update report_scn", KR(ret), K_(tenant_id), K(pairs));
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

// check all tablets of this table finished compaction or not.
//
// we need to notice that, when one table finished compaction, we need to execute tablet_replica
// checksum verification if this table has tablet.
int ObIndexChecksumValidator::check_table_compaction_finished(
    const ObTableSchema &table_schema,
    const SCN &frozen_scn,
    const hash::ObHashMap<ObTabletLSPair, ObTabletCompactionStatus> &tablet_compaction_map,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map,
    ObTableCompactionInfo &latest_compaction_info)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = UINT64_MAX;
  latest_compaction_info.reset();
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema));
  } else if (FALSE_IT(table_id = table_schema.get_table_id())) {
  } else if (OB_FAIL(table_compaction_map.get_refactored(table_id, latest_compaction_info))) {
    if (OB_HASH_NOT_EXIST == ret) { // first initial
      ret = OB_SUCCESS;
      latest_compaction_info.table_id_ = table_id;
      if (OB_FAIL(table_compaction_map.set_refactored(table_id, latest_compaction_info))) {
        LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(latest_compaction_info));
      }
    } else {
      LOG_WARN("fail to get val from hashmap", KR(ret), K(table_id));
    }
  }
  
  if (OB_SUCC(ret) && latest_compaction_info.is_uncompacted()) {
    SMART_VAR(ObArray<ObTabletID>, tablet_ids) {
      SMART_VAR(ObArray<ObTabletLSPair>, pairs) {
        if (table_schema.has_tablet()) {
          FREEZE_TIME_GUARD;
          if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
            LOG_WARN("fail to get tablet_ids from table schema", KR(ret), K(table_schema));
          } else if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_ls_pairs(tenant_id_, table_id,
                     *sql_proxy_, tablet_ids, pairs))) {
            LOG_WARN("fail to get tablet_ls pairs", KR(ret), K_(tenant_id), K(table_id));
          } else if (pairs.count() < 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get tablet_ls pairs of current table schema", KR(ret), K_(tenant_id), K(table_id));
          } else {
            // iterate all tablets to check 'compacted/finished status' or not.
            const int64_t tablet_cnt = tablet_ids.count();
            int64_t idx = 0;
            bool exist_skip_verifying_tablet = false;
            for (; OB_SUCC(ret) && (idx < tablet_cnt); ++idx) {
              ObTabletCompactionStatus tablet_status = ObTabletCompactionStatus::INITIAL;
              if (OB_FAIL(tablet_compaction_map.get_refactored(pairs.at(idx), tablet_status))) {
                if (OB_HASH_NOT_EXIST == ret) { // if tablet not finish compaction, it won't be added into this map
                  ret = OB_SUCCESS;
                  break;
                } else {
                  LOG_WARN("fail to get tablet compaction status from map", KR(ret), K(idx), "pair", pairs.at(idx));
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
            // if current table 'has tablet' & 'finished compaction' & 'not skip verifying', verify tablet replica checksum
            if (OB_SUCC(ret) && latest_compaction_info.is_compacted()) {
              FREEZE_TIME_GUARD;
              if (OB_FAIL(ObTabletReplicaChecksumOperator::check_tablet_replica_checksum(tenant_id_, pairs, frozen_scn,
                  *sql_proxy_))) {
                if (OB_CHECKSUM_ERROR == ret) {
                  LOG_ERROR("ERROR! ERROR! ERROR! checksum error in major tablet_replica_checksum", KR(ret),
                    K_(tenant_id), K(frozen_scn), "pair_cnt", pairs.count());
                } else {
                  LOG_WARN("fail to check major tablet_replica checksum", KR(ret), K_(tenant_id), K(frozen_scn), K(table_schema));
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

bool ObIndexChecksumValidator::is_index_table(
    const ObSimpleTableSchemaV2 &simple_schema)
{
  return (simple_schema.is_index_table()
         && simple_schema.can_read_index());
}

bool ObIndexChecksumValidator::exist_in_table_array(
    const uint64_t table_id,
    const ObIArray<uint64_t> &table_ids)
{
  bool exist = false;
  for (int64_t i = 0; (i < table_ids.count()) && !exist; ++i) {
    if (table_id == table_ids.at(i)) {
      exist = true;
    }
  }
  return exist;
}

int ObIndexChecksumValidator::check_valid_and_verified_for_data_table(
    const ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas,
    hash::ObHashMap<uint64_t, ObTableCompactionInfo> &table_compaction_map)
{
  int ret = OB_SUCCESS;
  int64_t table_count = table_schemas.count();
  // initialize 'is_valid_data_table_' and 'all_index_verifed_'
  for (int64_t i = 0; (i < table_count) && OB_SUCC(ret); ++i) {
    const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
    uint64_t table_id = 0;
    ObTableCompactionInfo table_compaction_info;
    if (OB_ISNULL(simple_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
    } else if (FALSE_IT(table_id = simple_schema->get_table_id())) {
    } else if (OB_FAIL(table_compaction_map.get_refactored(table_id, table_compaction_info))) {
      LOG_WARN("fail to get refactored", KR(ret), K(table_id));
    } else {
      table_compaction_info.is_valid_data_table_ = false;
      table_compaction_info.all_index_verified_ = true;
      if (OB_FAIL(table_compaction_map.set_refactored(table_id, table_compaction_info, true/*overwrite*/))) {
        LOG_WARN("fail to set refactored", KR(ret), K(table_id), K(table_compaction_info));
      }
    }
  }
  // check 'is_valid_data_table_' and 'all_index_verifed_' for data tables according to index tables
  for (int64_t i = 0; (i < table_count) && OB_SUCC(ret); ++i) {
    const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
    if (OB_ISNULL(simple_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
    } else if (is_index_table(*simple_schema)) {
      const uint64_t index_table_id = simple_schema->get_table_id();
      const uint64_t data_table_id = simple_schema->get_data_table_id();
      ObTableCompactionInfo index_table_compaction_info;
      ObTableCompactionInfo data_table_compaction_info;
      if (OB_FAIL(table_compaction_map.get_refactored(index_table_id, index_table_compaction_info))) {
        LOG_WARN("fail to get refactored", KR(ret), K(index_table_id));
      } else if (OB_FAIL(table_compaction_map.get_refactored(data_table_id, data_table_compaction_info))) {
        LOG_WARN("fail to get refactored", KR(ret), K(data_table_id));
      } else {
        bool need_update = false;
        if (!data_table_compaction_info.is_valid_data_table_) {
          need_update = true;
          data_table_compaction_info.is_valid_data_table_ = true;
        }
        if (!index_table_compaction_info.is_verified()
            && data_table_compaction_info.all_index_verified_) {
          need_update = true;
          data_table_compaction_info.all_index_verified_ = false;
        }
        if (need_update) {
          if (OB_FAIL(table_compaction_map.set_refactored(data_table_id, data_table_compaction_info,
                                                          true/*overwrite*/))) {
            LOG_WARN("fail to set refactored", KR(ret), K(data_table_id), K(data_table_compaction_info));
          }
        }
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
