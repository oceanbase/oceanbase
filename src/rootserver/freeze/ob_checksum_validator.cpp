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
#include "rootserver/ob_root_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/backup/ob_backup_manager.h"
#include "share/ob_tablet_replica_checksum_operator.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

int ObChecksumValidatorBase::init(
    const uint64_t tenant_id,
    ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy) || (OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    sql_proxy_ = sql_proxy;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObChecksumValidatorBase::check(const ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  if (!frozen_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_status));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (need_check_) {
    if (OB_FAIL(do_check(frozen_status))) {
      LOG_WARN("fail to do check", KR(ret), K_(tenant_id), K(frozen_status));
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////

int ObTabletChecksumValidator::do_check(const ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  
  int64_t check_cnt= 0;
  HEAP_VAR(ObTabletReplicaChecksumIterator, tablet_replica_checksum_iter) {
    if (OB_FAIL(tablet_replica_checksum_iter.init(tenant_id_, sql_proxy_))) {
      LOG_WARN("fail to init tablet_replica_checksum iter", KR(ret), K_(tenant_id));
    } else {
      tablet_replica_checksum_iter.set_snapshot_version(frozen_status.frozen_scn_);

      ObTabletReplicaChecksumItem prev_item;
      ObTabletReplicaChecksumItem curr_item;
      while (OB_SUCC(ret)) {
        curr_item.reset();
        if (OB_FAIL(tablet_replica_checksum_iter.next(curr_item))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to iter next tablet replica checksum item", KR(ret), K_(tenant_id));
          }
        } else {
          if (prev_item.is_key_valid()) {
            if (curr_item.is_same_tablet(prev_item)) { // same tablet
              ++check_cnt;
              if (OB_FAIL(curr_item.verify_checksum(prev_item))) {
                if (OB_CHECKSUM_ERROR == ret) {
                  LOG_ERROR("ERROR! ERROR! ERROR! checksum error in tablet replica checksum", KR(ret),
                    K(curr_item), K(prev_item));
                  check_ret = ret;
                  ret = OB_SUCCESS; // continue checking next checksum
                } else {
                  LOG_WARN("unexpected error in tablet replica checksum", KR(ret), K(curr_item), K(prev_item));
                }
              }
            } else { // next tablet
              prev_item = curr_item;
            }
          } else {
            prev_item = curr_item;
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
  LOG_INFO("finish verifying tablet checksum", KR(ret), KR(check_ret), K_(tenant_id), 
    K(frozen_status), K(check_cnt));
  return ret;
}

///////////////////////////////////////////////////////////////////////////////

int ObCrossClusterTableteChecksumValidator::do_check(const ObSimpleFrozenStatus &frozen_status)
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
        tablet_checksum_iter.set_snapshot_version(frozen_status.frozen_scn_);
        tablet_replica_checksum_iter.set_snapshot_version(frozen_status.frozen_scn_);

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
    K(frozen_status), K(check_cnt));
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

int ObIndexChecksumValidator::do_check(const ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  int64_t check_cnt = 0;

  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;

  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id_, table_schemas))) {
    LOG_WARN("fail to get tenant table schemas", KR(ret), K_(tenant_id));
  } else {
    for (int64_t i = 0; (i < table_schemas.count()) && OB_SUCC(ret); ++i) {
      const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);

      if (OB_ISNULL(simple_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, simple schema is null", KR(ret), K_(tenant_id));
      } else if (simple_schema->is_index_table() 
                 && simple_schema->can_read_index() 
                 // virtual index table has no tablet
                 && simple_schema->has_tablet()) {
        const ObTableSchema *data_table_schema = nullptr;
        const ObTableSchema *index_table_schema = nullptr;
        const uint64_t index_table_id = simple_schema->get_table_id();
        const uint64_t data_table_id = simple_schema->get_data_table_id();
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_table_id, index_table_schema))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id_), K(index_table_id));
        } else if (OB_ISNULL(index_table_schema)) {
          // index table is deleted, do nothing
        } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, data_table_schema))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id_), K(data_table_id));
        } else if (OB_ISNULL(data_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("fail to get data table schema", KR(ret), K_(tenant_id), K(data_table_id));
        } else {
          ++check_cnt;
        }
        
        if (FAILEDx(ObTabletReplicaChecksumOperator::check_column_checksum(tenant_id_, *data_table_schema,
            *index_table_schema, frozen_status.frozen_scn_, *sql_proxy_))) {
          if (OB_CHECKSUM_ERROR == ret) {
            LOG_ERROR("ERROR! ERROR! ERROR! checksum error in index checksum", KR(ret), K_(tenant_id),
              K(frozen_status), K(*data_table_schema), K(*index_table_schema));
            check_ret = OB_CHECKSUM_ERROR;
            ret = OB_SUCCESS; // continue checking next checksum
          } else if (OB_EAGAIN != ret) {
            LOG_WARN("fail to check index column checksum", KR(ret), K_(tenant_id), K(*data_table_schema),
              K(*index_table_schema));
          } else {
            if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
              LOG_WARN("fail to check index column checksum", KR(ret), K_(tenant_id), K(*data_table_schema),
                K(*index_table_schema));
            }
          }
        }
        ret = OB_SUCCESS; // ignore ret, and continue check next table_schema
      }
    } // end for loop 
  }
  
  if (OB_CHECKSUM_ERROR == check_ret) {
    ret = OB_CHECKSUM_ERROR;
  }
  LOG_INFO("finish verifying index checksum", KR(ret), KR(check_ret), K_(tenant_id), 
    K(frozen_status), K(check_cnt));
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
