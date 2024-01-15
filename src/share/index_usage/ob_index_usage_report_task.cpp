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
#include "ob_index_usage_info_mgr.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_config_mgr.h"

#define USING_LOG_PREFIX SERVER

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

const char *OB_INDEX_USAGE_REPORT_TASK = "IndexUsageReportTask";
#define INSERT_INDEX_USAGE_HEAD_SQL                                                                               \
  "INSERT INTO %s"                                                                                                \
  " (tenant_id, object_id, name, owner,"                                                                          \
  " total_access_count, total_exec_count, total_rows_returned,"                                                  \
  " bucket_0_access_count, bucket_1_access_count,"                                                               \
  " bucket_2_10_access_count, bucket_2_10_rows_returned,"                                                        \
  " bucket_11_100_access_count, bucket_11_100_rows_returned,"                                                    \
  " bucket_101_1000_access_count, bucket_101_1000_rows_returned,"                                                \
  " bucket_1000_plus_access_count, bucket_1000_plus_rows_returned,"                                              \
  " last_used, last_flush_time) VALUES "
#define INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL                                                                  \
  " ON DUPLICATE KEY UPDATE"                                                                                     \
  " total_access_count = total_access_count + VALUES(total_access_count),"                                       \
  " total_exec_count = total_exec_count + VALUES(total_exec_count),"                                             \
  " total_rows_returned = total_rows_returned + VALUES(total_rows_returned),"                                    \
  " bucket_0_access_count = bucket_0_access_count + VALUES(bucket_0_access_count),"                              \
  " bucket_1_access_count = bucket_1_access_count + VALUES(bucket_1_access_count),"                              \
  " bucket_2_10_access_count = bucket_2_10_access_count + VALUES(bucket_2_10_access_count),"                     \
  " bucket_2_10_rows_returned = bucket_2_10_rows_returned + VALUES(bucket_2_10_rows_returned),"                  \
  " bucket_11_100_access_count = bucket_11_100_access_count + VALUES(bucket_11_100_access_count),"               \
  " bucket_11_100_rows_returned = bucket_11_100_rows_returned + VALUES(bucket_11_100_rows_returned),"            \
  " bucket_101_1000_access_count = bucket_101_1000_access_count + VALUES(bucket_101_1000_access_count),"         \
  " bucket_101_1000_rows_returned = bucket_101_1000_rows_returned + VALUES(bucket_101_1000_rows_returned),"      \
  " bucket_1000_plus_access_count = bucket_1000_plus_access_count + VALUES(bucket_1000_plus_access_count),"      \
  " bucket_1000_plus_rows_returned = bucket_1000_plus_rows_returned + VALUES(bucket_1000_plus_rows_returned),"   \
  " last_used = VALUES(last_used),"                                                                              \
  " last_flush_time = VALUES(last_flush_time) "
ObIndexUsageReportTask::ObIndexUsageReportTask() :
  is_inited_(false),
  mgr_(nullptr),
  sql_proxy_(nullptr) {}

int ObIndexUsageReportTask::GetIndexUsageItemsFn::operator()(common::hash::HashMapPair<ObIndexUsageKey, ObIndexUsageInfo> &entry)
{
  int ret = OB_SUCCESS;
  bool exist = true;
  const uint64_t index_table_id = entry.first.index_table_id_;
  if (OB_ISNULL(schema_guard_) || OB_INVALID_TENANT_ID == tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard_), K(tenant_id_));
  } else if (OB_FAIL(schema_guard_->check_table_exist(tenant_id_, index_table_id, exist))) {
    LOG_WARN("fail to check table exists", K(ret), K(tenant_id_), K(index_table_id));
  } else if (!exist) {
    if (OB_FAIL(remove_items_.push_back(entry.first))) {
      LOG_WARN("fail to push back remove key", K(ret), K(entry.first));
    }
  } else {
    if (entry.second.has_data()) { // has new data
      ObIndexUsagePair pair;
      pair.init(entry.first, entry.second);  // clear data
      if (OB_FAIL(dump_items_.push_back(pair))) {
        LOG_WARN("fail to push back to list", K(ret), K(tenant_id_));
      } else if (++total_dump_count_ >= MAX_DUMP_ITEM_COUNT) {
        ret = OB_ITER_END;
        LOG_INFO("Reach index usage info dump limit", K(ret), K(total_dump_count_));
      }
      entry.second.reset(); // reset record
    }
    if (OB_SUCC(ret)) { // remove exist item from deleted map
      if (OB_FAIL(deleted_map_.erase_refactored(entry.first))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to remove exist item from deleted map", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIndexUsageReportTask::init(ObIndexUsageInfoMgr *mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const ObMemAttr attr(tenant_id, OB_INDEX_USAGE_REPORT_TASK);
    if (OB_FAIL(deleted_map_.create(MAX_DELETE_HASHMAP_SIZE, attr))) {
      LOG_WARN("fail to create deleted map", K(ret), K(tenant_id));
    } else {
      set_sql_proxy(GCTX.sql_proxy_);
      set_mgr(mgr);
      set_is_inited(true);
    }
  }
  return ret;
}

void ObIndexUsageReportTask::destroy()
{
  set_is_inited(false);
  set_sql_proxy(nullptr);
  set_mgr(nullptr);
  deleted_map_.clear();
}

void ObIndexUsageReportTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
  } else if (OB_FAIL(dump())) {
    LOG_WARN("dump index usage info failed", K(ret), K(MTL_ID()));
  }
}

int ObIndexUsageReportTask::storage_index_usage(const ObIndexUsagePairList &info_list)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  uint64_t tenant_id = MTL_ID();
  uint64_t extract_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to GET_MIN_DATA_VERSION", K(ret));
  } else if (data_version < DATA_VERSION_4_3_0_0) {
    // skip // only for version > 4.3.0.0
    LOG_INFO("data version less than 430 is not support", K(tenant_id));
  } else if (!info_list.empty()) {
    ObSqlString insert_update_sql;
    if (OB_FAIL(insert_update_sql.append_fmt(INSERT_INDEX_USAGE_HEAD_SQL, OB_ALL_INDEX_USAGE_INFO_TNAME))) {
      LOG_WARN("fail to append sql string", K(ret));
    }
    // append sql string
    for (ObIndexUsagePairList::const_iterator it = info_list.begin(); OB_SUCC(ret) && it != info_list.end(); it++) {
      if (OB_FAIL(insert_update_sql.append_fmt(
            "(%lu,%lu,'','', %lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,usec_to_time(%lu),now(6)),",
          extract_tenant_id,
          it->first.index_table_id_,
          it->second.total_exec_count_, // total_access_count_
          it->second.total_exec_count_,
          it->second.total_rows_returned_,
          it->second.bucket_0_access_count_,
          it->second.bucket_1_access_count_,
          it->second.bucket_2_10_access_count_,
          it->second.bucket_2_10_rows_returned_,
          it->second.bucket_11_100_access_count_,
          it->second.bucket_11_100_rows_returned_,
          it->second.bucket_101_1000_access_count_,
          it->second.bucket_101_1000_rows_returned_,
          it->second.bucket_1000_plus_access_count_,
          it->second.bucket_1000_plus_rows_returned_,
          it->second.last_used_time_))) {
        LOG_WARN("fail to append sql string", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      insert_update_sql.set_length(insert_update_sql.length() - 1);
      if (OB_FAIL(insert_update_sql.append(INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL))) {
        LOG_WARN("fail to append sql string", K(ret));
      } else if (OB_FAIL(sql_proxy_->write(tenant_id, insert_update_sql.ptr(), affected_rows))) {
        LOG_WARN("insert update sql error", K(ret), K(tenant_id), K(insert_update_sql));
      } else if (affected_rows < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(tenant_id), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObIndexUsageReportTask::del_index_usage(const ObIndexUsageKey &key)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  uint64_t data_version = 0;
  uint64_t tenant_id = MTL_ID();
  uint64_t extract_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(*sql_proxy_, tenant_id);

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to GET_MIN_DATA_VERSION", K(ret));
  } else if (data_version < DATA_VERSION_4_3_0_0) {
    // skip // only for version > 4.3.0.0
    LOG_INFO("data version less than 430 is not support", K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id)) ||
             OB_FAIL(dml.add_pk_column("object_id", key.index_table_id_))) {
    LOG_WARN("dml add column failed", K(ret), K(tenant_id), K(extract_tenant_id));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_INDEX_USAGE_INFO_TNAME, dml, affected_rows))) {
    LOG_WARN("del sql exec error", K(ret), K(tenant_id), K(extract_tenant_id), K(key));
  }
  return ret;
}

int ObIndexUsageReportTask::check_and_delete(const ObIArray<ObIndexUsageKey> &candidate_deleted_item, ObIndexUsageHashMap *hashmap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hashmap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidate_deleted_item.count(); ++i) {
    const ObIndexUsageKey &key = candidate_deleted_item.at(i);
    uint64_t value = 0;
    if (OB_FAIL(deleted_map_.get_refactored(key, value))) {
      if (OB_HASH_NOT_EXIST == ret) { // insert new
        ret = OB_SUCCESS;
        if (deleted_map_.size() >= MAX_DELETE_HASHMAP_SIZE) {
          LOG_INFO("reach max deleted map upper limited", K(deleted_map_.size()));
          break;
        } else if (OB_FAIL(deleted_map_.set_refactored(key, 1, true /* overwrite */))) {
          LOG_WARN("fail to set delete map", K(ret), K(key));
        }
      } else {
        LOG_WARN("fail to get from deleted map", K(ret));
      }
    } else if (++value <= MAX_CHECK_NOT_EXIST_CNT) { // update
      if (OB_FAIL(deleted_map_.set_refactored(key, value, true /* overwrite */))) {
        LOG_WARN("fail to set deleted map", K(ret), K(key), K(value));
      }
    } else { // delete
      if (OB_FAIL(deleted_map_.erase_refactored(key))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to del deleted map record", K(ret), K(key));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(del_index_usage(key))) {
          LOG_WARN("fail to del inner table record", K(ret));
        } else if (OB_FAIL(hashmap->erase_refactored(key))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to del index usage hashmap record", K(ret), K(key));
          }
        }
      }
    }
  }
  return ret;
}

/*
1. dump ObIndexUsageInfo by batch size DUMP_BATCH_SIZE to dump limit MAX_DUMP_ITEM_COUNT (1 cycle)
2. check deleted index to del record in hashmap or in inner table
*/
int ObIndexUsageReportTask::dump()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index usage mgr not init", K(ret));
  } else {
    const uint64_t tenant_id = MTL_ID();
    mgr_->refresh_config();
    GetIndexUsageItemsFn index_usage_items_fn(deleted_map_, tenant_id, mgr_->get_allocator());
    // dump batch
    for (int64_t i = 0; OB_SUCC(ret) && i < mgr_->get_hashmap_count() &&
      index_usage_items_fn.total_dump_count_ < MAX_DUMP_ITEM_COUNT; ++i) {
      ObIndexUsageHashMap *hashmap = mgr_->get_index_usage_map() + i;
      {
        ObSchemaGetterGuard schema_guard;
        int64_t schema_version = 0;
        if (OB_ISNULL(GCTX.schema_service_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema service null pointer", K(ret));
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
          LOG_WARN("failed to get schema_guard", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
          LOG_WARN("failed to get schema version", K(ret), K(tenant_id));
        } else if (!ObSchemaService::is_formal_version(schema_version)) {
          ret = OB_EAGAIN;
          LOG_INFO("is not a formal_schema_version", K(ret), K(tenant_id), K(schema_version));
        } else if (OB_FALSE_IT(index_usage_items_fn.set_schema_guard(&schema_guard))) {
        } else if (OB_FAIL(hashmap->foreach_refactored(index_usage_items_fn))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("foreach refactored failed", K(ret), K(tenant_id));
          } else {
            ret = OB_SUCCESS; // reach max dump count
          }
        }
      }
      if (OB_SUCC(ret)) {
        // dump
        const uint64_t dump_item_count = index_usage_items_fn.dump_items_.size();
        uint64_t index = 0;
        ObIndexUsagePairList tmp_list(mgr_->get_allocator());
        for (ObIndexUsagePairList::const_iterator it = index_usage_items_fn.dump_items_.begin();
          OB_SUCC(ret) && index < dump_item_count; it++, index++) {
          ObIndexUsagePair tmp_pair;
          tmp_pair.init(it->first, it->second);  // clear data
          if (OB_FAIL(tmp_list.push_back(tmp_pair))) {
            LOG_WARN("fail to push back to list", K(ret), K(tenant_id));
          } else if (tmp_list.size() < DUMP_BATCH_SIZE && index < dump_item_count - 1) {
            // continue
          } else if (OB_FAIL(storage_index_usage(tmp_list))) {
            LOG_WARN("flush index usage batch failed", K(ret), K(tenant_id));
          } else {
            tmp_list.reset();
          }
        }
        if (OB_SUCC(ret)) {
          index_usage_items_fn.dump_items_.reset();
          if (OB_FAIL(check_and_delete(index_usage_items_fn.remove_items_, hashmap))) {
            LOG_WARN("fail to check and delete index usage record", K(ret));
          } else {
            index_usage_items_fn.remove_items_.reuse();
          }
        }
      }
    }
  }
  return ret;
}

// ========================= refresh tenant config ===========================//
ObIndexUsageRefreshConfTask::ObIndexUsageRefreshConfTask() :
  is_inited_(false), mgr_(nullptr)
{}

int ObIndexUsageRefreshConfTask::init(ObIndexUsageInfoMgr *mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    set_mgr(mgr);
    set_is_inited(true);
  }
  return ret;
}

void ObIndexUsageRefreshConfTask::destroy()
{
  set_is_inited(false);
  set_mgr(nullptr);
}

void ObIndexUsageRefreshConfTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index usage mgr not init", K(ret));
  } else if (!is_inited_) {
    // skip
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(mgr_->get_tenant_id()));
    if (OB_LIKELY(tenant_config.is_valid())) {
      mgr_->set_max_entries(tenant_config->_iut_max_entries.get());
      mgr_->set_is_enabled(tenant_config->_iut_enable);
      mgr_->set_is_sample_mode(tenant_config->_iut_stat_collection_type.get_value_string().case_compare("SAMPLED") == 0);
      LOG_TRACE("success to refresh index usage config.",
        K(mgr_->get_max_entries()), K(mgr_->get_is_enabled()), K(mgr_->get_is_sample_mode()));
    }
    // get data version
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(mgr_->get_tenant_id(), data_version))) {
      LOG_WARN("failed to GET_MIN_DATA_VERSION", K(ret));
    } else {
      mgr_->set_min_tenant_data_version(data_version);
    }
    // get time
    mgr_->set_current_time(common::ObClockGenerator::getClock());
  }
}

} // namespace share
} // namespace oceanbase

#undef INSERT_INDEX_USAGE_HEAD_SQL
#undef INSERT_INDEX_USAGE_ON_DUPLICATE_END_SQL
