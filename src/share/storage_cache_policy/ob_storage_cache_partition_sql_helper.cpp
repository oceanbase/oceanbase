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
#define USING_LOG_PREFIX STORAGE
#include "share/storage_cache_policy/ob_storage_cache_partition_sql_helper.h"
#include "share/schema/ob_partition_sql_helper.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
int ObAlterIncPartPolicyHelper::alter_partition_policy()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", KR(ret));
  } else if (!ori_table_->is_user_partition_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user partition table", KR(ret), KPC(ori_table_));
  } else {
    const uint64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t table_id = ori_table_->get_table_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObPartition **part_array = inc_table_->get_part_array();
    ObPartition *inc_part = nullptr;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    common::hash::ObHashSet<int64_t> exist_part_ids;
    // This constant indicates that if the key exists in the set, OB_HASH_EXIST will be returned.
    const int OVERWRITE_FLAG = 0;
    const int BUCKET_NUM = 16;

    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inc table part_array is null", KR(ret), KP(inc_table_));
    } else if (OB_FAIL(exist_part_ids.create(BUCKET_NUM))) {
      LOG_WARN("fail to create exist_part_ids", KR(ret), KPC(inc_table_), KPC(ori_table_));
    } else {
      ObDMLSqlSplicer dml;
      ObSqlString part_sql;
      ObDMLSqlSplicer history_dml;
      ObSqlString part_history_sql;
      int64_t affected_rows = 0;
      int64_t part_id = OB_INVALID_OBJECT_ID;

      for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
        inc_part = part_array[i];
        if (OB_ISNULL(inc_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inc_part is null", KR(ret));
        } else if (FALSE_IT(part_id = inc_part->get_part_id())) {
        } else if (OB_FAIL(exist_part_ids.set_refactored(part_id, OVERWRITE_FLAG/*flag*/))) {
          if (OB_HASH_EXIST == ret) {
            LOG_WARN("duplicate partition when alter partition storage_cache_policy", KR(ret), K(part_id), KPC(inc_part), K(inc_part_num));
            LOG_USER_ERROR(OB_HASH_EXIST, "duplicate partition when alter partition storage_cache_policy");
          } else {
            LOG_WARN("failed to set refactored for exist_part_ids", KR(ret), K(part_id), KPC(inc_part), K(inc_part_num));
          }
        } else {
          storage::ObStorageCachePolicyType part_storage_cache_policy_type = inc_part->get_part_storage_cache_policy_type();
          const char *part_storage_cache_policy_str = nullptr;
          if (OB_FAIL(storage::ObStorageCacheGlobalPolicy::safely_get_str(part_storage_cache_policy_type, part_storage_cache_policy_str))) {
            LOG_WARN("get part policy failed", K(ret), K(part_storage_cache_policy_type));
          } else if (OB_ISNULL(part_storage_cache_policy_str)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part_storage_cache_policy_str is null", KR(ret));
          } else if (OB_FAIL(dml.add_pk_column("tenant_id",ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
              || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))
              || OB_FAIL(dml.add_pk_column("part_id", inc_part->get_part_id()))
              || OB_FAIL(dml.add_column("schema_version", schema_version_))
              || OB_FAIL(dml.add_column("storage_cache_policy", part_storage_cache_policy_str))) {
            LOG_WARN("dml add column failed", KR(ret));
          } else if (FAILEDx(dml.finish_row())) {
            LOG_WARN("finish row failed", KR(ret));
          } else {
            HEAP_VAR(ObAddIncPartDMLGenerator, part_dml_gen, ori_table_, *inc_part, inc_part_num,
                inc_part->get_part_idx(), schema_version_)
            {
              if (OB_FAIL(part_dml_gen.gen_dml(history_dml))) {
                LOG_WARN("gen dml failed", KR(ret));
              } else if (OB_FAIL(history_dml.add_column("is_deleted", false))) {
                LOG_WARN("add column failed", KR(ret));
              } else if (FAILEDx(history_dml.finish_row())) {
                LOG_WARN("finish row failed", KR(ret));
              }
            }
          }
        }
      }
      if (FAILEDx(dml.splice_batch_insert_update_sql(OB_ALL_PART_TNAME, part_sql))) {
        LOG_WARN("fail to splice batch insert update sql", KR(ret), K(part_sql));
      } else if (OB_FAIL(sql_client_.write(tenant_id, part_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write sql", KR(ret), K(part_sql),
            K(affected_rows));
      } else if (OB_UNLIKELY(2 * exist_part_ids.size() != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", KR(ret), K(inc_part_num), K(affected_rows), K(part_sql));
      } else if (FAILEDx(history_dml.splice_batch_insert_sql(OB_ALL_PART_HISTORY_TNAME, part_history_sql))) {
        LOG_WARN("fail to splice batch insert update sql", KR(ret), K(part_history_sql));
      } else if (OB_FAIL(sql_client_.write(tenant_id, part_history_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write sql", KR(ret), K(part_history_sql),
            K(affected_rows));
      } else if (OB_UNLIKELY(exist_part_ids.size() != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", KR(ret), K(exist_part_ids.size()), K(affected_rows), K(part_history_sql));
      }
      FLOG_INFO("alter partition policy finish", KR(ret), K(part_sql), K(part_history_sql), K(affected_rows), K(inc_part_num));
    }
  }
  return ret;
}

int ObAlterIncSubpartPolicyHelper::alter_subpartition_policy()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_table_) || OB_ISNULL(inc_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", KR(ret), KP(ori_table_), KP(inc_table_));
  } else if (!ori_table_->is_user_subpartition_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user subpartition table", KR(ret), KPC(ori_table_));
  } else {
    int64_t tenant_id = ori_table_->get_tenant_id();
    const uint64_t table_id = ori_table_->get_table_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObPartition **part_array = inc_table_->get_part_array();
    ObPartition *inc_part = nullptr;
    const int64_t inc_part_num = inc_table_->get_partition_num();
    common::hash::ObHashSet<int64_t> exist_subpart_ids;
    // This constant indicates that if the key exists in the set, OB_HASH_EXIST will be returned.
    const int OVERWRITE_FLAG = 0;
    const int BUCKET_NUM = 16;

    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", KR(ret), KP(inc_table_));
    } else if (OB_FAIL(exist_subpart_ids.create(BUCKET_NUM))) {
      LOG_WARN("fail to create exist_subpart_ids", KR(ret), KPC(inc_table_), KPC(ori_table_));
    } else {
      ObDMLSqlSplicer dml;
      ObSqlString subpart_sql;
      ObDMLSqlSplicer history_sub_dml;
      ObSqlString subpart_history_sql;
      int64_t affected_rows = 0;
      int64_t subpart_id = OB_INVALID_OBJECT_ID;

      for (int64_t i = 0; OB_SUCC(ret) && i < inc_part_num; ++i) {
        inc_part = part_array[i];
        if (OB_ISNULL(inc_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inc_part is null", KR(ret));
        } else {
          ObSubPartition **subpart_array = inc_part->get_subpart_array();
          ObSubPartition *inc_subpart = nullptr;
          const int64_t inc_subpart_num = inc_part->get_subpartition_num();
          if (OB_ISNULL(subpart_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("subpart_array is null", KR(ret));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < inc_subpart_num; ++j) {
              inc_subpart = subpart_array[j];
              if (OB_ISNULL(inc_subpart)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("inc_subpart is null", KR(ret));
              } else if (FALSE_IT(subpart_id = inc_subpart->get_sub_part_id())) {
              } else if (OB_FAIL(exist_subpart_ids.set_refactored(subpart_id, OVERWRITE_FLAG/*flag*/))) {
                if (OB_HASH_EXIST == ret) {
                  LOG_WARN("duplicate subpartition when alter subpartition storage_cache_policy", KR(ret), K(subpart_id), KPC(inc_subpart), KPC(inc_part),
                      K(inc_subpart_num), K(inc_part_num));
                  LOG_USER_ERROR(OB_HASH_EXIST, "duplicate subpartition when alter subpartition storage_cache_policy");
                } else {
                  LOG_WARN("failed to set refactored for exist_subpart_ids", KR(ret), K(subpart_id), KPC(inc_subpart), KPC(inc_part),
                      K(inc_subpart_num), K(inc_part_num));
                }
              } else {
                storage::ObStorageCachePolicyType subpart_storage_cache_policy_type = inc_subpart->get_part_storage_cache_policy_type();
                const char *subpart_storage_cache_policy_str = nullptr;
                if (OB_FAIL(storage::ObStorageCacheGlobalPolicy::safely_get_str(subpart_storage_cache_policy_type, subpart_storage_cache_policy_str))) {
                  LOG_WARN("get part policy failed", K(ret), K(subpart_storage_cache_policy_type));
                } else if (OB_ISNULL(subpart_storage_cache_policy_str)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("subpart_storage_cache_policy_str is null", KR(ret));
                } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
                      ||OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))
                      ||OB_FAIL(dml.add_pk_column("part_id", inc_part->get_part_id()))
                      ||OB_FAIL(dml.add_pk_column("sub_part_id", inc_subpart->get_sub_part_id()))
                      ||OB_FAIL(dml.add_column("schema_version", schema_version_))
                      ||OB_FAIL(dml.add_column("storage_cache_policy", subpart_storage_cache_policy_str))) {
                  LOG_WARN("dml add column failed", KR(ret));
                } else if (FAILEDx(dml.finish_row())) {
                    LOG_WARN("finish row failed", KR(ret));
                } else {
                  HEAP_VAR(ObAddIncSubPartDMLGenerator, sub_part_dml_gen,
                          ori_table_, *inc_part, *inc_subpart, inc_part_num, inc_part->get_part_idx(),
                          inc_subpart->get_sub_part_idx(), schema_version_) {
                    if (OB_FAIL(sub_part_dml_gen.gen_dml(history_sub_dml))) {
                      LOG_WARN("gen dml history failed", KR(ret));
                    } else if (OB_FAIL(history_sub_dml.add_column("is_deleted", false))) {
                      LOG_WARN("add column failed", KR(ret));
                    } else if (OB_FAIL(history_sub_dml.finish_row())) {
                      LOG_WARN("fail to finish row", KR(ret));
                    }
                  }
                }
              }
            }
          }
        }
      }
      if (FAILEDx(dml.splice_batch_insert_update_sql(OB_ALL_SUB_PART_TNAME, subpart_sql))) {
        LOG_WARN("fail to splice batch insert update sql", KR(ret), K(subpart_sql));
      } else if (OB_FAIL(sql_client_.write(tenant_id, subpart_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write sql", KR(ret), K(subpart_sql), K(affected_rows));
      } else if (OB_UNLIKELY(2 * exist_subpart_ids.size() != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", KR(ret), K(exist_subpart_ids.size()), K(affected_rows), K(subpart_sql));
      } else if (FAILEDx(history_sub_dml.splice_batch_insert_sql(OB_ALL_SUB_PART_HISTORY_TNAME, subpart_history_sql))) {
        LOG_WARN("fail to splice batch insert update sql", KR(ret), K(subpart_history_sql));
      } else if (OB_FAIL(sql_client_.write(tenant_id, subpart_history_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write sql", KR(ret), K(subpart_history_sql), K(affected_rows));
      } else if (OB_UNLIKELY(exist_subpart_ids.size() != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", KR(ret), K(exist_subpart_ids.size()), K(affected_rows), K(subpart_history_sql));
      }
      FLOG_INFO("alter subpartition policy success", KR(ret), K(subpart_sql), K(subpart_history_sql), K(affected_rows), K(exist_subpart_ids.size()));
    }
  }
  return ret;
}
} //end of schema
} // end of share
} // end of oceanbase
