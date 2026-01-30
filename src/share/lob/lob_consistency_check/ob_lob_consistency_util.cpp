/**
 * Copyright (c) 2025 OceanBase
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

#include "share/lob/lob_consistency_check/ob_lob_consistency_util.h"
#include "share/lob/lob_consistency_check/ob_lob_check_task.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/tablet/ob_tablet.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace share {

using namespace common;
using namespace sqlclient;

const ObTabletID ObLobConsistencyUtil::LOB_CHECK_TABLET_ID = ObTabletID(ObTabletID::INVALID_TABLET_ID);

int ObLobConsistencyUtil::query_exception_tablets(uint64_t tenant_id, uint64_t ls_id, uint64_t table_id,
                                                  ObLobInconsistencyType inconsistency_type, ObMySQLTransaction &trans,
                                                  ObIAllocator &allocator, ObJsonNode *&tablets_json)
{
  int ret = OB_SUCCESS;
  tablets_json = nullptr;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult *result = nullptr;

    if (OB_FAIL(sql.assign_fmt("SELECT tablet_ids FROM %s WHERE tenant_id = %lu AND ls_id = %lu AND table_id = %lu AND "
                               "inconsistency_type = %d for update",
                               OB_ALL_LOB_CHECK_EXCEPTION_RESULT_TNAME, tenant_id, ls_id, table_id,
                               inconsistency_type))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(trans.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get result", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      ObString tablet_ids_str;
      char *tablet_ids_buf = nullptr;
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "tablet_ids", tablet_ids_str);
      if (OB_SUCC(ret) && !tablet_ids_str.empty()) {
        if (OB_ISNULL(tablet_ids_buf = static_cast<char *>(allocator.alloc(tablet_ids_str.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failt to allocate memory", K(ret), K(tablet_ids_str));
        } else if (OB_FAIL(ObJsonBaseFactory::get_json_tree(&allocator, tablet_ids_str, ObJsonInType::JSON_TREE,
                                                            tablets_json))) {
          LOG_WARN("fail to parse json", K(ret), K(tablet_ids_str));
        }
      }
    }
  }

  return ret;
}

int ObLobConsistencyUtil::insert_or_merge_exception_tablets(uint64_t tenant_id, uint64_t ls_id, uint64_t table_id,
                                                            ObJsonNode *new_tablets_json,
                                                            ObLobInconsistencyType inconsistency_type,
                                                            ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("LobExcepMerge");
  ObJsonNode *existing_tablets = nullptr;

  if (OB_ISNULL(new_tablets_json)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new_tablets_json is null", K(ret));
  } else if (OB_FAIL(query_exception_tablets(tenant_id, ls_id, table_id, inconsistency_type, trans, allocator,
                                             existing_tablets))) {
    LOG_WARN("fail to query exception tablets", K(ret), K(tenant_id), K(ls_id), K(table_id));
  } else {
    hash::ObHashSet<uint64_t> tablet_set;
    if (OB_FAIL(tablet_set.create(1024))) {
      LOG_WARN("fail to create tablet set", K(ret));
    } else {
      if (OB_NOT_NULL(existing_tablets)) {
        if (OB_FAIL(json_array_to_tablet_set(existing_tablets, tablet_set))) {
          LOG_WARN("fail to convert existing tablets to set", K(ret));
        }
      }
      // TODO: check whether tablet_set's count is raise
      if (OB_SUCC(ret)) {
        if (OB_FAIL(json_array_to_tablet_set(new_tablets_json, tablet_set))) {
          LOG_WARN("fail to convert new tablets to set", K(ret));
        }
      }

      ObString json_str;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tablet_set_to_json_string(tablet_set, allocator, json_str))) {
          LOG_WARN("fail to convert tablet set to json string", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ObSqlString sql;
        if (OB_ISNULL(existing_tablets)) {
          if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (tenant_id, ls_id, table_id, inconsistency_type, tablet_ids) "
                                     "VALUES (%lu, %lu, %lu, %d, '%.*s')",
                                     OB_ALL_LOB_CHECK_EXCEPTION_RESULT_TNAME, tenant_id, ls_id, table_id,
                                     inconsistency_type, json_str.length(), json_str.ptr()))) {
            LOG_WARN("fail to assign sql", K(ret));
          }
        } else {
          if (OB_FAIL(sql.assign_fmt("UPDATE %s SET tablet_ids = '%.*s' WHERE tenant_id = %lu AND ls_id = %lu AND "
                                     "table_id = %lu AND inconsistency_type = %d",
                                     OB_ALL_LOB_CHECK_EXCEPTION_RESULT_TNAME, json_str.length(), json_str.ptr(),
                                     tenant_id, ls_id, table_id, inconsistency_type))) {
            LOG_WARN("fail to assign sql", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          int64_t affected_rows = 0;
          if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
            LOG_WARN("fail to execute sql", K(ret), K(sql));
          }
        }
      }
    }
  }

  return ret;
}

int ObLobConsistencyUtil::remove_exception_tablets(uint64_t tenant_id, uint64_t ls_id, uint64_t table_id,
                                                   ObJsonNode *tablets_to_remove, ObMySQLTransaction &trans,
                                                   ObLobInconsistencyType inconsistency_type)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("LobExcepRemove");
  ObJsonNode *existing_tablets = nullptr;

  if (OB_ISNULL(tablets_to_remove)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablets to remove is null", K(ret), K(tenant_id), K(ls_id), K(table_id));
  } else if (OB_FAIL(query_exception_tablets(tenant_id, ls_id, table_id, inconsistency_type, trans, allocator,
                                             existing_tablets))) {
    LOG_WARN("fail to query exception tablets", K(ret), K(tenant_id), K(ls_id), K(table_id));
  } else if (OB_ISNULL(existing_tablets)) {
    LOG_INFO("no existing exception tablets, skip remove", K(tenant_id), K(ls_id), K(table_id));
  } else {
    hash::ObHashSet<uint64_t> existing_set;
    hash::ObHashSet<uint64_t> remove_set;

    if (OB_FAIL(existing_set.create(1024))) {
      LOG_WARN("fail to create existing set", K(ret));
    } else if (OB_FAIL(remove_set.create(1024))) {
      LOG_WARN("fail to create remove set", K(ret));
    } else if (OB_FAIL(json_array_to_tablet_set(existing_tablets, existing_set))) {
      LOG_WARN("fail to convert existing tablets to set", K(ret));
    } else if (OB_FAIL(json_array_to_tablet_set(tablets_to_remove, remove_set))) {
      LOG_WARN("fail to convert tablets to remove to set", K(ret));
    } else {
      hash::ObHashSet<uint64_t>::iterator iter = remove_set.begin();
      for (; iter != remove_set.end(); ++iter) {
        existing_set.erase_refactored(iter->first);
      }

      if (existing_set.empty()) {
        if (OB_FAIL(delete_exception_record_if_empty(tenant_id, ls_id, table_id, trans, inconsistency_type))) {
          LOG_WARN("fail to delete exception record", K(ret), K(tenant_id), K(ls_id), K(table_id));
        }
      } else {
        ObString json_str;
        if (OB_FAIL(tablet_set_to_json_string(existing_set, allocator, json_str))) {
          LOG_WARN("fail to convert tablet set to json string", K(ret));
        } else {
          ObSqlString sql;
          if (OB_FAIL(sql.assign_fmt(
                  "UPDATE %s SET tablet_ids = '%.*s' WHERE tenant_id = %lu AND ls_id = %lu AND table_id = %lu AND "
                  "inconsistency_type = %d",
                  OB_ALL_LOB_CHECK_EXCEPTION_RESULT_TNAME, json_str.length(), json_str.ptr(), tenant_id, ls_id,
                  table_id, inconsistency_type))) {
            LOG_WARN("fail to assign sql", K(ret));
          } else {
            int64_t affected_rows = 0;
            if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
              LOG_WARN("fail to execute sql", K(ret), K(sql));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObLobConsistencyUtil::delete_exception_record_if_empty(uint64_t tenant_id, uint64_t ls_id, uint64_t table_id, ObISQLClient &trans, int32_t inconsistency_type)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND ls_id = %lu AND table_id = %lu",
                             OB_ALL_LOB_CHECK_EXCEPTION_RESULT_TNAME, tenant_id, ls_id, table_id))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" AND inconsistency_type = %d", inconsistency_type))) {
    LOG_WARN("fail to append sql", K(ret));
  } else if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("delete exception record", K(tenant_id), K(ls_id), K(table_id), K(affected_rows));
  }

  return ret;
}

int ObLobConsistencyUtil::json_array_to_tablet_set(ObJsonNode *json_array, hash::ObHashSet<uint64_t> &tablet_set)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(json_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json_array is null", K(ret));
  } else if (json_array->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json is not array type", K(ret), K(json_array->json_type()));
  } else {
    ObJsonArray *json_arr = static_cast<ObJsonArray *>(json_array);
    uint64_t count = json_arr->element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObIJsonBase *elem = nullptr;
      if (OB_FAIL(json_arr->get_array_element(i, elem))) {
        LOG_WARN("fail to get array element", K(ret), K(i));
      } else if (OB_ISNULL(elem)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("element is null", K(ret), K(i));
      } else {
        uint64_t tablet_id = 0;
        if (elem->json_type() == ObJsonNodeType::J_UINT) {
          tablet_id = elem->get_uint();
        } else if (elem->json_type() == ObJsonNodeType::J_INT) {
          tablet_id = static_cast<uint64_t>(elem->get_int());
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid element type", K(ret), K(elem->json_type()));
          continue;
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(tablet_set.set_refactored(tablet_id))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to insert tablet_id to set", K(ret), K(tablet_id));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObLobConsistencyUtil::tablet_set_to_json_string(const hash::ObHashSet<uint64_t> &tablet_set,
                                                    ObIAllocator &allocator, ObString &json_str)
{
  int ret = OB_SUCCESS;
  void *arr_buf = nullptr;
  void *uint_buf = nullptr;
  ObJsonArray *json_arr = nullptr;
  ObJsonUint *json_uint = nullptr;

  if (OB_ISNULL(arr_buf = allocator.alloc(sizeof(ObJsonArray)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate json array", K(ret));
  } else {
    json_arr = new (arr_buf) ObJsonArray(&allocator);

    hash::ObHashSet<uint64_t>::const_iterator iter = tablet_set.begin();
    for (; OB_SUCC(ret) && iter != tablet_set.end(); ++iter) {
      uint64_t tablet_id = iter->first;
      if (OB_ISNULL(uint_buf = allocator.alloc(sizeof(ObJsonUint)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate json uint", K(ret));
      } else {
        json_uint = new (uint_buf) ObJsonUint(tablet_id);
        if (OB_FAIL(json_arr->append(json_uint))) {
          LOG_WARN("fail to append json uint to array", K(ret), K(tablet_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObJsonBuffer json_buffer(&allocator);
      if (OB_FAIL(json_arr->print(json_buffer, false, 0, false, 0))) {
        LOG_WARN("fail to print json array", K(ret));
      } else if (OB_FAIL(json_buffer.get_result_string(json_str))) {
        LOG_WARN("fail to get result string", K(ret));
      }
    }
  }

  return ret;
}

int ObLobConsistencyUtil::check_all_ls_finished(uint64_t tenant_id,
                                                bool is_repair_task,
                                                const ObString &row_key,
                                                uint64_t task_id,
                                                bool &all_finished)
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<storage::ObLSIterator> ls_iter;
  storage::ObLS *ls = nullptr;
  all_finished = true;
  ObSqlString sql;
  if (OB_ISNULL(MTL(storage::ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(MTL(storage::ObLSService *)->get_ls_iter(ls_iter, storage::ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls iterator", K(ret));
  } else {
    while (OB_SUCC(ret) && all_finished) {
      ObSEArray<ObTabletTablePair, 1> tablet_table_pairs;

      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next ls", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", K(ret));
      } else if (!row_key.empty()) {
        if (OB_FAIL(ObLobCheckTask::parse_table_and_tablet(tenant_id, ls, row_key, is_repair_task, 0, 0, 1,
                                                           tablet_table_pairs))) {
          LOG_WARN("fail to parse table and tablet", KR(ret), K(tenant_id), K(row_key));
        }
      } else if (is_repair_task) {
        if (OB_FAIL(ObLobCheckTask::read_orphan_data_to_pairs(tenant_id, ls, 0, tablet_table_pairs))) {
          LOG_WARN("fail to read orphan data to pairs", KR(ret), K(tenant_id), K(ls->get_ls_id()));
        }
      } else if (OB_FAIL(ObLobCheckTask::cache_table_and_tablet_pairs(tenant_id, ls,
                                                                      0,  // start_table_id
                                                                      0,  // start_tablet_id
                                                                      1,  // max_count
                                                                      tablet_table_pairs))) {
        LOG_WARN("fail to cache table and tablet pairs", KR(ret), K(tenant_id), K(ls->get_ls_id()));
      }

      if (OB_SUCC(ret) && !tablet_table_pairs.empty()) {
        SMART_VAR(ObISQLClient::ReadResult, res)
        {
          sqlclient::ObMySQLResult *result = nullptr;
          sql.reset();
          if (OB_FAIL(sql.append_fmt(
                  "SELECT count(*) as cnt FROM %s WHERE tenant_id = %ld and task_id = %ld and ls_id = %ld and (status "
                  "= %ld or status = %ld) and table_id >= 0",
                  OB_ALL_KV_TTL_TASK_TNAME, tenant_id, task_id, ls->get_ls_id().id(),
                  static_cast<int64_t>(ObTTLTaskStatus::OB_TTL_TASK_FINISH),
                  static_cast<int64_t>(ObTTLTaskStatus::OB_TTL_TASK_CANCEL)))) {
            LOG_WARN("fail to assign sql", KR(ret));
          } else if (OB_FAIL(GCTX.sql_proxy_->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
            LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
          } else if (OB_ISNULL(result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get mysql result failed", KR(ret), K(sql));
          } else if (OB_FAIL(result->next())) {
            LOG_WARN("fail to get next result", KR(ret), K(sql));
          } else {
            int64_t cnt = 0;
            EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "cnt", cnt, int64_t, true /*skip_null_error*/,
                                                       false /*skip_column_error*/, 0 /*default value*/);
            if (OB_SUCC(ret) && cnt == 0) {
              all_finished = false;
            }
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLobConsistencyUtil::handle_lob_task_info(table::ObTTLTaskCtx *&ctx, table::ObTTLTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  ObLobTaskInfo &lob_task_info = ctx->lob_task_info_;
  if (ctx->task_info_.tablet_id_ != task_info.tablet_id_ || OB_ITER_END == task_info.err_code_) {
    if (lob_task_info.lob_not_found_cnt_ == 0) {
      if (OB_FAIL(ObLobConsistencyUtil::merge_ctx_exception_tablets(lob_task_info.exception_allocator_,
                                                                    lob_task_info.not_found_removed_tablets_json_,
                                                                    ctx->task_info_))) {
        LOG_WARN("fail to merge ctx exception tablets", KR(ret));
      }
    } else if (OB_FAIL(ObLobConsistencyUtil::merge_ctx_exception_tablets(lob_task_info.exception_allocator_,
                                                                         lob_task_info.not_found_tablets_json_,
                                                                         ctx->task_info_))) {
      LOG_WARN("fail to merge ctx exception tablets", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (lob_task_info.lob_length_mismatch_cnt_ == 0) {
      if (OB_FAIL(ObLobConsistencyUtil::merge_ctx_exception_tablets(lob_task_info.exception_allocator_,
                                                                    lob_task_info.mismatch_len_removed_tablets_json_,
                                                                    ctx->task_info_))) {
        LOG_WARN("fail to merge ctx exception tablets", KR(ret));
      }
    } else if (OB_FAIL(ObLobConsistencyUtil::merge_ctx_exception_tablets(lob_task_info.exception_allocator_,
                                                                         lob_task_info.mismatch_len_tablets_json_,
                                                                         ctx->task_info_))) {
      LOG_WARN("fail to merge ctx exception tablets", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (lob_task_info.lob_orphan_cnt_ == 0) {
      if (OB_FAIL(ObLobConsistencyUtil::merge_ctx_exception_tablets(lob_task_info.exception_allocator_,
                                                                    lob_task_info.orphan_removed_tablets_json_,
                                                                    ctx->task_info_))) {
        LOG_WARN("fail to merge ctx exception tablets", KR(ret));
      }
    } else if (OB_FAIL(ObLobConsistencyUtil::merge_ctx_exception_tablets(lob_task_info.exception_allocator_,
                                                                         lob_task_info.orphan_tablets_json_,
                                                                         ctx->task_info_))) {
      LOG_WARN("fail to merge ctx exception tablets", KR(ret));
    }
    lob_task_info.lob_not_found_cnt_ = 0;
    lob_task_info.lob_length_mismatch_cnt_ = 0;
    lob_task_info.lob_orphan_cnt_ = 0;
  }
  lob_task_info.lob_not_found_cnt_ += task_info.ttl_del_cnt_;
  lob_task_info.lob_length_mismatch_cnt_ += task_info.max_version_del_cnt_;
  lob_task_info.lob_orphan_cnt_ += task_info.scan_cnt_;
  return ret;
}

int ObLobConsistencyUtil::merge_ctx_exception_tablets(ObArenaAllocator &allocator, ObJsonNode *&exception_table_tablets,
                                                      table::ObTTLTaskInfo &info)
{
  int ret = OB_SUCCESS;
  // Initialize ctx->exception_table_tablets_ if null
  if (OB_ISNULL(exception_table_tablets)) {
    void *buf = allocator.alloc(sizeof(ObJsonObject));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for json object", KR(ret));
    } else {
      exception_table_tablets = new (buf) ObJsonObject(&allocator);
    }
  }

  if (OB_SUCC(ret)) {
    // Construct table_id key
    char *table_id_buf = nullptr;
    if (OB_ISNULL(table_id_buf = reinterpret_cast<char *>(allocator.alloc(sizeof(uint64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for uint64_t", KR(ret));
    } else {
      *reinterpret_cast<uint64_t *>(table_id_buf) = info.table_id_;
      ObString table_id_key(sizeof(uint64_t), table_id_buf);
      ObJsonObject *exception_obj = static_cast<ObJsonObject *>(exception_table_tablets);
      ObJsonNode *tablets_node = exception_obj->get_value(table_id_key);

      if (OB_ISNULL(tablets_node)) {
        // Create new array for this table_id
        void *array_buf = allocator.alloc(sizeof(ObJsonArray));
        if (OB_ISNULL(array_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for json array", KR(ret));
        } else {
          ObJsonArray *tablets_array = new (array_buf) ObJsonArray(&allocator);
          // Add current tablet_id
          void *uint_buf = allocator.alloc(sizeof(ObJsonUint));
          if (OB_ISNULL(uint_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory for json uint", KR(ret));
          } else {
            ObJsonUint *tablet_id_node = new (uint_buf) ObJsonUint(info.tablet_id_.id());
            if (OB_FAIL(tablets_array->append(tablet_id_node))) {
              LOG_WARN("fail to append tablet id to array", KR(ret), K(info.tablet_id_));
            } else if (OB_FAIL(exception_obj->add(table_id_key, tablets_array))) {
              LOG_WARN("fail to add tablets array to exception object", KR(ret), K(table_id_key));
            }
          }
        }
      } else if (tablets_node->json_type() == ObJsonNodeType::J_ARRAY) {
        // Append to existing array
        ObJsonArray *tablets_array = static_cast<ObJsonArray *>(tablets_node);
        void *uint_buf = allocator.alloc(sizeof(ObJsonUint));
        if (OB_ISNULL(uint_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for json uint", KR(ret));
        } else {
          ObJsonUint *tablet_id_node = new (uint_buf) ObJsonUint(info.tablet_id_.id());
          if (OB_FAIL(tablets_array->append(tablet_id_node))) {
            LOG_WARN("fail to append tablet id to existing array", KR(ret), K(info.tablet_id_));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablets node is not array", KR(ret), K(table_id_key));
      }
    }
  }

  return ret;
}

int ObLobConsistencyUtil::filter_valid_tablets(const ObString &tablet_ids_str,
                                               const ObArray<ObTabletID> &all_tablet_ids,
                                               ObIAllocator &allocator,
                                               hash::ObHashSet<uint64_t> &valid_tablet_set,
                                               bool &all_valid,
                                               bool &all_invalid)
{
  int ret = OB_SUCCESS;
  all_valid = true;
  all_invalid = true;
  ObJsonNode *tablets_json = nullptr;

  if (tablet_ids_str.empty()) {
    all_invalid = true;
    all_valid = false;
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_tree(&allocator, tablet_ids_str, ObJsonInType::JSON_TREE, tablets_json))) {
    LOG_WARN("fail to parse tablet_ids json", K(ret), K(tablet_ids_str));
  } else if (tablets_json->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablets json is not array", K(ret));
  } else {
    ObJsonArray *json_arr = static_cast<ObJsonArray *>(tablets_json);
    uint64_t total_count = json_arr->element_count();

    for (uint64_t i = 0; OB_SUCC(ret) && i < total_count; ++i) {
      ObIJsonBase *elem = nullptr;
      uint64_t tablet_id_val = 0;

      if (OB_FAIL(json_arr->get_array_element(i, elem))) {
        LOG_WARN("fail to get array element", K(ret), K(i));
      } else if (OB_ISNULL(elem)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("element is null", K(ret), K(i));
      } else {
        if (elem->json_type() == ObJsonNodeType::J_UINT) {
          tablet_id_val = elem->get_uint();
        } else if (elem->json_type() == ObJsonNodeType::J_INT) {
          tablet_id_val = static_cast<uint64_t>(elem->get_int());
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid tablet id type", K(ret), K(elem->json_type()));
        }

        // Check tablet existence
        if (OB_SUCC(ret)) {
          ObTabletID tablet_id(tablet_id_val);
          if (!common::is_contain(all_tablet_ids, tablet_id)) {
            // Tablet doesn't exist
            LOG_INFO("tablet not exist", K(tablet_id));
            all_valid = false;
          } else if (OB_FAIL(valid_tablet_set.set_refactored(tablet_id_val))) {
            all_invalid = false;
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to set valid tablet", K(ret), K(tablet_id_val));
            }
          } else {
            // Tablet exists
            all_invalid = false;
          }
        }
      }
    }
  }

  return ret;
}

int ObLobConsistencyUtil::update_or_delete_exception_record(uint64_t tenant_id,
                                                            uint64_t ls_id,
                                                            uint64_t table_id,
                                                            int32_t inconsistency_type,
                                                            const ObString &json_str,
                                                            bool should_delete,
                                                            ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (should_delete) {
    // Delete record
    if (OB_FAIL(ObLobConsistencyUtil::delete_exception_record_if_empty(tenant_id, ls_id, table_id, trans, inconsistency_type))) {
      LOG_WARN("fail to remove exception tablets", K(ret));
    }
  } else {
    // Update record with valid tablets
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt(
        "UPDATE %s SET tablet_ids = '%.*s' WHERE tenant_id = %lu AND ls_id = %lu AND table_id = %lu AND inconsistency_type = %d",
        OB_ALL_LOB_CHECK_EXCEPTION_RESULT_TNAME, json_str.length(), json_str.ptr(),
        tenant_id, ls_id, table_id, inconsistency_type))) {
      LOG_WARN("fail to assign update sql", K(ret));
    } else if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute update sql", K(ret), K(sql));
    } else {
      LOG_INFO("updated exception record with valid tablets", K(tenant_id), K(ls_id),
                  K(table_id), K(inconsistency_type), K(json_str));
    }
  }
  return ret;
}

int ObLobConsistencyUtil::clean_invalid_exception_records(uint64_t tenant_id,
                                                           ObMySQLProxy &sql_proxy,
                                                           uint64_t &last_processed_ls_id,
                                                           uint64_t &last_processed_table_id,
                                                           int64_t batch_size,
                                                           int64_t &processed_count)
{
  int ret = OB_SUCCESS;
  processed_count = 0;
  ObSqlString sql;
  ObArenaAllocator allocator("LobCleanOrphan");
  ObMySQLTransaction trans;

  // Collect operations to execute after result set is released
  ObSEArray<PendingOp, 64> pending_ops;
  if (OB_FAIL(trans.start(&sql_proxy, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("fail to start transaction", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT ls_id, table_id, inconsistency_type, tablet_ids FROM %s WHERE tenant_id = %lu",
                                     OB_ALL_LOB_CHECK_EXCEPTION_RESULT_TNAME, tenant_id))) {
    LOG_WARN("fail to assign sql", K(ret));
  } else if (last_processed_ls_id > 0 || last_processed_table_id > 0) {
    if (OB_FAIL(sql.append_fmt(" AND (ls_id > %lu OR (ls_id = %lu AND table_id > %lu))",
                               last_processed_ls_id, last_processed_ls_id, last_processed_table_id))) {
      LOG_WARN("fail to append pagination condition", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append_fmt(" ORDER BY ls_id, table_id, inconsistency_type LIMIT %ld for update", batch_size))) {
      LOG_WARN("fail to append order and limit", K(ret), K(batch_size));
    }
  }
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult *result = nullptr;
      share::schema::ObSchemaGetterGuard schema_guard;
      storage::ObLSService *ls_service = MTL(storage::ObLSService*);
      hash::ObHashSet<uint64_t> valid_tablet_set;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(trans.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", K(ret));
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
      } else if (OB_ISNULL(ls_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls service is null", K(ret));
      } else if (OB_FAIL(valid_tablet_set.create(1024))) {
        LOG_WARN("fail to create valid tablet set", K(ret));
      }

      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        uint64_t rec_ls_id = 0;
        uint64_t rec_table_id = 0;
        int32_t inconsistency_type = 0;
        ObString tablet_ids_str;
        bool should_delete = false;
        bool should_update = false;

        // Extract fields
        EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", rec_ls_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "table_id", rec_table_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "inconsistency_type", inconsistency_type, int32_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "tablet_ids", tablet_ids_str);

        last_processed_ls_id = rec_ls_id;
        last_processed_table_id = rec_table_id;
        processed_count++;

        // Check LS existence
        storage::ObLSHandle ls_handle;
        ObLSID ls_id(rec_ls_id);
        if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, storage::ObLSGetMod::STORAGE_MOD))) {
          if (OB_LS_NOT_EXIST == ret) {
            should_delete = true;
            ret = OB_SUCCESS;
            LOG_INFO("LS not exist, will delete exception record", K(tenant_id), K(rec_ls_id), K(rec_table_id));
          } else {
            LOG_WARN("fail to get ls", K(ret), K(ls_id));
            ret = OB_SUCCESS;
            continue;
          }
        }
        PendingOp op;
        // Check table and tablet existence
        if (!should_delete) {
          ObArray<ObTabletID> tablet_ids;
          bool is_valid = false;
          if (OB_FAIL(ObLobCheckTask::check_table_valid_and_return_tablet(tenant_id, rec_table_id, is_valid, tablet_ids))) {
            if (ret != OB_TABLE_NOT_EXIST) {
              LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(rec_table_id));
            } else {
              should_delete = true;
              LOG_INFO("table not exist, will delete exception record", K(tenant_id), K(rec_ls_id), K(rec_table_id));
            }
            ret = OB_SUCCESS;
          } else if (!is_valid) {
            should_delete = true;
          } else {
            bool all_valid = false;
            bool all_invalid = false;
            valid_tablet_set.reuse();
            if (OB_FAIL(filter_valid_tablets(tablet_ids_str, tablet_ids, allocator, valid_tablet_set, all_valid, all_invalid))) {
              LOG_WARN("fail to filter valid tablets", K(ret), K(tenant_id), K(rec_ls_id), K(rec_table_id));
            } else if (all_invalid) {
              should_delete = true;
            } else if (!all_valid) {
              should_update = true;
              if (OB_FAIL(tablet_set_to_json_string(valid_tablet_set, allocator, op.json_str_))) {
                LOG_WARN("fail to convert valid tablet set to json", K(ret));
              }
            }
          }
        }

        // Collect pending operation (do NOT execute write while result is still active)
        if (should_delete || should_update) {
          op.ls_id_ = rec_ls_id;
          op.table_id_ = rec_table_id;
          op.inconsistency_type_ = inconsistency_type;
          op.should_delete_ = should_delete;

          if (OB_FAIL(pending_ops.push_back(op))) {
            LOG_WARN("fail to push pending op", K(ret), K(tenant_id), K(rec_ls_id), K(rec_table_id));
            ret = OB_SUCCESS;  // Continue processing other records
            continue;
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      valid_tablet_set.destroy();
    }  // SMART_VAR scope ends here - result is released
  }

  // Phase 2: Execute all pending operations after result set is released
  for (int64_t i = 0; OB_SUCC(ret) && i < pending_ops.count(); ++i) {
    const PendingOp &op = pending_ops.at(i);
    int tmp_ret = update_or_delete_exception_record(tenant_id, op.ls_id_, op.table_id_,
                                                    op.inconsistency_type_, op.json_str_,
                                                    op.should_delete_, trans);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to update or delete exception record", K(tmp_ret),
              K(tenant_id), K(op.ls_id_), K(op.table_id_), K(op.inconsistency_type_));
    }
  }
  allocator.reuse();
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
