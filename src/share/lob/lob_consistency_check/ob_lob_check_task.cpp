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

#include "share/lob/lob_consistency_check/ob_lob_check_task.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_table_schema.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/lob/ob_lob_access_param.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/ob_lob_meta.h"
#include "storage/lob/ob_lob_persistent_iterator.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ob_storage_struct.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "lib/json_type/ob_json_base.h"
#include "ob_lob_consistency_util.h"

namespace oceanbase {
namespace share {

const ObString ObLobCheckTask::LOB_META_SCAN_INDEX = "LOB META";

ObLobBatchMetaDelIter::ObLobBatchMetaDelIter()
    : storage::ObValueRowIterator(), dml_base_param_(nullptr), start_seq_no_st_(), used_seq_cnt_(0)
{
}

ObLobBatchMetaDelIter::~ObLobBatchMetaDelIter()
{
  dml_base_param_ = nullptr;
  start_seq_no_st_.reset();
  used_seq_cnt_ = 0;
}

int ObLobBatchMetaDelIter::init(ObDMLBaseParam &dml_base_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(storage::ObValueRowIterator::init())) {
    LOG_WARN("fail to init value row iterator", K(ret));
  } else {
    dml_base_param_ = &dml_base_param;
    start_seq_no_st_ = dml_base_param.spec_seq_no_;
  }
  return ret;
}

int ObLobBatchMetaDelIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  dml_base_param_->spec_seq_no_ = start_seq_no_st_ + used_seq_cnt_;
  used_seq_cnt_++;
  if (OB_FAIL(storage::ObValueRowIterator::get_next_row(row))) {
    LOG_WARN("fail to get next row", K(ret));
  }
  return ret;
}

ObLobCheckTask::ObLobCheckTask()
    : ObITask(ObITask::TASK_TYPE_LOB_CHECK_TASK), is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), table_id_(0),
      meta_table_id_(OB_INVALID_ID), tablet_id_(), ls_id_(), ls_(nullptr),
      main_allocator_(ObMemAttr(MTL_ID(), "LobCkMaTask")), aux_allocator_(ObMemAttr(MTL_ID(), "LobCkAuxTask")),
      allocator_(ObMemAttr(MTL_ID(), "LobChecAlloc")), main_table_iter_(), aux_table_iter_(nullptr), lob_column_idxs_(),
      tablet_ttl_scheduler_(nullptr), param_(), info_(), lob_id_map_(), tablet_table_pairs_(), scan_tablet_idx_(0),
      need_next_tablet_(false)
{
}

ObLobCheckTask::~ObLobCheckTask()
{
  // 释放 main_table_iter_ (通过 ObAccessService::table_scan 获取，需要归还)
  if (OB_NOT_NULL(main_table_iter_)) {
    ObAccessService *oas = MTL(ObAccessService *);
    if (OB_NOT_NULL(oas)) {
      oas->revert_scan_iter(main_table_iter_);
    }
    main_table_iter_ = nullptr;
  }

  // 释放 aux_table_iter_ (使用 OB_NEWx 分配的内存，手动析构和释放)
  if (OB_NOT_NULL(aux_table_iter_)) {
    aux_table_iter_->~ObLobMetaIterator();
    aux_table_iter_ = nullptr;
  }

  // 销毁 lob_id_map_
  if (lob_id_map_.created()) {
    lob_id_map_.destroy();
  }
}

int ObLobCheckTask::init(table::ObTabletTTLScheduler *tablet_ttl_scheduler, const table::ObTTLTaskParam &param,
                         const table::ObTTLTaskInfo &info)
{
  int ret = OB_SUCCESS;
  tablet_ttl_scheduler_ = tablet_ttl_scheduler;
  tenant_id_ = info.tenant_id_;
  ObLSHandle ls_handle;
  ls_id_ = info.ls_id_;
  is_repair_task_ = tablet_ttl_scheduler_->get_task_type() == ObTTLType::LOB_REPAIR;
  info_ = info;
  info_.reset_cnt();
  table_id_ = info.table_id_;
  tablet_id_ = info.tablet_id_;
  info_.scan_index_ = is_repair_task_ ? LOB_META_SCAN_INDEX : ObTTLTaskConstant::TTL_SCAN_INDEX_DEFAULT_VALUE;
  bool is_valid = false;
  ObArray<ObTabletID> tablet_ids;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("LobCheckTask already inited", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (ls_handle.get_ls()->is_offline()) {
    ret = OB_LS_OFFLINE;
    LOG_WARN("log stream is offline", K(ret), K(ls_id_));
  } else if (FALSE_IT(ls_ = ls_handle.get_ls())) {
  } else if (OB_FAIL(lob_id_map_.create(BATCH_SIZE, ObMemAttr(tenant_id_, "LobIdMap")))) {
    LOG_WARN("fail to create lob_id_map", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id_ || !ls_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), K(tablet_id_), K(ls_id_));
  } else if (!info.row_key_.empty() && OB_FAIL(ob_write_string(allocator_, info.row_key_, info_.row_key_))) {
    LOG_WARN("fail to write string to rowkey", K(ret), K(info.row_key_));
  } else if (OB_FAIL(check_table_valid_and_return_tablet(tenant_id_, table_id_, is_valid, tablet_ids))) {
    if (ret != OB_TABLE_NOT_EXIST) {
      LOG_WARN("fail to check table valid and return tablet", KR(ret), K(tenant_id_), K(table_id_));
    } else {
      // overwrite ret
      ret = OB_SUCCESS;
      need_next_tablet_ = true;
    }
  } else if (is_valid && !is_contain(tablet_ids, tablet_id_)) {
    need_next_tablet_ = true;
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObLobCheckTask::reuse_iterators()
{
  if (OB_NOT_NULL(main_table_iter_)) {
    ObAccessService *oas = MTL(ObAccessService *);
    if (OB_NOT_NULL(oas)) {
      oas->revert_scan_iter(main_table_iter_);
    }
    main_table_iter_ = nullptr;
  }

  if (OB_NOT_NULL(aux_table_iter_)) {
    aux_table_iter_->~ObLobMetaIterator();
    aux_table_iter_ = nullptr;
  }
  main_allocator_.reuse();
  aux_allocator_.reuse();
}

int ObLobCheckTask::check_table_valid_and_return_tablet(const uint64_t tenant_id, uint64_t table_id, bool &is_valid,
                                                        ObArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  is_valid = false;
  tablet_ids.reset();
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(table_schema) || table_schema->is_in_recyclebin()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret), K(table_id));
  } else if (!table_schema->is_sys_table() && !table_schema->is_user_table() && !table_schema->is_index_table()) {
  } else if (!table_schema->has_lob_aux_table()) {
  } else if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("failed to get tablet ids", KR(ret), K(table_id));
  } else if (tablet_ids.count() == 0) {
    LOG_WARN("no tablet ids", KR(ret), K(table_id));
  } else {
    ob_sort(tablet_ids.begin(), tablet_ids.end());
    is_valid = true;
  }
  return ret;
}

int ObLobCheckTask::check_tablet_belongs_to_ls(storage::ObLS *ls, const ObTabletID &tablet_id, bool &belong)
{
  int ret = OB_SUCCESS;
  belong = false;
  ObTabletHandle tablet_handle;
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", K(ret));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    belong = true;
  }
  return ret;
}

int ObLobCheckTask::cache_table_and_tablet_pairs(const uint64_t tenant_id, storage::ObLS *ls,
                                                 const uint64_t start_table_id, const uint64_t start_tablet_id,
                                                 const int64_t max_count, ObIArray<ObTabletTablePair> &pairs)
{
  int ret = OB_SUCCESS;
  int64_t cached_count = 0;
  ObTabletHandle tablet_handle;
  ObSEArray<uint64_t, table::ObTabletTTLScheduler::DEFAULT_TABLE_ARRAY_SIZE> table_id_array;
  ObTimeGuard guard("cache_table_and_tablet_pairs", 5 * 1000 * 10000);  // 5秒超时

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", K(ret));
  } else if (OB_FAIL(ObTTLUtil::get_tenant_table_ids(tenant_id, table_id_array))) {
    LOG_WARN("fail to get tenant table ids", KR(ret), K(tenant_id));
  } else {
    lib::ob_sort(table_id_array.begin(), table_id_array.end());
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_id_array.count() && cached_count < max_count; i++) {
    const int64_t table_id = table_id_array.at(i);
    if (table_id < start_table_id) {
      continue;
    }

    ObArray<ObTabletID> tablet_ids;
    bool is_valid = false;

    if (OB_FAIL(ObLobCheckTask::check_table_valid_and_return_tablet(tenant_id, table_id, is_valid, tablet_ids))) {
      if (OB_TABLE_NOT_EXIST != ret) {
        LOG_WARN("fail to check table valid and return tablet", KR(ret), K(table_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (!is_valid) {
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_ids.count() && cached_count < max_count; j++) {
        bool belong = false;
        if (tablet_ids.at(j).id() <= start_tablet_id) {
          continue;
        } else if (OB_FAIL(check_tablet_belongs_to_ls(ls, tablet_ids.at(j), belong))) {
          LOG_WARN("fail to check tablet belongs to ls", KR(ret), K(tablet_ids.at(j)));
        } else if (!belong) {
        } else if (OB_FAIL(pairs.push_back(ObTabletTablePair(tablet_ids.at(j), table_id)))) {
          LOG_WARN("fail to push back tablet table pair", KR(ret), K(tablet_ids.at(j)), K(table_id));
        } else {
          cached_count++;
        }
      }
    }
  }
  return ret;
}

int ObLobCheckTask::process()
{
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_LOB_CHECK);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LobCheckTask not inited", K(ret));
  } else {
    bool need_stop = false;
    if (OB_FAIL(tablet_ttl_scheduler_->report_task_status(info_, param_, need_stop, false))) {
      LOG_WARN("fail to report ttl task status", KR(ret));
    }
    need_stop = false;
    while (!need_stop && OB_SUCC(ret)) {
      lib::ContextParam param;
      param.set_mem_attr(tenant_id_, "LobCheckMemCtx", ObCtxIds::DEFAULT_CTX_ID)
          .set_properties(lib::USE_TL_PAGE_OPTIONAL);

      CREATE_WITH_TEMP_CONTEXT(param)
      {
        if (OB_FAIL(process_one())) {
          LOG_WARN("fail to process one", KR(ret));
        }
        if (OB_FAIL(tablet_ttl_scheduler_->report_task_status(info_, param_, need_stop))) {
          LOG_WARN("fail to report ttl task status", KR(ret));
        }
        LOG_INFO("wuhuang.wh::process_one", K(ret), K(info_), K(tenant_id_), K(ls_id_), K(table_id_), K(tablet_id_));
      }
    }
  }
  tablet_ttl_scheduler_->dec_dag_ref();
  return ret;
}

int ObLobCheckTask::process_one()
{
  int ret = OB_SUCCESS;
  if (need_next_tablet_) {
    // 扫描下一个tablet
    if (OB_FAIL(next_tablet_task())) {
      LOG_WARN("fail to next tablet task", KR(ret));
    }
    need_next_tablet_ = false;
  }
  if (OB_FAIL(ret) || OB_ITER_END == info_.err_code_) {
  } else if (!is_repair_task_ && info_.scan_index_.case_compare(LOB_META_SCAN_INDEX) != 0) {
    // 1. 扫描主表，统计行数和lob列数
    if (OB_FAIL(scan_main_table())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to scan main table", K(ret));
      } else {
        info_.scan_index_ = LOB_META_SCAN_INDEX;
        info_.row_key_.reset();
        ret = OB_SUCCESS;
      }
    }
  } else if (info_.scan_index_.case_compare(LOB_META_SCAN_INDEX) == 0) {
    // 2. 扫描辅助表看下是否存在主表不存在的数据
    if (OB_FAIL(scan_auxiliary_table())) {
      LOG_WARN("fail to scan auxiliary table", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scan index", K(ret), K(info_.scan_index_));
  }
  if (ret != OB_SUCCESS) {
    info_.err_code_ = ret;
  }
  LOG_INFO("process one finished", K(ret), K(info_), K(tenant_id_), K(ls_id_), K(table_id_), K(tablet_id_));
  return ret;
}

int ObLobCheckTask::find_meta_table_id()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret), K(tenant_id_), K(table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret), K(tenant_id_), K(table_id_));
  } else if (table_schema->has_lob_aux_table()) {
    meta_table_id_ = table_schema->get_aux_lob_meta_tid();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table has no lob aux table", KR(ret), K(tenant_id_), K(table_id_));
  }
  return ret;
}

int ObLobCheckTask::parse_repair_table_and_tablet(const uint64_t tenant_id, storage::ObLS *ls,
                                                   const common::ObString &row_key, const uint64_t start_table_id,
                                                   const uint64_t start_tablet_id, const int64_t max_count,
                                                   ObIArray<ObTabletTablePair> &pairs)
{
  int ret = OB_SUCCESS;
  uint16_t saved_cnt = 0;
  ObIJsonBase *json_base = nullptr;
  ObMySQLTransaction trans;
  ObArenaAllocator tmp_allocator;
  if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("fail to start transation", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&tmp_allocator, row_key, ObJsonInType::JSON_BIN,
                                                      ObJsonInType::JSON_TREE, json_base, 0))) {
    LOG_WARN("fail to get json base", K(ret));
  } else if (OB_ISNULL(json_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json base is null", K(ret));
  } else if (json_base->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json is not an object", K(ret));
  } else {
    ObJsonObject *json_object = static_cast<ObJsonObject *>(json_base);
    const uint64_t *table_id = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < json_object->element_count(); i++) {
      ObString table_id_str;
      ObIJsonBase *tablet_ids_tree = nullptr;
      bool is_valid = false;
      ObArray<ObTabletID> all_tablet_ids;
      if (OB_FAIL(json_object->get_key(i, table_id_str))) {
        LOG_WARN("fail to get key", K(ret));
      } else if (OB_ISNULL(table_id = reinterpret_cast<const uint64_t *>(table_id_str.ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table id is null", K(ret));
      } else if (*table_id < start_table_id) {
      } else if (OB_FAIL(check_table_valid_and_return_tablet(tenant_id, *table_id, is_valid, all_tablet_ids))) {
        if (ret != OB_TABLE_NOT_EXIST) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("check table valid and return tablet failed", KR(ret), K(*table_id));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (!is_valid) {
      } else if (OB_FAIL(json_object->get_object_value(i, tablet_ids_tree))) {
        LOG_WARN("fail to get value", K(ret));
      } else if (OB_ISNULL(tablet_ids_tree)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet ids tree is null", K(ret));
      } else if (tablet_ids_tree->json_type() == ObJsonNodeType::J_NULL) {
        for (int curr_tablet_idx = 0;
             OB_SUCC(ret) && curr_tablet_idx < all_tablet_ids.count() && saved_cnt < TABLE_AND_TABLET_BATCH_SIZE;
             curr_tablet_idx++) {
          bool belong = false;
          if (all_tablet_ids.at(curr_tablet_idx).id() <= start_tablet_id) {
          } else if (OB_FAIL(check_tablet_belongs_to_ls(ls, all_tablet_ids.at(curr_tablet_idx), belong))) {
            LOG_WARN("fail to check tablet belongs to ls", KR(ret), K(all_tablet_ids.at(curr_tablet_idx)));
          } else if (!belong) {
          } else if (OB_FAIL(pairs.push_back(ObTabletTablePair(all_tablet_ids.at(curr_tablet_idx), *table_id)))) {
            LOG_WARN("fail to push back tablet table pair", K(ret));
          } else {
            saved_cnt++;
          }
        }
      } else if (tablet_ids_tree->json_type() != ObJsonNodeType::J_ARRAY) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tablet ids tree is not an array", K(ret));
      } else {
        ObJsonArray *tablet_ids_array = static_cast<ObJsonArray *>(tablet_ids_tree);
        ObSEArray<uint64_t, 8> tablet_ids;
        for (int j = 0;
             OB_SUCC(ret) && j < tablet_ids_array->element_count() && saved_cnt < TABLE_AND_TABLET_BATCH_SIZE; j++) {
          ObIJsonBase *tablet_id_node = nullptr;
          if (OB_FAIL(tablet_ids_array->get_array_element(j, tablet_id_node))) {
            LOG_WARN("fail to get array element", K(ret));
          } else if (OB_ISNULL(tablet_id_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet id tree is null", K(ret));
          } else if (tablet_id_node->json_type() != ObJsonNodeType::J_INT &&
                     tablet_id_node->json_type() != ObJsonNodeType::J_UINT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("tablet id node is not an integer", K(ret));
          } else if (OB_FAIL(tablet_ids.push_back(static_cast<ObJsonInt *>(tablet_id_node)->value()))) {
            LOG_WARN("fail to push back tablet id", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ob_sort(tablet_ids.begin(), tablet_ids.end());
          for (int curr_tablet_idx = 0;
               OB_SUCC(ret) && curr_tablet_idx < tablet_ids.count() && saved_cnt < TABLE_AND_TABLET_BATCH_SIZE;
               curr_tablet_idx++) {
            bool belong = false;
            if (tablet_ids.at(curr_tablet_idx) <= start_tablet_id) {
            } else if (!common::is_contain(all_tablet_ids, ObTabletID(tablet_ids.at(curr_tablet_idx)))) {
            } else if (OB_FAIL(check_tablet_belongs_to_ls(ls, ObTabletID(tablet_ids.at(curr_tablet_idx)), belong))) {
              LOG_WARN("fail to check tablet belongs to ls", KR(ret), K(tablet_ids.at(curr_tablet_idx)));
            } else if (!belong) {
            } else if (OB_FAIL(
                           pairs.push_back(ObTabletTablePair(ObTabletID(tablet_ids.at(curr_tablet_idx)), *table_id)))) {
              LOG_WARN("fail to push back tablet table pair", K(ret));
            } else {
              saved_cnt++;
            }
          }
        }
      }
    }
  }

  if (trans.is_started()) {
    bool commit = (OB_SUCCESS == ret);
    int tmp_ret = ret;
    if (OB_FAIL(trans.end(commit))) {
      LOG_WARN("faile to end trans", "commit", commit, KR(ret));
    }
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }
  return ret;
}

int ObLobCheckTask::parse_check_table_and_tablet(const uint64_t tenant_id, storage::ObLS *ls,
                                                 const common::ObString &row_key, const uint64_t start_table_id,
                                                 const uint64_t start_tablet_id, const int64_t max_count,
                                                 ObIArray<ObTabletTablePair> &pairs)
{
  int ret = OB_SUCCESS;
  uint16_t saved_cnt = 0;
  ObIJsonBase *json_base = nullptr;
  ObArenaAllocator tmp_allocator;
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&tmp_allocator, row_key, ObJsonInType::JSON_BIN, ObJsonInType::JSON_TREE,
                                               json_base, 0))) {
    LOG_WARN("fail to get json base", K(ret));
  } else if (OB_ISNULL(json_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json base is null", K(ret));
  } else if (json_base->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json is not an array", K(ret), K(json_base->json_type()));
  } else {
    ObJsonArray *json_array = static_cast<ObJsonArray *>(json_base);
    for (int i = 0; OB_SUCC(ret) && i < json_array->element_count() && saved_cnt < TABLE_AND_TABLET_BATCH_SIZE; i++) {
      ObIJsonBase *table_id_node = nullptr;
      bool is_valid = false;
      ObArray<ObTabletID> all_tablet_ids;
      uint64_t table_id = 0;
      if (OB_FAIL(json_array->get_array_element(i, table_id_node))) {
        LOG_WARN("fail to get array element", K(ret));
      } else if (OB_ISNULL(table_id_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table id node is null", K(ret));
      } else if (table_id_node->json_type() != ObJsonNodeType::J_INT &&
                 table_id_node->json_type() != ObJsonNodeType::J_UINT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("table id node is not an integer", K(ret));
      } else if (OB_FALSE_IT(table_id = static_cast<ObJsonInt *>(table_id_node)->value())) {
      } else if (table_id < start_table_id) {
      } else if (OB_FAIL(check_table_valid_and_return_tablet(tenant_id, table_id, is_valid, all_tablet_ids))) {
        if (OB_TABLE_NOT_EXIST != ret) {
          LOG_WARN("fail to check table valid and return tablet", KR(ret), K(table_id));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (!is_valid) {
      } else {
        for (int curr_tablet_idx = 0;
             OB_SUCC(ret) && curr_tablet_idx < all_tablet_ids.count() && saved_cnt < TABLE_AND_TABLET_BATCH_SIZE;
             curr_tablet_idx++) {
          bool belong = false;
          if (all_tablet_ids.at(curr_tablet_idx).id() <= start_tablet_id) {
          } else if (OB_FAIL(check_tablet_belongs_to_ls(ls, all_tablet_ids.at(curr_tablet_idx), belong))) {
            LOG_WARN("fail to check tablet belongs to ls", KR(ret), K(all_tablet_ids.at(curr_tablet_idx)));
          } else if (!belong) {
          } else if (OB_FAIL(pairs.push_back(ObTabletTablePair(all_tablet_ids.at(curr_tablet_idx), table_id)))) {
            LOG_WARN("fail to push back tablet table pair", K(ret));
          } else {
            saved_cnt++;
          }
        }
      }
    }
  }
  return ret;
}

int ObLobCheckTask::read_orphan_data_to_pairs(const uint64_t tenant_id, storage::ObLS *ls,
                                              const uint64_t start_table_id, ObIArray<ObTabletTablePair> &pairs)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObArenaAllocator tmp_allocator("LobOrphanRead");
  uint64_t table_id = 0;
  ObString tablet_ids_str;
  ObJsonNode *tablets_json = nullptr;
  bool is_valid = false;
  ObArray<ObTabletID> all_tablet_ids;

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", K(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("fail to start transaction", KR(ret), K(tenant_id));
  } else {
    // 从异常结果表按 table_id 递增读取一条孤儿数据记录
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT table_id, tablet_ids FROM %s WHERE tenant_id = %lu AND ls_id = %lu AND "
                               "inconsistency_type = %d AND table_id > %lu ORDER BY table_id ASC LIMIT 1",
                               OB_ALL_LOB_CHECK_EXCEPTION_RESULT_TNAME, tenant_id, ls->get_ls_id().id(),
                               ObLobInconsistencyType::LOB_ORPHAN_TYPE, start_table_id))) {
      LOG_WARN("fail to assign sql", K(ret));
    }
    while (OB_SUCC(ret) && pairs.count() == 0) {
      {
        ObMySQLProxy::MySQLResult res;
        common::sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(trans.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get result", K(ret));
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          // 提取 table_id 和 tablet_ids
          EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "tablet_ids", tablet_ids_str);

          if (OB_SUCC(ret)) {
            // 解析 tablet_ids JSON 数组
            if (OB_FAIL(ObJsonBaseFactory::get_json_tree(&tmp_allocator, tablet_ids_str, ObJsonInType::JSON_TREE,
                                                         tablets_json))) {
              LOG_WARN("fail to parse tablet_ids json", KR(ret), K(tablet_ids_str));
            } else if (OB_ISNULL(tablets_json) || tablets_json->json_type() != ObJsonNodeType::J_ARRAY) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("tablet_ids is not an array", KR(ret), K(tablet_ids_str));
            }
          }
        }
      }
      if (OB_SUCC(ret) && table_id > 0) {
        // 检查 table_id 是否有效
        if (OB_FAIL(check_table_valid_and_return_tablet(tenant_id, table_id, is_valid, all_tablet_ids))) {
          if (ret != OB_TABLE_NOT_EXIST) {
            LOG_INFO("fail to check table valid and return tablet", KR(ret), K(tenant_id), K(table_id));
          } else {
            // overwrite ret
            ret = OB_SUCCESS;
            LOG_INFO("table not exist, will read next orphan data", KR(ret), K(tenant_id), K(table_id));
          }
        } else if (!is_valid) {
        } else {
          // table_id 有效，处理 tablet_ids
          ObJsonArray *tablets_array = static_cast<ObJsonArray *>(tablets_json);

          for (uint64_t i = 0; OB_SUCC(ret) && i < tablets_array->element_count(); ++i) {
            ObIJsonBase *tablet_node = nullptr;
            uint64_t tablet_id = 0;
            bool belong = false;

            if (OB_FAIL(tablets_array->get_array_element(i, tablet_node))) {
              LOG_WARN("fail to get array element", K(ret), K(i));
            } else if (OB_ISNULL(tablet_node)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tablet node is null", K(ret), K(i));
            } else if (tablet_node->json_type() == ObJsonNodeType::J_UINT) {
              tablet_id = tablet_node->get_uint();
            } else if (tablet_node->json_type() == ObJsonNodeType::J_INT) {
              tablet_id = static_cast<uint64_t>(tablet_node->get_int());
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid tablet node type", K(ret), K(tablet_node->json_type()));
              continue;
            }

            if (OB_SUCC(ret)) {
              // 检查 tablet_id 是否在合法列表中
              if (!common::is_contain(all_tablet_ids, ObTabletID(tablet_id))) {
              } else if (OB_FAIL(check_tablet_belongs_to_ls(ls, ObTabletID(tablet_id), belong))) {
                LOG_WARN("fail to check tablet belongs to ls", KR(ret), K(tablet_id));
              } else if (!belong) {
                LOG_INFO("tablet not belong to current ls", K(tablet_id), K(ls->get_ls_id()));
              } else if (OB_FAIL(pairs.push_back(ObTabletTablePair(ObTabletID(tablet_id), table_id)))) {
                LOG_WARN("fail to push back tablet table pair", KR(ret), K(tablet_id), K(table_id));
              }
            }
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  LOG_INFO("read orphan data to pairs", K(ret), K(tenant_id), K(ls->get_ls_id()), K(start_table_id), K(table_id),
           "pairs_count", pairs.count());
  return ret;
}

int ObLobCheckTask::parse_table_and_tablet(const uint64_t tenant_id, storage::ObLS *ls, const common::ObString &row_key,
                                           bool is_repair_task, const uint64_t start_table_id,
                                           const uint64_t start_tablet_id, const int64_t max_count,
                                           ObIArray<ObTabletTablePair> &pairs)
{
  int ret = OB_SUCCESS;
  if (is_repair_task) {
    if (OB_FAIL(parse_repair_table_and_tablet(tenant_id, ls, row_key, start_table_id, start_tablet_id, max_count,
                                               pairs))) {
      LOG_WARN("fail to parse repair table and tablet", K(ret));
    }
  } else if (OB_FAIL(parse_check_table_and_tablet(tenant_id, ls, row_key, start_table_id, start_tablet_id, max_count,
                                                  pairs))) {
    LOG_WARN("fail to parse check table and tablet", K(ret));
  }
  return ret;
}

int ObLobCheckTask::next_tablet_task()
{
  int ret = OB_SUCCESS;
  reuse_iterators();
  allocator_.reuse();
  if (!is_repair_task_) {
    info_.scan_index_ = ObTTLTaskConstant::TTL_SCAN_INDEX_DEFAULT_VALUE;
  }
  info_.row_key_.reset();
  info_.err_code_ = OB_SUCCESS;
  // 检查是否需要重新缓存 tablet
  if (scan_tablet_idx_ >= tablet_table_pairs_.count()) {
    tablet_table_pairs_.reuse();
    if (!tablet_ttl_scheduler_->get_row_key().empty()) {
      if (OB_FAIL(parse_table_and_tablet(tenant_id_, ls_, tablet_ttl_scheduler_->get_row_key(), is_repair_task_,
                                         table_id_, tablet_id_.id(), TABLE_AND_TABLET_BATCH_SIZE,
                                         tablet_table_pairs_))) {
        LOG_WARN("fail to parse row key", K(ret), K(tablet_ttl_scheduler_->get_row_key()));
      }
    } else if (is_repair_task_) {
      if (OB_FAIL(read_orphan_data_to_pairs(tenant_id_, ls_, table_id_, tablet_table_pairs_))) {
        LOG_WARN("fail to read orphan data to pairs", KR(ret), K(tenant_id_), K(table_id_));
      }
    } else if (OB_FAIL(cache_table_and_tablet_pairs(tenant_id_, ls_, table_id_, tablet_id_.id(),
                                                    TABLE_AND_TABLET_BATCH_SIZE, tablet_table_pairs_))) {
      LOG_WARN("fail to cache table and tablet", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (tablet_table_pairs_.count() == 0) {
      info_.err_code_ = OB_ITER_END;
      LOG_INFO("all tablets scanned", K(tenant_id_));
    } else {
      scan_tablet_idx_ = 0;
    }
  }

  // 检查是否还有 tablet 需要扫描
  if (OB_SUCC(ret) && info_.err_code_ == OB_SUCCESS) {
    ObTabletTablePair &tablet_table_pair = tablet_table_pairs_.at(scan_tablet_idx_);
    info_.table_id_ = tablet_table_pair.get_table_id();
    info_.tablet_id_ = tablet_table_pair.get_tablet_id();
    table_id_ = info_.table_id_;
    tablet_id_ = info_.tablet_id_;
    scan_tablet_idx_++;
    if (OB_FAIL(find_meta_table_id())) {
      LOG_WARN("fail to find meta table id", K(ret));
    }
  }
  LOG_INFO("next tablet task", K(ret), K(info_), K(tablet_table_pairs_.count()), K(scan_tablet_idx_));
  return ret;
}

// TODO 扫描辅助表的时候，可以使用is_get模式，构造single_rowkey，然后把长度的校验放在辅助表的校验里，从而提高性能
int ObLobCheckTask::scan_main_table()
{
  int ret = OB_SUCCESS;
  int64_t scanned_rows = 0;                            // 当前批次已扫描的行数
  blocksstable::ObDatumRow *last_datum_row = nullptr;  // 保存最后一行用于记录 rowkey
  blocksstable::ObDatumRow *datum_row = nullptr;
  transaction::ObTxReadSnapshot snapshot;
  ObTableScanParam main_scan_param;
  // 开启事务
  transaction::ObTxDesc *tx_desc = nullptr;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  const int64_t timeout_ts = ObTimeUtility::current_time() + LOB_CHECK_TIMEOUT_US;
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get ob access service, get nullptr", K(ret));
  } else if (txs->acquire_tx(tx_desc)) {
    LOG_WARN("fail to acquire tx", K(ret));
  } else if (OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc, get nullptr", K(ret));
  } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_ts,
                                               snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret));
  } else if (OB_FAIL(init_iterators(snapshot, main_scan_param))) {
    LOG_WARN("fail to init iter", K(ret));
  }
  // 扫描主表数据，每次最多扫描 BATCH_SIZE 行
  while (OB_SUCC(ret) && scanned_rows < BATCH_SIZE) {
    datum_row = nullptr;
    if (OB_FAIL(main_table_iter_->get_next_row(datum_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret), K(scanned_rows));
      } else {
        break;
      }
    } else if (OB_ISNULL(datum_row)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("datum row is null", K(ret));
    } else {
      scanned_rows++;
      last_datum_row = datum_row;  // 记录最后一行

      // 遍历该行的所有 LOB 列
      for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs_.count(); i++) {
        const blocksstable::ObStorageDatum &datum = datum_row->storage_datums_[lob_column_idxs_.at(i)];
        if (datum.is_null()) {
          continue;
        }

        // 获取 LOB 数据
        ObString lob_data = datum.get_string();
        if (lob_data.length() < sizeof(ObLobCommon)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid lob data length, should not happen since we only query lob columns",
                   K(ret), K(i), K(lob_data.length()));
        } else {
          ObLobCommon *lob_common = reinterpret_cast<ObLobCommon *>(lob_data.ptr());
          if (!lob_common->is_valid()) {
            LOG_WARN("invalid lob common, should not happen since we only query lob columns, maybe occured ddl",
                     K(ret), K(i), KPC(lob_common));
          } else if (lob_common->in_row_) {
          } else {
            ObLobData *lob_data_ptr = reinterpret_cast<ObLobData *>(lob_common->buffer_);
            uint64_t expected_byte_len = lob_common->get_byte_size(lob_data.length());
            common::ObNewRange range;
            if (OB_FAIL(build_range_from_lob_id(lob_data_ptr->id_, allocator_, range))) {
              LOG_WARN("fail to build range from lob_id", K(ret), K(lob_data_ptr->id_));
            } else if (OB_FAIL(aux_table_iter_->rescan(range))) {
              LOG_WARN("fail to rescan aux table iter", K(ret), K(range));
            } else {
              // 读取辅助表的所有行，累加 byte_len
              uint64_t total_aux_byte_len = 0;
              bool found = false;
              ObLobMetaInfo meta_info;
              while (OB_SUCC(ret)) {
                if (OB_FAIL(aux_table_iter_->get_next_row(meta_info))) {
                  if (OB_ITER_END != ret) {
                    LOG_WARN("fail to get next meta row", K(ret), K(range));
                  } else {
                    ret = OB_SUCCESS;
                    break;
                  }
                } else {
                  found = true;
                  total_aux_byte_len += meta_info.byte_len_;
                }
              }

              // 检查辅助表查询结果
              if (OB_FAIL(ret)) {
                // 辅助表查询失败，已记录日志，直接退出
              } else if (!found) {
                // 辅助表中没有找到该 lob_id
                LOG_WARN("lob_id not found in aux table", K(range), K(expected_byte_len));
                info_.ttl_del_cnt_++;
              } else if (total_aux_byte_len != expected_byte_len) {
                // 长度不一致
                LOG_WARN("lob byte_len mismatch", K(range), K(expected_byte_len), K(total_aux_byte_len));
                info_.max_version_del_cnt_++;
              }
            }
          }
        }
      }
    }
  }
  // 扫描完成后，保存最后一行的 rowkey 用于断点续传
  if ((OB_SUCC(ret)) && OB_NOT_NULL(last_datum_row) && scanned_rows > 0) {
    if (OB_FAIL(save_current_rowkey(true, main_scan_param, *last_datum_row))) {
      LOG_WARN("fail to save current rowkey", K(ret), K(scanned_rows));
    }
  }
  if (OB_NOT_NULL(tx_desc) && OB_NOT_NULL(txs)) {
    txs->release_tx(*tx_desc);
  }
  reuse_iterators();
  return ret;
}

int ObLobCheckTask::scan_auxiliary_table()
{
  int ret = OB_SUCCESS;
  int64_t scanned_rows = 0;  // 当前批次已扫描的辅助表行数
  lob_id_map_.reuse();
  meta_seq_info_array_.reuse();

  // 2. 扫描辅助表，每次最多收集 BATCH_SIZE 个不同的 lob_id 到 HashMap
  uint64_t last_lob_id = OB_INVALID_ID;  // 上一次的 lob_id（辅助表按 lob_id 排序，相同 lob_id 连续
  transaction::ObTxReadSnapshot snapshot;
  ObTableScanParam main_scan_param;
  // 开启事务
  transaction::ObTxDesc *tx_desc = nullptr;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  const int64_t timeout_ts = ObTimeUtility::current_time() + LOB_CHECK_TIMEOUT_US;
  if (OB_FAIL(ObInsertLobColumnHelper::start_trans(ls_id_, false /*is_for_read*/, timeout_ts, tx_desc))) {
    LOG_WARN("fail to get tx_desc", K(ret));
  } else if (OB_ISNULL(tx_desc) || OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
  } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_ts,
                                               snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret));
  } else if (OB_FAIL(init_iterators(snapshot, main_scan_param))) {
    LOG_WARN("fail to init iter", K(ret));
  }
  while (OB_SUCC(ret) && scanned_rows < BATCH_SIZE + 1) {
    ObLobMetaInfo meta_info;
    if (OB_FAIL(aux_table_iter_->get_next_row(meta_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row from auxiliary table", K(ret));
      }
    } else {
      // 检查是否是新的 lob_id（与上一次不同）
      if (meta_info.lob_id_.lob_id_ != last_lob_id) {
        if (scanned_rows == BATCH_SIZE) {
          break;
        } else if (OB_FAIL(meta_seq_info_array_.push_back(ObLobMetaSeqInfo(meta_info.lob_id_)))) {
          LOG_WARN("fail to push back meta seq info", K(ret), K(last_lob_id), K(meta_info.seq_id_));
        } else if (OB_FAIL(meta_seq_info_array_.at(meta_seq_info_array_.count() - 1)
                               .add_seq_id(meta_info.seq_id_, allocator_))) {
          LOG_WARN("fail to add seq id", K(ret), K(meta_info.seq_id_));
        } else if (OB_FAIL(lob_id_map_.set_refactored(meta_info.lob_id_.lob_id_, false))) {
          LOG_WARN("fail to set lob_id to map", K(ret), K(meta_info.lob_id_));
        } else {
          scanned_rows++;              // 统计扫描的行数
          last_lob_id = meta_info.lob_id_.lob_id_;
        }
      } else if (OB_FAIL(meta_seq_info_array_.at(meta_seq_info_array_.count() - 1)
                             .add_seq_id(meta_info.seq_id_, allocator_))) {
        LOG_WARN("fail to add seq id", K(ret), K(meta_info.seq_id_));
      }
    }
  }
  if (OB_ITER_END == ret) {
    need_next_tablet_ = true;
    ret = OB_SUCCESS;
  }

  // 3. 扫描主表，标记在 HashMap 中找到的 lob_id
  if (OB_SUCC(ret) && lob_id_map_.size() > 0) {
    int64_t found_count = 0;  // 已找到的 lob_id 数量
    const int64_t total_count = lob_id_map_.size();
    blocksstable::ObDatumRow *datum_row = nullptr;
    int16_t begin_idx = data_table_rowkey_idxs_.count();
    while (OB_SUCC(ret) && found_count < total_count) {
      if (OB_FAIL(main_table_iter_->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row from main table", K(ret));
        }
      } else if (OB_ISNULL(datum_row)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("datum row is null", K(ret));
      } else {
        // 遍历该行的所有 LOB 列（此时 datum_row 只包含 LOB 列, oracle模式下含有主键）
        for (int64_t i = begin_idx; OB_SUCC(ret) && i < datum_row->count_; i++) {
          const blocksstable::ObStorageDatum &datum = datum_row->storage_datums_[i];
          const ObLobCommon *lob_common = reinterpret_cast<const ObLobCommon *>(datum.ptr_);
          if (datum.is_null()) {
          } else if (OB_ISNULL(lob_common)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("lob common is null", K(ret), K(i), K(datum_row->count_));
          } else if (lob_common->in_row_) {
          } else if (lob_common->is_init_) {
            const ObLobData *lob_data = reinterpret_cast<const ObLobData *>(lob_common->buffer_);
            if (OB_NOT_NULL(lob_data)) {
              bool found = false;
              // 检查该 lob_id（真实的 lob_id）是否在 HashMap 中
              int tmp_ret = lob_id_map_.get_refactored(lob_data->id_.lob_id_, found);
              if (OB_SUCCESS == tmp_ret && !found) {
                // 找到了，标记为 true
                if (OB_FAIL(lob_id_map_.set_refactored(lob_data->id_.lob_id_, true, 1 /*overwrite*/))) {
                  LOG_WARN("fail to update lob_id in map", K(ret), K(lob_data->id_));
                } else {
                  found_count++;
                }
              }
            }
          }
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }

    // 保存辅助表的 rowkey 用于断点续传（如果扫描了数据）
    if (OB_SUCC(ret) && scanned_rows > 0) {
      // 在栈上分配 storage_datums
      blocksstable::ObStorageDatum datums[ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT];
      blocksstable::ObDatumRow aux_datum_row;
      aux_datum_row.count_ = ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT;
      aux_datum_row.storage_datums_ = datums;
      int64_t orphan_count = total_count - found_count;
      info_.scan_cnt_ += orphan_count;
      ObLobMetaInfo last_meta_info;
      last_meta_info.lob_id_.tablet_id_ = aux_table_iter_->get_scan_param().tablet_id_.id();
      last_meta_info.lob_id_.lob_id_ = last_lob_id;
      if (orphan_count > 0 && is_repair_task_ && OB_FAIL(delete_orphan_auxiliary_data(tx_desc, snapshot))) {
        LOG_WARN("fail to delete orphan auxiliary data", K(ret), K(info_));
      } else if (OB_FAIL(ObLobMetaUtil::build_rowkey_from_info(last_meta_info, &aux_datum_row))) {
        LOG_WARN("fail to build rowkey from lob meta info", K(ret), K(last_meta_info));
      } else if (OB_FAIL(save_current_rowkey(false, main_scan_param, aux_datum_row))) {
        LOG_WARN("fail to save current rowkey", K(ret));
      }
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(ObInsertLobColumnHelper::end_trans(tx_desc, false /* not rollback */, timeout_ts))) {
    ret = tmp_ret;
    LOG_WARN("fail to end trans", K(ret));
  }
  reuse_iterators();
  return ret;
}

int ObLobCheckTask::delete_orphan_auxiliary_data(transaction::ObTxDesc *tx_desc,
                                                 transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t need_seq_cnt = 0;
  ObLobBatchMetaDelIter batch_meta_del_iter;
  ObDatumRow del_row;
  ObDMLBaseParam dml_param;
  if (OB_FAIL(batch_meta_del_iter.init(dml_param))) {
    LOG_WARN("fail to init delete row iterator", K(ret));
  } else if (OB_FAIL(del_row.init(allocator_, ObLobMetaUtil::LOB_META_COLUMN_CNT))) {
    LOG_WARN("fail to init row", K(ret));
  }
  // 1. 收集所有孤立的 lob_id（主表不存在的）
  for (common::hash::ObHashMap<uint64_t, bool>::const_iterator iter = lob_id_map_.begin();
       OB_SUCC(ret) && iter != lob_id_map_.end(); ++iter) {
    for (int64_t i = 0; OB_SUCC(ret) && !iter->second && i < meta_seq_info_array_.count(); i++) {
      if (meta_seq_info_array_.at(i).lob_id_.lob_id_ == iter->first) {
        need_seq_cnt += meta_seq_info_array_.at(i).seq_ids_.count();
        for (int64_t j = 0; OB_SUCC(ret) && j < meta_seq_info_array_.at(i).seq_ids_.count(); j++) {
          del_row.storage_datums_[0].set_string(reinterpret_cast<const char *>(&meta_seq_info_array_.at(i).lob_id_),
                                                sizeof(ObLobId));
          del_row.storage_datums_[1].set_string(meta_seq_info_array_.at(i).seq_ids_.at(j));
          del_row.storage_datums_[2].set_nop();
          del_row.storage_datums_[3].set_nop();
          del_row.storage_datums_[4].set_nop();
          del_row.storage_datums_[5].set_nop();
          if (OB_FAIL(batch_meta_del_iter.add_row(del_row))) {
            LOG_WARN("fail to add seq id", K(ret), K(meta_seq_info_array_.at(i).seq_ids_.at(j)));
          }
        }
        LOG_INFO("delete orphan auxiliary data", K(ret), K(tenant_id_), K(table_id_), K(tablet_id_), K(meta_seq_info_array_.at(i).lob_id_));
        break;
      }
    }
  }
  if (OB_FAIL(ret) || need_seq_cnt == 0) {
  } else {
    ObArenaAllocator del_allocator;

    ObAccessService *oas = MTL(ObAccessService *);
    const ObTableSchema *meta_table_schema = nullptr;
    storage::ObStoreCtxGuard store_ctx_guard;
    int64_t affected_rows = 0;

    const ObIArray<uint64_t> &dml_column_ids = aux_table_iter_->get_scan_param().column_ids_;
    const int64_t timeout_ts = ObTimeUtility::current_time() + LOB_CHECK_TIMEOUT_US;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.write_flag_.reset();
    dml_param.check_schema_version_ = false;  // lob tablet should not check schema version
    dml_param.write_flag_.set_is_insert_up();
    dml_param.tz_info_ = NULL;
    // dml_param.data_row_for_lob_ = param.data_row_;
    dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
    dml_param.timeout_ = timeout_ts;
    dml_param.snapshot_.assign(snapshot);
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    dml_param.schema_version_ = 0;
    dml_param.dml_allocator_ = &del_allocator;
    // 每删除一行需要一个序列号，预估需要 orphan_lob_ids 数量的序列号
    transaction::ObTxSEQ seq_no_st;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(MTL(ObLobManager *)->get_table_dml_param(dml_param.table_param_))) {
      LOG_WARN("failed to get table param", K(ret));
    } else if (OB_FAIL(oas->get_write_store_ctx_guard(ls_id_, timeout_ts, *tx_desc, snapshot, 0, dml_param.write_flag_,
                                                      store_ctx_guard))) {
      LOG_WARN("failed to get write store context guard", K(ret));
    } else if (OB_FAIL(tx_desc->get_and_inc_tx_seq(0, need_seq_cnt, seq_no_st))) {
      LOG_WARN("failed to get and inc tx seq", K(ret));
    } else {
      dml_param.spec_seq_no_ = seq_no_st;
      batch_meta_del_iter.set_start_seq_no_st(seq_no_st);
      if (OB_FAIL(oas->delete_rows(ls_id_, aux_table_iter_->get_scan_param().tablet_id_, *tx_desc, dml_param,
                                   dml_column_ids, &batch_meta_del_iter, affected_rows))) {
        LOG_WARN("failed to delete rows from snapshot table", K(ret), K(snapshot),
                 K(aux_table_iter_->get_scan_param().tablet_id_));
      } else if (need_seq_cnt != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete rows from snapshot table count mismatch", K(ret), K(need_seq_cnt), K(affected_rows),
                 K(meta_seq_info_array_.count()), K(lob_id_map_.size()));
      } else {
        store_ctx_guard.reset();
      }
    }
  }
  return ret;
}

int ObLobCheckTask::build_range_from_lob_id(ObLobId &lob_id, ObArenaAllocator &allocator, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  ObObj *key_objs = nullptr;
  if (OB_ISNULL(key_objs = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 4)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for key objs", K(ret));
  } else {
    key_objs[0].set_varchar(reinterpret_cast<const char *>(&lob_id), sizeof(ObLobId));
    key_objs[0].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
    key_objs[1] = ObObj::make_min_obj();
    ObRowkey min_rowkey(key_objs, 2);
    key_objs[2].set_varchar(reinterpret_cast<const char *>(&lob_id), sizeof(ObLobId));
    key_objs[2].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
    key_objs[3] = ObObj::make_max_obj();
    ObRowkey max_rowkey(key_objs + 2, 2);
    range.start_key_ = min_rowkey;
    range.end_key_ = max_rowkey;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObLobCheckTask::save_current_rowkey(bool is_main_table, ObTableScanParam &scan_param,
                                        const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = is_main_table ? data_table_rowkey_idxs_.count() : 2;
  allocator_.reuse();
  info_.row_key_.reset();
  if (rowkey_cnt > datum_row.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey count mismatch", K(ret), K(rowkey_cnt), K(datum_row.count_));
  } else {
    ObStorageDatum datums_[rowkey_cnt];
    blocksstable::ObDatumRowkey rowkey;
    if (is_main_table) {
      if (data_table_rowkey_idxs_.at(rowkey_cnt - 1) >= datum_row.count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data table rowkey index out of range", K(ret), K(data_table_rowkey_idxs_.at(rowkey_cnt - 1)),
                 K(datum_row.count_));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < data_table_rowkey_idxs_.count(); i++) {
        if (OB_FAIL(datums_[i].deep_copy(datum_row.storage_datums_[data_table_rowkey_idxs_.at(i)], allocator_))) {
          LOG_WARN("deep copy failed", K(ret), K(i), K(data_table_rowkey_idxs_.at(i)));
        }
      }
      if (OB_SUCC(ret)) {
        rowkey.assign(datums_, rowkey_cnt);
      }
    } else {
      rowkey.assign(datum_row.storage_datums_, 1);
    }
    int64_t pos = 0;
    int buf_len = rowkey.get_serialize_size();
    char *rowkey_buf = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(rowkey_buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for rowkey buffer", K(ret), K(buf_len));
    } else if (OB_FAIL(rowkey.serialize(rowkey_buf, buf_len, pos))) {
      LOG_WARN("fail to serialize rowkey", K(ret), K(rowkey), K(rowkey_cnt));
    } else if (pos != buf_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("serialize rowkey failed", K(ret), K(rowkey), K(rowkey_cnt), K(buf_len), K(pos));
    } else {
      info_.row_key_.assign(rowkey_buf, buf_len);
    }
  }
  LOG_INFO("save current rowkey", K(is_main_table), KPHEX(info_.row_key_.ptr(), info_.row_key_.length()));
  return ret;
}

int ObLobCheckTask::restore_start_key_from_rowkey(bool is_main_table, ObTableScanParam &main_scan_param,
                                                  ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if (info_.row_key_.empty() || (is_main_table && info_.scan_index_.case_compare(LOB_META_SCAN_INDEX) == 0) ||
      (!is_main_table && info_.scan_index_.case_compare(ObTTLTaskConstant::TTL_SCAN_INDEX_DEFAULT_VALUE) == 0)) {
    // 没有保存的 rowkey，使用全表扫描
    range.set_whole_range();
  } else if (is_main_table && OB_ISNULL(main_scan_param.table_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table param is null", K(ret));
  } else {
    // 反序列化 rowkey
    int64_t pos = 0;
    const int64_t restore_rowkey_cnt = is_main_table ? data_table_rowkey_idxs_.count() : 1;
    const int64_t rowkey_cnt = is_main_table ? data_table_rowkey_idxs_.count() : ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT;
    // 在栈上分配 datum 数组（临时用于反序列化）
    blocksstable::ObStorageDatum temp_datums[restore_rowkey_cnt];
    blocksstable::ObStorageDatum copy_datums[restore_rowkey_cnt];
    // 反序列化 ObDatumRowkey
    blocksstable::ObDatumRowkey datum_rowkey;
    datum_rowkey.assign(temp_datums, restore_rowkey_cnt);

    if (OB_FAIL(datum_rowkey.deserialize(info_.row_key_.ptr(), info_.row_key_.length(), pos))) {
      LOG_WARN("fail to deserialize rowkey", K(ret), K(info_.row_key_));
    } else if (pos != info_.row_key_.length()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deserialize rowkey size mismatch", K(ret), K(pos), K(info_.row_key_.length()));
    } else {
      // 分配 ObObj 数组用于构造 ObRowkey
      ObObj *objs = nullptr;
      if (OB_ISNULL(objs = static_cast<ObObj *>(allocator_.alloc(sizeof(ObObj) * rowkey_cnt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for obj array", K(ret), K(rowkey_cnt));
      } else {
        // 将 ObStorageDatum 转换为 ObObj
        if (is_main_table) {
          for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
            // 主表：从 table_param 获取列描述符
            const ObColumnParam *column_param = main_scan_param.table_param_->get_read_info().get_columns()->at(i);
            if (OB_ISNULL(column_param)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column param is null", K(ret), K(i));
            } else if (OB_FAIL(copy_datums[i].deep_copy(temp_datums[i], allocator_))) {
              LOG_WARN("fail to deep copy datum", K(ret), K(i));
            } else if (OB_FAIL(copy_datums[i].to_obj_enhance(objs[i], column_param->get_meta_type()))) {
              LOG_WARN("fail to convert datum to obj", K(ret), K(i));
            }
          }
        } else {
          // 辅助表：第一个是 lob_id (varchar)，第二个是 max_obj
          objs[1] = ObObj::make_max_obj();
          ObObjMeta obj_meta;
          obj_meta.set_varchar();
          obj_meta.set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
          obj_meta.set_collation_level(common::ObCollationLevel::CS_LEVEL_IMPLICIT);
          if (OB_FAIL(copy_datums[0].deep_copy(temp_datums[0], allocator_))) {
            LOG_WARN("fail to deep copy datum", K(ret));
          } else if (OB_FAIL(copy_datums[0].to_obj_enhance(objs[0], obj_meta))) {
            LOG_WARN("fail to convert datum to obj for aux table", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          // 设置 range：从 rowkey 到正无穷
          range.start_key_.assign(objs, rowkey_cnt);
          range.end_key_.set_max_row();
          range.border_flag_.unset_inclusive_start();
          range.border_flag_.unset_inclusive_end();

          LOG_INFO("restore start key from saved rowkey", K(is_main_table), K(range), KPHEX(info_.row_key_.ptr(), info_.row_key_.length()));
        }
      }
    }
  }

  return ret;
}

int ObLobCheckTask::init_iterators(transaction::ObTxReadSnapshot &snapshot, ObTableScanParam &main_scan_param)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager *);
  ObAccessService *oas = MTL(ObAccessService *);
  ObNewRowIterator *table_iter = nullptr;
  if (OB_NOT_NULL(main_table_iter_) || OB_NOT_NULL(aux_table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("main table iter is already initialized", K(ret));
  } else if (OB_FAIL(init_meta_iter(snapshot))) {
    LOG_WARN("fail to init lob meta iterator", K(ret));
  } else if (OB_FAIL(build_table_scan_param(main_allocator_, snapshot, true, tablet_id_, main_scan_param))) {
    LOG_WARN("fail to build table scan param", K(ret), K(tablet_id_));
  } else if (OB_FAIL(oas->table_scan(main_scan_param, table_iter))) {
    LOG_WARN("fail to table scan main table", K(ret), K(main_scan_param), K(main_table_iter_));
  } else if (OB_ISNULL(table_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table iter is null", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(table_iter)) {
    oas->revert_scan_iter(table_iter);
  } else if (OB_NOT_NULL(table_iter)) {
    main_table_iter_ = static_cast<storage::ObTableScanIterator *>(table_iter);
  }
  return ret;
}

int ObLobCheckTask::init_meta_iter(transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  ObLobManager *lob_mngr = MTL(ObLobManager *);
  ObTabletHandle handle;
  ObTabletBindingMdsUserData ddl_data;

  // 分配 ObLobMetaIterator
  if (OB_NOT_NULL(aux_table_iter_)) {
    LOG_WARN("aux table iter is not null", K(ret));
  } else if (OB_ISNULL(aux_table_iter_ = OB_NEWx(storage::ObLobMetaIterator, (&aux_allocator_), nullptr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc aux table iter", K(ret));
  } else if (OB_FAIL(get_tablet_handle_inner(snapshot, handle))) {
    LOG_WARN("fail to get tablet id", K(ret));
  } else if (OB_FAIL(handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("fail to get ddl data", K(ret), K(handle));
  } else if (OB_UNLIKELY(
                 check_lob_tablet_id(tablet_id_, ddl_data.lob_meta_tablet_id_, ddl_data.lob_piece_tablet_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("tablet id is not valid", K(ret), K(tablet_id_), K(ddl_data));
  } else if (OB_FAIL(build_table_scan_param(aux_allocator_, snapshot, false, ddl_data.lob_meta_tablet_id_,
                                            aux_table_iter_->get_scan_param()))) {
    LOG_WARN("fail to build table scan param", K(ret), K(ddl_data));
  } else if (OB_FAIL(aux_table_iter_->open(tablet_id_, ddl_data.lob_piece_tablet_id_))) {
    LOG_WARN("fail to open aux table iter", K(ret), K(ddl_data));
  }

  // 如果初始化失败，清理已分配的资源
  if (OB_FAIL(ret) && OB_NOT_NULL(aux_table_iter_)) {
    aux_table_iter_->~ObLobMetaIterator();
    aux_allocator_.free(aux_table_iter_);
    aux_table_iter_ = nullptr;
  }

  return ret;
}

bool ObLobCheckTask::check_lob_tablet_id(const ObTabletID &data_tablet_id, const ObTabletID &lob_meta_tablet_id,
                                         const ObTabletID &lob_piece_tablet_id)
{
  bool bret = false;
  if (OB_UNLIKELY(!lob_meta_tablet_id.is_valid() || lob_meta_tablet_id == data_tablet_id)) {
    bret = true;
  } else if (OB_UNLIKELY(!lob_piece_tablet_id.is_valid() || lob_piece_tablet_id == data_tablet_id)) {
    bret = true;
  } else if (OB_UNLIKELY(lob_meta_tablet_id == lob_piece_tablet_id)) {
    bret = true;
  }
  return bret;
}

int ObLobCheckTask::get_tablet_id_by_ls_handle(transaction::ObTxReadSnapshot &snapshot, ObLSHandle &ls_handle,
                                               ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_->get_tablet_with_timeout(tablet_id_, handle, 0, ObMDSGetTabletMode::READ_READABLE_COMMITED,
                                           snapshot.version()))) {
    if (OB_TABLET_IS_SPLIT_SRC == ret) {
      if (OB_FAIL(ls_->get_tablet_with_timeout(tablet_id_, handle, 0, ObMDSGetTabletMode::READ_ALL_COMMITED,
                                               snapshot.version()))) {
        LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
      }
    } else {
      LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
    }
  }
  return ret;
}

int ObLobCheckTask::get_tablet_handle_inner(transaction::ObTxReadSnapshot &snapshot, ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletBindingMdsUserData ddl_data;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (ls_handle.get_ls()->is_offline()) {
    ret = OB_LS_OFFLINE;
    LOG_WARN("log stream is offline", K(ret), K(ls_id_));
  } else if (OB_FAIL(get_tablet_id_by_ls_handle(snapshot, ls_handle, handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
  }
  return ret;
}

int ObLobCheckTask::add_table_param(common::ObArenaAllocator &allocator, ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  // 释放之前的 table_param（如果存在）
  if (OB_NOT_NULL(scan_param.table_param_)) {
    scan_param.table_param_->~ObTableParam();
    scan_param.table_param_ = nullptr;
  }

  // 清空之前的 column_ids_
  scan_param.column_ids_.reuse();
  lob_column_idxs_.reuse();
  data_table_rowkey_idxs_.reuse();
  // 主表：从 schema 获取列，并构造 table_param
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id_, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id_), K(table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", K(ret), K(tenant_id_), K(table_id_));
  }
  LOG_INFO("add_table_param: table_schema", K(table_schema->get_table_name()));

  if (OB_SUCC(ret) && OB_NOT_NULL(table_schema)) {
    // 获取主表的所有 LOB 列
    int64_t column_count = table_schema->get_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
      const share::schema::ObColumnSchemaV2 *column = table_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", K(ret), K(i));
      } else if (info_.scan_index_.case_compare(ObTTLTaskConstant::TTL_SCAN_INDEX_DEFAULT_VALUE) == 0) {
        if (OB_FAIL(scan_param.column_ids_.push_back(column->get_column_id()))) {
          LOG_WARN("push col id failed", K(ret), K(i), K(column->get_column_id()));
        } else if (column->is_rowkey_column() && OB_FAIL(data_table_rowkey_idxs_.push_back(i))) {
          LOG_WARN("push data table rowkey col id failed", K(ret), K(i), K(column->get_column_id()));
        } else if ((is_lob_storage(column->get_data_type()) || column->is_string_lob()) &&
                      column->is_column_stored_in_sstable() && OB_FAIL(lob_column_idxs_.push_back(i))) {
          LOG_WARN("push lob col id failed", K(ret), K(i), K(column->get_column_id()));
        }
      } else if (((is_lob_storage(column->get_data_type()) || column->is_string_lob()) &&
                   column->is_column_stored_in_sstable()) || (column->is_rowkey_column() && lib::is_oracle_mode())) {
        if (OB_FAIL(scan_param.column_ids_.push_back(column->get_column_id()))) {
          LOG_WARN("push col id failed", K(ret), K(i), K(column->get_column_id()));
        } else if (column->is_rowkey_column() && lib::is_oracle_mode()) {
          if (OB_FAIL(data_table_rowkey_idxs_.push_back(i))) {
            LOG_WARN("push data table rowkey col id failed", K(ret), K(i), K(column->get_column_id()));
          }
        }
      }
    }

    // 为主表创建 table_param
    if (OB_SUCC(ret)) {
      share::schema::ObTableParam *table_param = nullptr;
      if (OB_ISNULL(table_param = OB_NEWx(share::schema::ObTableParam, (&allocator), allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for table param", K(ret), K(allocator));
      } else {
        table_param->get_enable_lob_locator_v2() = true;
        if (OB_FAIL(table_param->convert(*table_schema, scan_param.column_ids_, 0))) {
          LOG_WARN("fail to convert table param", K(ret), "table_id", table_schema->get_table_id());
          // 失败时清理已分配的内存
          table_param->~ObTableParam();
          allocator.free(table_param);
          table_param = nullptr;
        } else {
          scan_param.table_param_ = table_param;
        }
      }
    }
  }
  return ret;
}

int ObLobCheckTask::build_table_scan_param(common::ObArenaAllocator &allocator,
                                           const transaction::ObTxReadSnapshot &snapshot, bool is_main_table,
                                           const ObTabletID &tablet_id, ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle handle;
  ObNewRange range;
  scan_param.key_ranges_.reset();
  ObQueryFlag query_flag(ObQueryFlag::Forward,    // scan_order
                         false,                   // daily_merge
                         false,                   // optimize
                         false,                   // sys scan
                         true,                    // full_row
                         false,                   // index_back
                         false,                   // query_stat
                         ObQueryFlag::MysqlMode,  // sql_mode
                         false                    // read_latest
  );
  query_flag.disable_cache();
  if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls_->get_tablet_with_timeout(tablet_id, handle, 0, ObMDSGetTabletMode::READ_READABLE_COMMITED,
                                                  snapshot.version()))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id), "tx_snapshot_version", snapshot.version());
  } else {
    scan_param.schema_version_ = handle.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
    scan_param.column_ids_.reuse();

    // 根据是主表还是辅助表，设置不同的 column_ids 和 table_param
    if (is_main_table) {
      query_flag.skip_read_lob_ = true;
      if (OB_FAIL(add_table_param(allocator, scan_param))) {
        LOG_WARN("fail to add table param", K(ret), K(tablet_id));
      }
    } else {
      int column_cnt = is_repair_task_ ? ObLobMetaUtil::LOB_META_COLUMN_CNT : ObLobMetaUtil::LOB_META_COLUMN_CNT - 1;
      for (uint32_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
        if (OB_FAIL(scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + i))) {
          LOG_WARN("push col id failed", K(ret), K(i));
        }
      }
    }

    // 构建扫描范围：如果是主表且有保存的 rowkey，则从 rowkey 恢复起始位置；否则全表扫描
    if (OB_SUCC(ret)) {
      range.table_id_ = is_main_table ? table_id_ : meta_table_id_;
      if (OB_FAIL(restore_start_key_from_rowkey(is_main_table, scan_param, range))) {
        LOG_WARN("fail to restore start key from rowkey", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
      LOG_WARN("fail to push back key range", K(ret));
    } else {
      scan_param.scan_flag_.flag_ = query_flag.flag_;
      scan_param.scan_flag_.scan_order_ = ObQueryFlag::Forward;
      scan_param.tenant_id_ = tenant_id_;
      scan_param.allocator_ = &allocator;
      scan_param.ls_id_ = ls_id_;
      scan_param.scan_allocator_ = &allocator;
      scan_param.tablet_id_ = tablet_id;
      scan_param.limit_param_.limit_ = -1;
      scan_param.limit_param_.offset_ = 0;
      scan_param.index_id_ = 0;
      scan_param.is_get_ = false;  // 全表扫描设置为 false
      scan_param.timeout_ = ObTimeUtility::current_time() + LOB_CHECK_TIMEOUT_US;
      scan_param.for_update_ = is_repair_task_ ? true : false;
      scan_param.for_update_wait_timeout_ = scan_param.timeout_;
      scan_param.sql_mode_ = SMO_DEFAULT;
      scan_param.frozen_version_ = -1;
      scan_param.force_refresh_lc_ = false;
      scan_param.output_exprs_ = nullptr;
      scan_param.aggregate_exprs_ = nullptr;
      scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
      scan_param.op_ = nullptr;
      scan_param.row2exprs_projector_ = nullptr;
      scan_param.need_scn_ = false;
      scan_param.pd_storage_flag_ = false;
      // 设置快照
      if (OB_FAIL(scan_param.snapshot_.assign(snapshot))) {
        LOG_WARN("fail to assign snapshot", K(ret));
      }

      // Debug log
      LOG_INFO("build_table_scan_param completed", K(ret), K(is_main_table), K(tablet_id), K(range), "column_ids_count",
               scan_param.column_ids_.count(), "has_table_param", (scan_param.table_param_ != nullptr),
               K(scan_param.snapshot_.version()));
    }
  }

  return ret;
}

}  // namespace share
}  // namespace oceanbase
