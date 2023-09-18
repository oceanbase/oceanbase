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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_utils.h"
#include "sql/das/ob_das_context.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_srs.h"
#include "lib/geo/ob_s2adapter.h"
#include "lib/geo/ob_geo_utils.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
void ObDASUtils::log_user_error_and_warn(const obrpc::ObRpcResultCode &rcode)
{
  if (OB_UNLIKELY(OB_SUCCESS != rcode.rcode_)) {
    FORWARD_USER_ERROR(rcode.rcode_, rcode.msg_);
  }
  for (int i = 0; i < rcode.warnings_.count(); ++i) {
    const common::ObWarningBuffer::WarningItem &warning_item = rcode.warnings_.at(i);
    if (ObLogger::USER_WARN == warning_item.log_level_) {
      FORWARD_USER_WARN(warning_item.code_, warning_item.msg_);
    } else if (ObLogger::USER_NOTE == warning_item.log_level_) {
      FORWARD_USER_NOTE(warning_item.code_, warning_item.msg_);
    }
  }
}

int ObDASUtils::store_warning_msg(const ObWarningBuffer &wb, obrpc::ObRpcResultCode &rcode)
{
  int ret = OB_SUCCESS;
  bool not_null = true;
  for (uint32_t idx = 0; OB_SUCC(ret) && not_null && idx < wb.get_readable_warning_count(); idx++) {
    const ObWarningBuffer::WarningItem *item = wb.get_warning_item(idx);
    if (item != NULL) {
      if (OB_FAIL(rcode.warnings_.push_back(*item))) {
        RPC_OBRPC_LOG(WARN, "Failed to add warning", K(ret));
      }
    } else {
      not_null = false;
    }
  }
  return ret;
}

int ObDASUtils::check_nested_sql_mutating(ObTableID ref_table_id, ObExecContext &exec_ctx, bool is_reading)
{
  int ret = OB_SUCCESS;
  ObExecContext *cur_parent_ctx = exec_ctx.get_parent_ctx();
  //parent_das_ctx.is_fk_cascading_ = true means that
  //the direct parent node of this sql is a foreign key,
  //indicating that this SQL is triggered by a foreign key,
  //and no mutating check is required.
  //pl_stack != nullptr means this sql is triggered by trigger or pl udf
  //only this sql(trigger or pl udf) needs to check mutating,
  //and the statement in autonomous transaction does not need to be checked
  if (OB_ISNULL(cur_parent_ctx)) {
    //do nothing
  } else if (cur_parent_ctx->get_das_ctx().is_fk_cascading_) {
    cur_parent_ctx = nullptr;
  } else if (OB_ISNULL(cur_parent_ctx->get_pl_stack_ctx())) {
    cur_parent_ctx = nullptr;
  } else if (cur_parent_ctx->get_pl_stack_ctx()->in_autonomous()) {
    cur_parent_ctx = nullptr;
  }
  while (OB_SUCC(ret) && cur_parent_ctx != nullptr) {
    ObDASCtx &parent_das_ctx = cur_parent_ctx->get_das_ctx();
    LOG_DEBUG("check nested sql mutating", K(cur_parent_ctx), K(parent_das_ctx), K(ref_table_id));
    FOREACH_X(node, parent_das_ctx.get_table_loc_list(), OB_SUCC(ret)) {
      ObDASTableLoc *table_loc = *node;
      if (table_loc->loc_meta_->ref_table_id_ == ref_table_id
          && (table_loc->is_writing_ || (!is_reading && lib::is_mysql_mode()))
          && !table_loc->is_fk_check_ ) {
        ObSchemaGetterGuard schema_guard;
        const ObTableSchema *table_schema = NULL;
        uint64_t tenant_id = exec_ctx.get_my_session()->get_effective_tenant_id();
        if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
          LOG_WARN("get tenant schema guard failed", K(ret));
        } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, ref_table_id, table_schema))) {
          LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(ref_table_id));
        } else if (table_schema != nullptr && lib::is_mysql_mode()) {
          LOG_MYSQL_USER_ERROR(OB_ERR_MUTATING_TABLE_OPERATION, table_schema->get_table_name());
        }
        ret = OB_ERR_MUTATING_TABLE_OPERATION;
        LOG_WARN("table is mutating", K(ret), K(ref_table_id));
      }
    }
    if (OB_SUCC(ret)) {
      cur_parent_ctx = cur_parent_ctx->get_parent_ctx();
      if (OB_ISNULL(cur_parent_ctx)) {
        //do nothing
      } else if (cur_parent_ctx->get_das_ctx().is_fk_cascading_) {
        cur_parent_ctx = nullptr;
      } else if (OB_ISNULL(cur_parent_ctx->get_pl_stack_ctx())) {
        cur_parent_ctx = nullptr;
      } else if (cur_parent_ctx->get_pl_stack_ctx()->in_autonomous()) {
        cur_parent_ctx = nullptr;
      }
    }
  }
  return ret;
}

ObDASTabletLoc *ObDASUtils::get_related_tablet_loc(const ObDASTabletLoc &tablet_loc,
                                                   ObTableID related_table_id)
{
  ObDASTabletLoc *ret_tablet_loc = nullptr;
  if (related_table_id == tablet_loc.loc_meta_->ref_table_id_) {
    ret_tablet_loc = const_cast<ObDASTabletLoc*>(&tablet_loc);
  } else {
    for (ObDASTabletLoc *cur_node = tablet_loc.next_;
        cur_node != nullptr && cur_node != &tablet_loc;
        cur_node = cur_node->next_) {
      if (cur_node->loc_meta_->ref_table_id_ == related_table_id) {
        ret_tablet_loc = cur_node;
        break;
      }
    }
  }
  return ret_tablet_loc;
}

int ObDASUtils::build_table_loc_meta(ObIAllocator &allocator,
                                     const ObDASTableLocMeta &src,
                                     ObDASTableLocMeta *&dst)
{
  int ret = OB_SUCCESS;
  dst = nullptr;
  void *buf = allocator.alloc(sizeof(ObDASTableLocMeta));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate table loc meta failed", K(ret), K(sizeof(ObDASTableLocMeta)));
  } else {
    dst = new(buf) ObDASTableLocMeta(allocator);
    if (OB_FAIL(dst->assign(src))) {
      LOG_WARN("assign table loc meta failed", K(ret));
    }
  }
  return ret;
}

int ObDASUtils::serialize_das_ctdefs(char *buf, int64_t buf_len, int64_t &pos,
                                     const DASDMLCtDefArray &ctdefs)
{
  int ret = common::OB_SUCCESS;
  OB_UNIS_ENCODE(ctdefs.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < ctdefs.count(); ++i) {
    OB_UNIS_ENCODE(*ctdefs.at(i));
  }
  return ret;
}

int64_t ObDASUtils::das_ctdefs_serialize_size(const DASDMLCtDefArray &ctdefs)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(ctdefs.count());
  for (int64_t i = 0; i < ctdefs.count(); ++i) {
    OB_UNIS_ADD_LEN(*ctdefs.at(i));
  }
  return len;
}

int ObDASUtils::deserialize_das_ctdefs(const char *buf, const int64_t data_len, int64_t &pos,
                                       common::ObIAllocator &allocator,
                                       ObDASOpType op_type,
                                       DASDMLCtDefArray &ctdefs)
{
  int ret = common::OB_SUCCESS;
  int64_t array_size = 0;
  OB_UNIS_DECODE(array_size);
  ctdefs.set_capacity(array_size);
  for (int64_t i = 0; OB_SUCC(ret) && i < array_size; ++i) {
    ObDASDMLBaseCtDef *ctdef = nullptr;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(op_type, allocator, ctdef))) {
      SQL_DAS_LOG(WARN, "allocate das ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*ctdef);
    OZ(ctdefs.push_back(ctdef));
  }
  return ret;
}

int ObDASUtils::project_storage_row(const ObDASDMLBaseCtDef &dml_ctdef,
                                    const ObDASWriteBuffer::DmlRow &dml_row,
                                    const IntFixedArray &row_projector,
                                    ObIAllocator &allocator,
                                    ObNewRow &storage_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_projector.count(); ++i) {
    int64_t projector_idx = row_projector.at(i);
    const ObObjMeta &col_type = dml_ctdef.column_types_.at(i);
    const ObAccuracy &col_accuracy = dml_ctdef.column_accuracys_.at(i);
    if (projector_idx < 0) {
      //this column is not touched by query, only need to be marked as nop
      storage_row.cells_[i].set_nop_value();
    } else if (OB_FAIL(dml_row.cells()[projector_idx].to_obj(storage_row.cells_[i], col_type))) {
      LOG_WARN("stored row to new row obj failed", K(ret),
               K(dml_row.cells()[projector_idx]), K(col_type), K(projector_idx), K(i));
    } else if (OB_FAIL(reshape_storage_value(col_type, col_accuracy, allocator, storage_row.cells_[i]))) {
      LOG_WARN("reshape storage value failed", K(ret));
    }
  }
  //to project shadow rowkey with unique index
  if (OB_SUCC(ret) && dml_ctdef.spk_cnt_) {
    bool need_shadow_columns = false;
    int64_t index_key_cnt = dml_ctdef.rowkey_cnt_ - dml_ctdef.spk_cnt_;
    if (lib::is_mysql_mode()) {
      //compatible with mysql: contain null value in unique index key,
      //need to fill shadow pk with the real pk value
      bool rowkey_has_null = false;
      for (int64_t i = 0; !rowkey_has_null && i < index_key_cnt; i++) {
        rowkey_has_null = storage_row.cells_[i].is_null();
      }
      need_shadow_columns = rowkey_has_null;
    } else {
      //compatible with Oracle: only all unique index keys are null value
      //need to fill shadow pk with the real pk value
      bool is_rowkey_all_null = true;
      for (int64_t i = 0; is_rowkey_all_null && i < index_key_cnt; i++) {
        is_rowkey_all_null = storage_row.cells_[i].is_null();
      }
      need_shadow_columns = is_rowkey_all_null;
    }
    if (!need_shadow_columns) {
      for (int64_t i = 0; i < dml_ctdef.spk_cnt_; ++i) {
        int64_t spk_idx = index_key_cnt + i;
        storage_row.cells_[spk_idx].set_null();
      }
    }
  }
  return ret;
}

int ObDASUtils::reshape_storage_value(const ObObjMeta &col_type,
                                      const ObAccuracy &col_accuracy,
                                      ObIAllocator &allocator,
                                      ObObj &value)
{
  int ret = OB_SUCCESS;
  if (value.is_binary()) {
    int32_t binary_len = col_accuracy.get_length();
    int32_t len = value.get_string_len();
    if (binary_len > len) {
      char *dest_str = NULL;
      const char *str = value.get_string_ptr();
      if (OB_ISNULL(dest_str = (char *)(allocator.alloc(binary_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mem to binary", K(ret), K(binary_len));
      } else {
        char pad_char = '\0';
        MEMCPY(dest_str, str, len);
        MEMSET(dest_str + len, pad_char, binary_len - len);
        value.set_binary(ObString(binary_len, dest_str));
      }
    }
  } else if (lib::is_oracle_mode() && value.is_character_type() && value.get_string_len() == 0) {
    // Oracle compatibility mode: '' as null
    LOG_DEBUG("reshape empty string to null", K(value));
    value.set_null();
  } else if (value.is_fixed_len_char_type()) {
    const char *str = value.get_string_ptr();
    int32_t len = value.get_string_len();
    ObString space_pattern = ObCharsetUtils::get_const_str(value.get_collation_type(), ' ');
    for (; len >= space_pattern.length(); len -= space_pattern.length()) {
      if (0 != MEMCMP(str + len - space_pattern.length(), space_pattern.ptr(), space_pattern.length())) {
        break;
      }
    }
    // need to set collation type
    value.set_string(value.get_type(), ObString(len, str));
    value.set_collation_type(value.get_collation_type());
  }
  return ret;
}

int ObDASUtils::generate_spatial_index_rows(
    ObIAllocator &allocator,
    const ObDASDMLBaseCtDef &das_ctdef,
    const ObString &wkb_str,
    const IntFixedArray &row_projector,
    const ObDASWriteBuffer::DmlRow &dml_row,
    ObSpatIndexRow &spat_rows)
{
  int ret = OB_SUCCESS;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs_item = NULL;
  const ObSrsBoundsItem *srs_bound = NULL;
  uint32_t srid = UINT32_MAX;
  uint64_t rowkey_num = das_ctdef.table_param_.get_data_table().get_rowkey_column_num();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "S2Adapter"));

  if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb_str, srid))) {
    LOG_WARN("failed to get srid", K(ret), K(wkb_str));
  } else if (srid != 0 &&
      OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
    LOG_WARN("failed to get srs guard", K(ret), K(MTL_ID()), K(srid));
  } else if (srid != 0 &&
      OB_FAIL(srs_guard.get_srs_item(srid, srs_item))) {
    LOG_WARN("failed to get srs item", K(ret), K(MTL_ID()), K(srid));
  } else if (((srid == 0) || !(srs_item->is_geographical_srs())) &&
              OB_FAIL(OTSRS_MGR->get_srs_bounds(srid, srs_item, srs_bound))) {
    LOG_WARN("failed to get srs bound", K(ret), K(srid));
  } else {
    ObS2Adapter s2object(&allocator, srid != 0 ? srs_item->is_geographical_srs() : false);
    ObSpatialMBR spa_mbr;
    ObObj *obj_arr = NULL;
    ObS2Cellids cellids;
    char *mbr = NULL;
    int64_t mbr_len = 0;
    if (OB_FAIL(s2object.init(wkb_str, srs_bound))) {
      LOG_WARN("Init s2object failed", K(ret));
    } else if (OB_FAIL(s2object.get_cellids(cellids, false))) {
      LOG_WARN("Get cellids from s2object failed", K(ret));
    } else if (OB_FAIL(s2object.get_mbr(spa_mbr))) {
      LOG_WARN("Get mbr from s2object failed", K(ret));
    } else if (spa_mbr.is_empty()) {
      if (cellids.size() == 0) {
        LOG_DEBUG("it's might be empty geometry collection", K(wkb_str));
      } else {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid geometry", K(ret), K(wkb_str));
      }
    } else if (OB_ISNULL(mbr = reinterpret_cast<char *>(allocator.alloc(OB_DEFAULT_MBR_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for spatial index row mbr", K(ret));
    } else if (OB_FAIL(spa_mbr.to_char(mbr, mbr_len))) {
      LOG_WARN("failed transform ObSpatialMBR to string", K(ret));
    } else {
      for (uint64_t i = 0; OB_SUCC(ret) && i < cellids.size(); i++) {
        if (OB_ISNULL(obj_arr = reinterpret_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * rowkey_num)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for spatial index row cells", K(ret));
        } else {
          // 索引行[cellid_obj][mbr_obj][rowkey_obj]
          for(uint64_t j = 0; OB_SUCC(ret) && j < rowkey_num; j++) {
            obj_arr[j].set_nop_value();
            const ObObjMeta &col_type = das_ctdef.column_types_.at(j);
            const ObAccuracy &col_accuracy = das_ctdef.column_accuracys_.at(j);
            int64_t projector_idx = row_projector.at(j);
            if (OB_FAIL(dml_row.cells()[projector_idx].to_obj(obj_arr[j], col_type))) {
              LOG_WARN("stored row to new row obj failed", K(ret),
                  K(dml_row.cells()[projector_idx]), K(col_type), K(projector_idx), K(j));
            } else if (OB_FAIL(ObDASUtils::reshape_storage_value(col_type, col_accuracy, allocator, obj_arr[j]))) {
              LOG_WARN("reshape storage value failed", K(ret), K(col_type), K(projector_idx), K(j));
            }
          }
          if (OB_SUCC(ret)) {
            int64_t cellid_col_idx = 0;
            int64_t mbr_col_idx = 1;
            obj_arr[cellid_col_idx].set_uint64(cellids.at(i));
            ObString mbr_val(mbr_len, mbr);
            obj_arr[mbr_col_idx].set_varchar(mbr_val);
            obj_arr[mbr_col_idx].set_collation_type(CS_TYPE_BINARY);
            obj_arr[mbr_col_idx].set_collation_level(CS_LEVEL_IMPLICIT);
            ObNewRow row;
            row.cells_ = obj_arr;
            row.count_ = rowkey_num;
            if (OB_FAIL(spat_rows.push_back(row))) {
              LOG_WARN("failed to push back spatial index row", K(ret), K(row));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDASUtils::wait_das_retry(int64_t retry_cnt)
{
  int ret = OB_SUCCESS;
  uint32_t timeout_factor = static_cast<uint32_t>((retry_cnt > 100) ? 100 : retry_cnt);
  int64_t sleep_us = 10000L * timeout_factor > THIS_WORKER.get_timeout_remain()
                                            ? THIS_WORKER.get_timeout_remain()
                                                : 10000L * timeout_factor;
  if (sleep_us > 0) {
    LOG_INFO("[DAS RETRY] will sleep", K(sleep_us), K(THIS_WORKER.get_timeout_remain()));
    THIS_WORKER.sched_wait();
    ob_usleep(static_cast<uint32_t>(sleep_us));
    THIS_WORKER.sched_run();
    if (THIS_WORKER.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("this worker is timeout after retry sleep. no more retry", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
