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
#include "pl/ob_pl.h"
#include "share/ob_tablet_autoincrement_service.h"
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
                                    blocksstable::ObDatumRow &storage_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_projector.count(); ++i) {
    int64_t projector_idx = row_projector.at(i);
    const ObObjMeta &col_type = dml_ctdef.column_types_.at(i);
    const ObAccuracy &col_accuracy = dml_ctdef.column_accuracys_.at(i);
    if (projector_idx < 0) {
      //this column is not touched by query, only need to be marked as nop
      storage_row.storage_datums_[i].set_nop();
    } else if (FALSE_IT(storage_row.storage_datums_[i].shallow_copy_from_datum(dml_row.cells()[projector_idx]))) {
    } else if (storage_row.storage_datums_[i].is_null()) {
      //nothing to do
    } else if (OB_FAIL(reshape_datum_value(col_type, col_accuracy, true, allocator, storage_row.storage_datums_[i]))) {
      LOG_WARN("reshape storage value failed", K(ret));
    } else if (col_type.is_lob_storage() && col_type.has_lob_header()) {
      storage_row.storage_datums_[i].set_has_lob_header();
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
        rowkey_has_null = storage_row.storage_datums_[i].is_null();
      }
      need_shadow_columns = rowkey_has_null;
    } else {
      //compatible with Oracle: only all unique index keys are null value
      //need to fill shadow pk with the real pk value
      bool is_rowkey_all_null = true;
      for (int64_t i = 0; is_rowkey_all_null && i < index_key_cnt; i++) {
        is_rowkey_all_null = storage_row.storage_datums_[i].is_null();
      }
      need_shadow_columns = is_rowkey_all_null;
    }
    if (!need_shadow_columns) {
      for (int64_t i = 0; i < dml_ctdef.spk_cnt_; ++i) {
        int64_t spk_idx = index_key_cnt + i;
        storage_row.storage_datums_[spk_idx].set_null();
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
  if (lib::is_oracle_mode() && value.is_character_type() && value.get_string_len() == 0) {
    // Oracle compatibility mode: '' as null
    LOG_DEBUG("reshape empty string to null", K(value));
    value.set_null();
  } else if (OB_FAIL(padding_fixed_string_value(col_accuracy.get_length(), allocator, value))) {
    LOG_WARN("padding char value failed", K(ret), K(col_accuracy), K(value));
  }
  return ret;
}

int ObDASUtils::padding_fixed_string_value(int64_t max_len, ObIAllocator &allocator, ObObj &value)
{
  int ret = OB_SUCCESS;
  if (value.is_binary()) {
    int32_t binary_len = max_len;
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

int ObDASUtils::reshape_datum_value(const ObObjMeta &col_type,
                                    const ObAccuracy &col_accuracy,
                                    const bool enable_oracle_empty_char_reshape_to_null,
                                    ObIAllocator &allocator,
                                    blocksstable::ObStorageDatum &datum_value)
{
  int ret = OB_SUCCESS;
  if (col_type.is_binary()) {
    int32_t binary_len = col_accuracy.get_length();
    int32_t len = datum_value.len_;
    if (binary_len > len) {
      char *dest_str = NULL;
      const char *str = datum_value.ptr_;
      if (OB_ISNULL(dest_str = (char *)(allocator.alloc(binary_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mem to binary", K(ret), K(binary_len));
      } else {
        char pad_char = '\0';
        MEMCPY(dest_str, str, len);
        MEMSET(dest_str + len, pad_char, binary_len - len);
        datum_value.set_string(ObString(binary_len, dest_str));
      }
    }
  } else if (lib::is_oracle_mode() && !enable_oracle_empty_char_reshape_to_null && col_type.is_character_type() && datum_value.len_ == 0) {
    // Oracle compatibility mode: '' as null
    LOG_DEBUG("reshape empty string to null", K(datum_value));
    datum_value.set_null();
  } else if (col_type.is_fixed_len_char_type()) {
    const char *str = datum_value.ptr_;
    int32_t len = datum_value.len_;
    ObString space_pattern = ObCharsetUtils::get_const_str(col_type.get_collation_type(), ' ');
    for (; len >= space_pattern.length(); len -= space_pattern.length()) {
      if (0 != MEMCMP(str + len - space_pattern.length(), space_pattern.ptr(), space_pattern.length())) {
        break;
      }
    }
    datum_value.set_string(ObString(len, str));
  }
  return ret;
}

static bool fast_check_vector_is_all_null(ObIVector *vector, const int64_t batch_size)
{
  bool is_all_null = false;
  VectorFormat format = vector->get_format();
  switch (format) {
    case VEC_FIXED:
    case VEC_DISCRETE:
    case VEC_CONTINUOUS:
      is_all_null = static_cast<ObBitmapNullVectorBase *>(vector)->get_nulls()->is_all_true(batch_size);
      break;
    default:
      break;
  }
  return is_all_null;
}

static int new_discrete_vector(VecValueTypeClass value_tc,
                               const int64_t max_batch_size,
                               ObIAllocator &allocator,
                               ObDiscreteBase *&result_vec)
{
  int ret = OB_SUCCESS;
  result_vec = nullptr;
  ObIVector *vector = nullptr;
  switch (value_tc) {
#define DISCRETE_VECTOR_INIT_SWITCH(value_tc)                           \
  case value_tc: {                                                      \
    using VecType = RTVectorType<VEC_DISCRETE, value_tc>;               \
    static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");        \
    vector = OB_NEWx(VecType, &allocator, nullptr, nullptr, nullptr);   \
    break;                                                              \
  }
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_STRING);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_RAW);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_LOB);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_JSON);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_GEO);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_UDT);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_COLLECTION);
    DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
#undef DISCRETE_VECTOR_INIT_SWITCH
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected discrete vector value type class", KR(ret), K(value_tc));
      break;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(vector)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc vecttor", KR(ret));
  } else {
    ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
    const int64_t nulls_size = ObBitVector::memory_size(max_batch_size);
    const int64_t lens_size = sizeof(int32_t) * max_batch_size;
    const int64_t ptrs_size = sizeof(char *) * max_batch_size;
    ObBitVector *nulls = nullptr;
    int32_t *lens = nullptr;
    char **ptrs = nullptr;
    if (OB_ISNULL(nulls = to_bit_vector(allocator.alloc(nulls_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
    } else if (OB_ISNULL(lens = static_cast<int32_t *>(allocator.alloc(lens_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(lens_size));
    } else if (OB_ISNULL(ptrs = static_cast<char **>(allocator.alloc(ptrs_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(ptrs_size));
    } else {
      nulls->reset(max_batch_size);
      discrete_vec->set_nulls(nulls);
      discrete_vec->set_lens(lens);
      discrete_vec->set_ptrs(ptrs);
      result_vec = discrete_vec;
    }
  }
  return ret;
}

int ObDASUtils::reshape_vector_value(const ObObjMeta &col_type,
                                     const ObAccuracy &col_accuracy,
                                     ObIAllocator &allocator,
                                     ObIVector *&vector,
                                     const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vector) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(vector), K(size));
  } else if (fast_check_vector_is_all_null(vector, size)) {
    // do nothing
  } else if (col_type.is_binary()) {
    const int32_t binary_len = col_accuracy.get_length();
    const char pad_char = '\0';
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_CONTINUOUS:
      {
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
        ObDiscreteBase *discrete_vec = nullptr;
        char *data = continuous_vec->get_data();
        uint32_t *offsets = continuous_vec->get_offsets();
        char **ptrs = nullptr;
        ObLength *lens = nullptr;
        bool has_value_change = false;
        VecValueTypeClass value_tc = get_vec_value_tc(col_type.get_type(),
                                                      col_type.get_scale(),
                                                      col_type.get_stored_precision());
        if (OB_FAIL(new_discrete_vector(value_tc, size, allocator, discrete_vec))) {
          LOG_WARN("fail to new discrete vector", KR(ret));
        } else {
          ptrs = discrete_vec->get_ptrs();
          lens = discrete_vec->get_lens();
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          if (continuous_vec->is_null(i)) {
            discrete_vec->set_null(i);
          } else {
            const ObLength len = offsets[i + 1] - offsets[i];
            char *str = data + offsets[i];
            if (len < binary_len) {
              char *dest_str = nullptr;
              if (OB_ISNULL(dest_str = (char *)(allocator.alloc(binary_len)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to alloc mem to binary", K(ret), K(binary_len));
              } else {
                MEMCPY(dest_str, str, len);
                MEMSET(dest_str + len, pad_char, binary_len - len);
                ptrs[i] = dest_str;
                lens[i] = binary_len;
                has_value_change = true;
              }
            } else {
              ptrs[i] = str;
              lens[i] = binary_len;
            }
          }
        }
        if (OB_SUCC(ret) && has_value_change) {
          vector = discrete_vec;
        }
        break;
      }
      case VEC_DISCRETE:
      {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        char **ptrs = discrete_vec->get_ptrs();
        ObLength *lens =discrete_vec->get_lens();
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          if (!discrete_vec->is_null(i) && lens[i] < binary_len) {
            char *str = ptrs[i];
            ObLength len = lens[i];
            char *dest_str = nullptr;
            if (OB_ISNULL(dest_str = (char *)(allocator.alloc(binary_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc mem to binary", K(ret), K(binary_len));
            } else {
              MEMCPY(dest_str, str, len);
              MEMSET(dest_str + len, pad_char, binary_len - len);
              ptrs[i] = dest_str;
              lens[i] = binary_len;
            }
          }
        }
        break;
      }
      case VEC_UNIFORM:
      {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        ObDatum *datums = uniform_vec->get_datums();
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          ObDatum &datum = datums[i];
          if (!datum.is_null() && datum.len_ < binary_len) {
            const char *str = datum.ptr_;
            ObLength len = datum.len_;
            char *dest_str = nullptr;
            if (OB_ISNULL(dest_str = (char *)(allocator.alloc(binary_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc mem to binary", K(ret), K(binary_len));
            } else {
              MEMCPY(dest_str, str, len);
              MEMSET(dest_str + len, pad_char, binary_len - len);
              datum.ptr_ = dest_str;
              datum.len_ = binary_len;
            }
          }
        }
        break;
      }
      case VEC_UNIFORM_CONST:
      {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        ObDatum &datum = uniform_vec->get_datums()[0];
        if (!datum.is_null() && datum.len_ < binary_len) {
          const char *str = datum.ptr_;
          ObLength len = datum.len_;
          char *dest_str = nullptr;
          if (OB_ISNULL(dest_str = (char *)(allocator.alloc(binary_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc mem to binary", K(ret), K(binary_len));
          } else {
            MEMCPY(dest_str, str, len);
            MEMSET(dest_str + len, pad_char, binary_len - len);
            datum.ptr_ = dest_str;
            datum.len_ = binary_len;
          }
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected binary vector format", KR(ret), K(format), K(col_type));
        break;
    }
  } else if (col_type.is_fixed_len_char_type()) {
    const ObString space_pattern = ObCharsetUtils::get_const_str(col_type.get_collation_type(), ' ');
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_CONTINUOUS:
      {
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
        ObDiscreteBase *discrete_vec = nullptr;
        char *data = continuous_vec->get_data();
        uint32_t *offsets = continuous_vec->get_offsets();
        char **ptrs = nullptr;
        ObLength *lens = nullptr;
        bool has_value_change = false;
        VecValueTypeClass value_tc = get_vec_value_tc(col_type.get_type(),
                                                      col_type.get_scale(),
                                                      col_type.get_stored_precision());
        if (OB_FAIL(new_discrete_vector(value_tc, size, allocator, discrete_vec))) {
          LOG_WARN("fail to new discrete vector", KR(ret));
        } else {
          ptrs = discrete_vec->get_ptrs();
          lens = discrete_vec->get_lens();
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          if (continuous_vec->is_null(i)) {
            discrete_vec->set_null(i);
          } else {
            const ObLength length = offsets[i + 1] - offsets[i];
            if (lib::is_oracle_mode() && 0 == length) {
              // Oracle compatibility mode: '' as null
              LOG_DEBUG("reshape empty string to null", K(i));
              continuous_vec->set_null(i);
              discrete_vec->set_null(i);
            } else {
              ObLength len = length;
              char *str = data + offsets[i];
              for (; len >= space_pattern.length(); len -= space_pattern.length()) {
                if (0 != MEMCMP(str + len - space_pattern.length(), space_pattern.ptr(), space_pattern.length())) {
                  break;
                }
              }
              ptrs[i] = str;
              lens[i] = len;
              if (len != length) {
                has_value_change = true;
              }
            }
          }
        }
        if (OB_SUCC(ret) && has_value_change) {
          vector = discrete_vec;
        }
        break;
      }
      case VEC_DISCRETE:
      {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        char **ptrs = discrete_vec->get_ptrs();
        ObLength *lens =discrete_vec->get_lens();
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          if (!discrete_vec->is_null(i)) {
            ObLength len = lens[i];
            if (lib::is_oracle_mode() && 0 == len) {
              // Oracle compatibility mode: '' as null
              LOG_DEBUG("reshape empty string to null", K(i));
              discrete_vec->set_null(i);
            } else {
              const char *str = ptrs[i];
              for (; len >= space_pattern.length(); len -= space_pattern.length()) {
                if (0 != MEMCMP(str + len - space_pattern.length(), space_pattern.ptr(), space_pattern.length())) {
                  break;
                }
              }
              lens[i] = len;
            }
          }
        }
        break;
      }
      case VEC_UNIFORM:
      {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        ObDatum *datums = uniform_vec->get_datums();
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          ObDatum &datum = datums[i];
          if (!datum.is_null()) {
            ObLength len = datum.len_;
            if (lib::is_oracle_mode() && 0 == len) {
              // Oracle compatibility mode: '' as null
              LOG_DEBUG("reshape empty string to null", K(i));
              datum.set_null();
            } else {
              const char *str = datum.ptr_;
              for (; len >= space_pattern.length(); len -= space_pattern.length()) {
                if (0 != MEMCMP(str + len - space_pattern.length(), space_pattern.ptr(), space_pattern.length())) {
                  break;
                }
              }
              datum.len_ = len;
            }
          }
        }
        break;
      }
      case VEC_UNIFORM_CONST:
      {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        ObDatum &datum = uniform_vec->get_datums()[0];
        if (!datum.is_null()) {
          ObLength len = datum.len_;
          if (lib::is_oracle_mode() && 0 == len) {
            // Oracle compatibility mode: '' as null
            LOG_DEBUG("reshape empty string to null");
            datum.set_null();
          } else {
            const char *str = datum.ptr_;
            for (; len >= space_pattern.length(); len -= space_pattern.length()) {
              if (0 != MEMCMP(str + len - space_pattern.length(), space_pattern.ptr(), space_pattern.length())) {
                break;
              }
            }
            datum.len_ = len;
          }
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fixed len char vector format", KR(ret), K(format), K(col_type));
        break;
    }
  } else if (lib::is_oracle_mode() && col_type.is_character_type()) {
    // Oracle compatibility mode: '' as null
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_CONTINUOUS:
      {
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
        uint32_t *offsets = continuous_vec->get_offsets();
        for (int64_t i = 0; i < size; ++i) {
          if (!continuous_vec->is_null(i) && offsets[i + 1] == offsets[i]) {
            LOG_DEBUG("reshape empty string to null", K(i));
            continuous_vec->set_null(i);
          }
        }
        break;
      }
      case VEC_DISCRETE:
      {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        ObLength *lens =discrete_vec->get_lens();
        for (int64_t i = 0; i < size; ++i) {
          if (!discrete_vec->is_null(i) && 0 == lens[i]) {
            LOG_DEBUG("reshape empty string to null", K(i));
            discrete_vec->set_null(i);
          }
        }
        break;
      }
      case VEC_UNIFORM:
      {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        ObDatum *datums = uniform_vec->get_datums();
        for (int64_t i = 0; i < size; ++i) {
          ObDatum &datum = datums[i];
          if (!datum.is_null() && 0 == datum.len_) {
            LOG_DEBUG("reshape empty string to null", K(i));
            datum.set_null();
          }
        }
        break;
      }
      case VEC_UNIFORM_CONST:
      {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        ObDatum &datum = uniform_vec->get_datums()[0];
        if (!datum.is_null() && 0 == datum.len_) {
          LOG_DEBUG("reshape empty string to null");
          datum.set_null();
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected character vector format", KR(ret), K(format), K(col_type));
        break;
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

int ObDASUtils::find_child_das_def(const ObDASBaseCtDef *root_ctdef,
                                   ObDASBaseRtDef *root_rtdef,
                                   ObDASOpType op_type,
                                   const ObDASBaseCtDef *&target_ctdef,
                                   ObDASBaseRtDef *&target_rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_ctdef) || OB_ISNULL(root_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root ctdef or rtdef is nullptr", K(ret), KP(root_ctdef), K(root_rtdef));
  } else if (OB_UNLIKELY(root_ctdef->op_type_ != root_rtdef->op_type_
      || root_ctdef->children_cnt_ != root_rtdef->children_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the op_type of ctdef and rtdef do not match", K(ret),
             K(root_ctdef->op_type_), K(root_rtdef->op_type_),
             K(root_ctdef->children_cnt_), K(root_rtdef->children_cnt_));
  } else if (root_ctdef->op_type_ == op_type) {
    target_ctdef = root_ctdef;
    target_rtdef = root_rtdef;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < root_ctdef->children_cnt_; ++i) {
      if (OB_FAIL(find_child_das_def(root_ctdef->children_[i],
                                     root_rtdef->children_[i],
                                     op_type,
                                     target_ctdef,
                                     target_rtdef))) {
        LOG_WARN("find child das def failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDASUtils::find_child_das_ctdef(const ObDASBaseCtDef *root_ctdef,
                                   ObDASOpType op_type,
                                   const ObDASBaseCtDef *&target_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root ctdef or rtdef is nullptr", K(ret), KP(root_ctdef));
  } else if (root_ctdef->op_type_ == op_type) {
    target_ctdef = root_ctdef;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < root_ctdef->children_cnt_; ++i) {
      if (OB_FAIL(find_child_das_ctdef(root_ctdef->children_[i],
                                     op_type,
                                     target_ctdef))) {
        LOG_WARN("find child das def failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDASUtils::generate_mlog_row(const ObLSID &ls_id,
                                  const ObTabletID &tablet_id,
                                  const storage::ObDMLBaseParam &dml_param,
                                  blocksstable::ObDatumRow &row,
                                  ObDASOpType op_type,
                                  bool is_old_row)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  uint64_t autoinc_seq = 0;
  ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
  if (OB_ISNULL(dml_param.table_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table param is null", KR(ret));
  } else if (!dml_param.table_param_->get_data_table().is_mlog_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table is not materialized view log",
        KR(ret), K(dml_param.table_param_->get_data_table()));
  } else if (row.count_ < 4) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("each mlog row should at least contain 4 columns", KR(ret), K(row.count_));
  } else if (OB_FAIL(auto_inc.get_autoinc_seq_for_mlog(tenant_id, ls_id, tablet_id, autoinc_seq))) {
    LOG_WARN("get_autoinc_seq fail", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
  } else {
    // mlog_row = | base_table_rowkey_cols | partition key cols | sequence_col | ... | dmltype_col | old_new_col |
    int sequence_col = 0;
    int dmltype_col = row.count_ - 2;
    int old_new_col = row.count_ - 1;
    const ObTableDMLParam::ObColDescArray &col_descs = dml_param.table_param_->get_col_descs();
    bool found_seq_col = false;
    for (int64_t i = 0; !found_seq_col && (i < row.count_); ++i) {
      if (OB_MLOG_SEQ_NO_COLUMN_ID == col_descs.at(i).col_id_) {
        sequence_col = i; // sequence_no is the last rowkey
        found_seq_col = true;
      }
    }

    row.storage_datums_[sequence_col].reuse();
    row.storage_datums_[dmltype_col].reuse();
    row.storage_datums_[old_new_col].reuse();

    row.storage_datums_[sequence_col].set_int(static_cast<int64_t>(autoinc_seq));
    if (sql::DAS_OP_TABLE_DELETE == op_type) {
      row.storage_datums_[dmltype_col].set_string(ObString("D"));
      row.storage_datums_[old_new_col].set_string(ObString("O"));
    } else if (sql::DAS_OP_TABLE_UPDATE == op_type) {
      row.storage_datums_[dmltype_col].set_string(ObString("U"));
      if (is_old_row) {
        row.storage_datums_[old_new_col].set_string(ObString("O"));
      } else {
        row.storage_datums_[old_new_col].set_string(ObString("N"));
      }
    } else {
      row.storage_datums_[dmltype_col].set_string(ObString("I"));
      row.storage_datums_[old_new_col].set_string(ObString("N"));
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
