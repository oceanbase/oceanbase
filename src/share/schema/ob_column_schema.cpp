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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_column_schema.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_service.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace std;
using namespace common;
namespace share
{
namespace schema
{
const char *ObColumnSchemaV2::convert_column_type_to_str(ColumnType type)
{
  const char *type_str = NULL;
  if (ObIntType == type) {
    type_str = STR_COLUMN_TYPE_INT;
  } else if (ObUInt64Type == type) {
    type_str = STR_COLUMN_TYPE_UINT64;
  } else if (ObVarcharType == type) {
    type_str = STR_COLUMN_TYPE_VCHAR;
  } else if (ObDateTimeType == type) {
    type_str = STR_COLUMN_TYPE_DATETIME;
  } else if (ObTimestampType == type) {
    type_str = STR_COLUMN_TYPE_TIMESTAMP;
  } else if (ObNumberType == type) {
    type_str = STR_COLUMN_TYPE_NUMBER;
  } else if (ObUnknownType == type) {
    type_str = STR_COLUMN_TYPE_UNKNOWN;
  } else if (ObFloatType == type) {
    type_str = STR_COLUMN_TYPE_FLOAT;
  } else if (ObUFloatType == type) {
    type_str = STR_COLUMN_TYPE_UFLOAT;
  } else if (ObDoubleType == type) {
    type_str = STR_COLUMN_TYPE_DOUBLE;
  } else if (ObUDoubleType == type) {
    type_str = STR_COLUMN_TYPE_UDOUBLE;
  } else if (ObRawType == type) {
    type_str = STR_COLUMN_TYPE_RAW;
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Not supported column type, ", K(type));
  }
  return type_str;
}

ColumnType ObColumnSchemaV2::convert_str_to_column_type(const char *str)
{
  ColumnType type = ObNullType;
  if (STRCMP(str, STR_COLUMN_TYPE_INT) == 0) {
    type = ObIntType;
  } else if (STRCMP(str, STR_COLUMN_TYPE_UINT64) == 0) {
    type = ObUInt64Type;
  } else if (STRCMP(str, STR_COLUMN_TYPE_FLOAT) == 0) {
    type = ObFloatType;
  } else if (STRCMP(str, STR_COLUMN_TYPE_UFLOAT) == 0) {
    type = ObUFloatType;
  } else if (STRCMP(str, STR_COLUMN_TYPE_DOUBLE) == 0) {
    type = ObDoubleType;
  } else if (STRCMP(str, STR_COLUMN_TYPE_UDOUBLE) == 0) {
    type = ObUDoubleType;
  } else if (STRCMP(str, STR_COLUMN_TYPE_VCHAR) == 0) {
    type = ObVarcharType;
  } else if (STRCMP(str, STR_COLUMN_TYPE_DATETIME) == 0) {
    type = ObDateTimeType;
  }  else if (STRCMP(str, STR_COLUMN_TYPE_TIMESTAMP) == 0) {
    type = ObTimestampType;
  } else if (STRCMP(str, STR_COLUMN_TYPE_NUMBER) == 0) {
    return ObNumberType;
  } else if (STRCMP(str, STR_COLUMN_TYPE_RAW) == 0) {
    return ObRawType;
  } else if (STRCMP(str, STR_COLUMN_TYPE_UNKNOWN) == 0) {
    type = ObUnknownType;
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Not supported column type, ", K(str));
  }
  return type;
}

ObColumnSchemaV2::ObColumnSchemaV2()
    : ObSchema()
{
  reset();
}

ObColumnSchemaV2::ObColumnSchemaV2(ObIAllocator *allocator)
    : ObSchema(allocator)
{
  reset();
}

ObColumnSchemaV2::ObColumnSchemaV2(const ObColumnSchemaV2 &src_schema)
    : ObSchema()
{
  reset();
  *this = src_schema;
}

ObColumnSchemaV2::~ObColumnSchemaV2()
{
}

ObColumnSchemaV2 &ObColumnSchemaV2::operator =(const ObColumnSchemaV2 &src_schema)
{
  if (this != &src_schema) {
    reset();
    error_ret_ = src_schema.error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    table_id_ = src_schema.table_id_;
    column_id_ = src_schema.column_id_;
    schema_version_ = src_schema.schema_version_;
    rowkey_position_ = src_schema.rowkey_position_;
    index_position_ = src_schema.index_position_;
    order_in_rowkey_ = src_schema.order_in_rowkey_;
    tbl_part_key_pos_ = src_schema.tbl_part_key_pos_;
    meta_type_ = src_schema.meta_type_;
    accuracy_ = src_schema.accuracy_;
    is_nullable_ = src_schema.is_nullable_;
    is_zero_fill_ = src_schema.is_zero_fill_;
    is_autoincrement_ = src_schema.is_autoincrement_;
    is_hidden_ = src_schema.is_hidden_;
    charset_type_ = src_schema.charset_type_;
    is_binary_collation_ = src_schema.is_binary_collation_;
    on_update_current_timestamp_ = src_schema.on_update_current_timestamp_;
    column_flags_ = src_schema.column_flags_;
    prev_column_id_ = src_schema.prev_column_id_;
    next_column_id_ = src_schema.next_column_id_;
    encoding_type_ = src_schema.encoding_type_;
    sequence_id_ = src_schema.sequence_id_;
    srs_id_ = src_schema.srs_id_;
    udt_set_id_ = src_schema.udt_set_id_;
    sub_type_ = src_schema.sub_type_;

    int ret = OB_SUCCESS;
    if (OB_FAIL(deep_copy_obj(src_schema.orig_default_value_, orig_default_value_))) {
      LOG_WARN("Fail to deepy copy orig_default_value, ", K(ret));
    } else if (OB_FAIL(deep_copy_obj(src_schema.cur_default_value_, cur_default_value_))) {
      LOG_WARN("Fail to deep copy cur_default_value, ", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.column_name_, column_name_))) {
      LOG_WARN("Fail to deep copy column name, ", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.comment_, comment_))) {
      LOG_WARN("Fail to deep copy comment, ", K(ret));
    } else if (OB_FAIL(set_extended_type_info(src_schema.extended_type_info_))) {
      LOG_WARN("set_extended_type_info failed", K(ret));
    } else if (src_schema.column_ref_idxs_ != NULL) {
      if (OB_FAIL(alloc_column_ref_set())) {
        LOG_WARN("alloc column ref set failed", K(ret));
      } else if (OB_FAIL(column_ref_idxs_->add_members(*src_schema.column_ref_idxs_))) {
        LOG_WARN("add members to column reference idxs failed", K(ret));
      }
    } else {/*do nothing*/}

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
    LOG_DEBUG("operator =", K(src_schema), K(*this));
  }
  return *this;
}

int ObColumnSchemaV2::assign(const ObColumnSchemaV2 &other)
{
  *this = other;
  return error_ret_;
}

bool ObColumnSchemaV2::operator==(const ObColumnSchemaV2 &r) const
{
  return (tenant_id_ == r.tenant_id_ && table_id_ == r.table_id_ && column_id_ == r.column_id_
      && schema_version_ == r.schema_version_);
}

bool ObColumnSchemaV2::operator !=(const ObColumnSchemaV2 &r) const
{
  return !(*this == r);
}

int64_t ObColumnSchemaV2::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);

  convert_size += orig_default_value_.get_deep_copy_size();
  convert_size += cur_default_value_.get_deep_copy_size();
  convert_size += column_name_.length() + 1;
  convert_size += comment_.length() + 1;
  if (column_ref_idxs_ != NULL) {
    convert_size += sizeof(ColumnReferenceSet);
  }
  convert_size += extended_type_info_.count() * static_cast<int64_t>(sizeof(ObString));
  for (int64_t i = 0; i < extended_type_info_.count(); ++i) {
    convert_size += extended_type_info_.at(i).length() + 1;
  }
  return convert_size;
}

bool ObColumnSchemaV2::is_prefix_column() const
{
  bool bret = false;
  if (is_hidden() && is_generated_column()) {
    const char *prefix_str = "__substr";
    int64_t min_len = min(column_name_.length(), static_cast<int64_t>(strlen(prefix_str)));
    bret = (0 == strncasecmp(get_column_name(), prefix_str, min_len));
  }
  return bret;
}

bool ObColumnSchemaV2::is_func_idx_column() const
{
  bool bret = false;
  if (is_hidden() && is_generated_column()) {
    const char *func_idx_str = "SYS_NC";
    int64_t min_len = min(column_name_.length(), static_cast<int64_t>(strlen(func_idx_str)));
    bret = (0 == strncmp(get_column_name(), func_idx_str, min_len));
  }

  return bret;
}

void ObColumnSchemaV2::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  column_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  rowkey_position_ = 0;
  index_position_ = 0;
  order_in_rowkey_ = ASC;
  tbl_part_key_pos_ = 0;
  meta_type_.reset();
  accuracy_.reset();
  is_nullable_ = true;
  is_zero_fill_ = false;
  is_autoincrement_ = false;
  is_hidden_ = false;
  charset_type_ = ObCharset::get_default_charset();
  is_binary_collation_ = false;
  on_update_current_timestamp_ = false;
  orig_default_value_.reset();
  cur_default_value_.reset();
  column_name_.reset();
  comment_.reset();
  column_flags_ = NON_CASCADE_FLAG;
  column_ref_idxs_ = NULL;
  prev_column_id_ = UINT64_MAX;
  next_column_id_ = UINT64_MAX;
  encoding_type_ = INT64_MAX;
  sequence_id_ = INT64_MAX;
  srs_id_ = OB_DEFAULT_COLUMN_SRS_ID;
  udt_set_id_ = 0;
  sub_type_ = 0;
  reset_string_array(extended_type_info_);
  ObSchema::reset();
}

OB_DEF_SERIALIZE(ObColumnSchemaV2)
{
  int ret = OB_SUCCESS;
  bool has_column_ref = (column_ref_idxs_ != NULL);
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              table_id_,
              column_id_,
              schema_version_,
              rowkey_position_,
              index_position_,
              order_in_rowkey_,
              tbl_part_key_pos_,
              meta_type_,
              accuracy_,
              is_nullable_,
              is_zero_fill_,
              is_autoincrement_,
              is_hidden_,
              charset_type_,
              is_binary_collation_,
              on_update_current_timestamp_,
              orig_default_value_,
              cur_default_value_,
              column_name_,
              comment_,
              column_flags_,
              has_column_ref);
  if (OB_SUCC(ret) && has_column_ref) {
    OB_UNIS_ENCODE(*column_ref_idxs_);
  }

  LST_DO_CODE(OB_UNIS_ENCODE,
              prev_column_id_,
              next_column_id_);

  if (!OB_SUCC(ret)) {
    LOG_WARN("Fail to serialize fixed length data", K(ret));
  } else if (OB_FAIL(serialize_string_array(buf, buf_len, pos, extended_type_info_))) {
    LOG_WARN("serialize_string_array failed", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
                sequence_id_,
                srs_id_,
                udt_set_id_,
                sub_type_);
  }

  return ret;
}

OB_DEF_DESERIALIZE(ObColumnSchemaV2)
{
  int ret = OB_SUCCESS;
  ObObj orig_default_value;
  ObObj cur_default_value;
  ObString column_name;
  ObString comment;
  bool has_column_ref = false;

  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              table_id_,
              column_id_,
              schema_version_,
              rowkey_position_,
              index_position_,
              order_in_rowkey_,
              tbl_part_key_pos_,
              meta_type_,
              accuracy_,
              is_nullable_,
              is_zero_fill_,
              is_autoincrement_,
              is_hidden_,
              charset_type_,
              is_binary_collation_,
              on_update_current_timestamp_,
              orig_default_value,
              cur_default_value,
              column_name,
              comment,
              column_flags_,
              has_column_ref);
  if (OB_SUCC(ret) && has_column_ref) {
    if (OB_FAIL(alloc_column_ref_set())) {
      LOG_WARN("alloc column reference set failed", K(ret));
    }
    OB_UNIS_DECODE(*column_ref_idxs_);
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              prev_column_id_,
              next_column_id_);

  if (!OB_SUCC(ret)) {
    LOG_WARN("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deserialize_string_array(buf, data_len, pos, extended_type_info_))) {
    LOG_WARN("deserialize_string_array failed", K(ret));
  } else if (OB_FAIL(deep_copy_obj(orig_default_value, orig_default_value_))) {
    LOG_WARN("Fail to deep copy orig_default_value, ", K(ret), K_(orig_default_value));
  } else if (OB_FAIL(deep_copy_obj(cur_default_value, cur_default_value_))) {
    LOG_WARN("Fail to deep copy cur_default_value, ", K(ret), K_(cur_default_value));
  } else if (OB_FAIL(deep_copy_str(column_name, column_name_))) {
    LOG_WARN("Fail to deep copy column_name, ", K(ret), K_(column_name));
  } else if (OB_FAIL(deep_copy_str(comment, comment_))) {
    LOG_WARN("Fail to deep copy comment, ", K(ret), K_(comment));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE,
                sequence_id_,
                srs_id_,
                udt_set_id_,
                sub_type_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObColumnSchemaV2)
{
  int64_t len = 0;
  bool has_column_ref = (column_ref_idxs_ != NULL);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              table_id_,
              column_id_,
              schema_version_,
              rowkey_position_,
              index_position_,
              order_in_rowkey_,
              tbl_part_key_pos_,
              meta_type_,
              accuracy_,
              is_nullable_,
              is_zero_fill_,
              is_autoincrement_,
              is_hidden_,
              charset_type_,
              is_binary_collation_,
              on_update_current_timestamp_,
              orig_default_value_,
              cur_default_value_,
              column_name_,
              comment_,
              column_flags_,
              has_column_ref);
  if (has_column_ref) {
    OB_UNIS_ADD_LEN(*column_ref_idxs_);
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              prev_column_id_,
              next_column_id_);
  len += get_string_array_serialize_size(extended_type_info_);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              sequence_id_,
              srs_id_,
              udt_set_id_,
              sub_type_);
  return len;
}

int ObColumnSchemaV2::set_part_key_pos(const int64_t part_key_pos)
{
  int ret = OB_SUCCESS;
  if (part_key_pos > UINT8_MAX) {
    ret =OB_ERR_UNEXPECTED;
    LOG_WARN("Partition key position should not big than UINT8_MAX", K(ret), K(part_key_pos));
  } else {
    part_pos_.part_key_pos_ = static_cast<uint8_t>(part_key_pos);
    if (0 != part_pos_.part_key_pos_) {
      add_column_flag(TABLE_PART_KEY_COLUMN_FLAG);
    }
  }
  return ret;
}

int ObColumnSchemaV2::set_subpart_key_pos(const int64_t subpart_key_pos)
{
  int ret = OB_SUCCESS;
  if (subpart_key_pos > UINT8_MAX) {
    ret =OB_ERR_UNEXPECTED;
    LOG_WARN("Partition key position should not big than UINT8_MAX", K(ret), K(subpart_key_pos));
  } else {
    part_pos_.subpart_key_pos_ = static_cast<uint8_t>(subpart_key_pos);
    if (0 != part_pos_.subpart_key_pos_) {
      add_column_flag(TABLE_PART_KEY_COLUMN_FLAG);
    }
  }
  return ret;
}

void ObColumnSchemaV2::print_info() const
{
  LOG_INFO(
      "COLUMN:",
      K(tenant_id_),
      K(table_id_),
      K(column_id_),
      K(schema_version_),
      K(charset_type_),
      "collation" , meta_type_.get_collation_type(),
      "column_name", column_name_.ptr());
}

void ObColumnSchemaV2::print(FILE *fd) const
{
  if (OB_ISNULL(fd)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fd is NULL");
  } else {
    fprintf(fd, " column: %8ld %16ld %24.*s %16s %8d %8d %8d\n",
            column_id_, schema_version_,
            column_name_.length(), column_name_.ptr(),
            ob_obj_type_str(meta_type_.get_type()),
            accuracy_.get_length(), accuracy_.get_precision(), accuracy_.get_scale());
  }
}

int64_t ObColumnSchemaV2::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  J_OBJ_START();
  J_KV(K_(tenant_id),
    K_(table_id),
    K_(column_id),
    K_(schema_version),
    K_(rowkey_position),
    K_(index_position),
    K_(order_in_rowkey),
    K_(tbl_part_key_pos),
    K_(meta_type),
    K_(accuracy),
    K_(is_nullable),
    K_(is_zero_fill),
    K_(is_autoincrement),
    K_(is_hidden),
    K_(charset_type),
    K_(on_update_current_timestamp),
    K_(orig_default_value),
    K_(cur_default_value),
    K_(column_name),
    K_(comment),
    K_(column_flags),
    K_(extended_type_info),
    K_(prev_column_id),
    K_(next_column_id),
    K_(sequence_id),
    K_(encoding_type),
    K_(srs_id),
    K_(udt_set_id),
    K_(sub_type),
    KPC_(column_ref_idxs));
  J_OBJ_END();
  return pos;
}

int ObColumnSchemaV2::get_byte_length(
    int64_t &length,
    const bool is_oracle_mode,
    const bool for_check_length) const
{
  int ret = OB_SUCCESS;
  if (CS_TYPE_INVALID == get_collation_type()
      && !ob_is_extend(meta_type_.get_type())
      && !ob_is_user_defined_sql_type(meta_type_.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collation type is invalid", K(ret));
  } else if (ob_is_text_tc(meta_type_.get_type()) || ob_is_json(meta_type_.get_type())
             || ob_is_geometry(meta_type_.get_type())) {
    if (for_check_length) {
      // when check row length, a lob will occupy at most 2KB
      length = min(get_data_length(), OB_MAX_LOB_HANDLE_LENGTH);
    } else {
      length = get_data_length();
    }
  } else {
    const ObLengthSemantics length_semantic = is_oracle_mode ? get_length_semantics() : LS_CHAR;
    if (LS_CHAR == length_semantic) {
      int64_t mbmaxlen = 0;
      if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(get_collation_type(), mbmaxlen))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mbmaxlen", K(ret), K(get_collation_type()));
      } else {
        length = get_data_length() * mbmaxlen;
      }
    } else {
      length = get_data_length();
    }
  }
  return ret;
}

int ObColumnSchemaV2::alloc_column_ref_set()
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = alloc(sizeof(ColumnReferenceSet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate cascaded column set failed");
  } else {
    column_ref_idxs_ = new(ptr) ColumnReferenceSet();
  }
  return ret;
}

int ObColumnSchemaV2::add_cascaded_column_id(uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_ref_idxs_)) {
    if (OB_FAIL(alloc_column_ref_set())) {
      LOG_WARN("alloc column ref set failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(column_ref_idxs_->add_member(column_id - OB_APP_MIN_COLUMN_ID))) {
      LOG_WARN("add member to cascaded column idxs failed", K(ret));
    } else {
      LOG_DEBUG("succ to add_cascaded_column_id", K(ret), K(*this), K(column_id), K(lbt()));
    }
  }
  return ret;
}

int ObColumnSchemaV2::del_cascaded_column_id(const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_id < OB_APP_MIN_COLUMN_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column id", K(ret));
  } else if (OB_ISNULL(column_ref_idxs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column ref idxs", K(ret));
  } else if (OB_FAIL(column_ref_idxs_->del_member(column_id - OB_APP_MIN_COLUMN_ID))) {
    LOG_WARN("failed to delete cascaded column id", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

bool ObColumnSchemaV2::has_cascaded_column_id(uint64_t column_id) const
{
  bool bret = false;
  if (column_ref_idxs_ != NULL) {
    bret = column_ref_idxs_->has_member(column_id - OB_APP_MIN_COLUMN_ID);
  }
  return bret;
}

int ObColumnSchemaV2::get_cascaded_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  if (column_ref_idxs_ != NULL) {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ref_idxs_->bit_count(); ++i) {
      if (column_ref_idxs_->has_member(i)) {
        if (OB_FAIL(column_ids.push_back(i + OB_APP_MIN_COLUMN_ID))) {
          LOG_WARN("store column id failed", K(i));
        }
      }
    }
  }
  return ret;
}

int ObColumnSchemaV2::set_extended_type_info(const ObIArray<common::ObString> &info)
{
  return deep_copy_string_array(info, extended_type_info_);
}

int ObColumnSchemaV2::add_type_info(const ObString &type_info)
{
  return add_string_to_array(type_info, extended_type_info_);
}

int ObColumnSchemaV2::convert_column_id(const hash::ObHashMap<uint64_t, uint64_t> &column_id_map)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> old_column_ids;
  if (OB_FAIL(get_cascaded_column_ids(old_column_ids))) {
    LOG_WARN("failed to get cascaded column ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < old_column_ids.count(); i++) {
      const uint64_t old_column_id = old_column_ids.at(i);
      if (OB_FAIL(del_cascaded_column_id(old_column_id))) {
        LOG_WARN("failed to delete cascaded column id", K(ret), K(old_column_id));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < old_column_ids.count(); i++) {
      const uint64_t old_column_id = old_column_ids.at(i);
      uint64_t new_column_id = 0;
      if (OB_FAIL(column_id_map.get_refactored(old_column_id, new_column_id))) {
        LOG_WARN("failed to get new column id", K(ret));
      } else if (OB_FAIL(add_cascaded_column_id(new_column_id))) {
        LOG_WARN("failed to add cascaded column id", K(ret), K(old_column_id), K(new_column_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    const uint64_t old_column_id = get_column_id();
    uint64_t new_column_id = 0;
    if (OB_FAIL(column_id_map.get_refactored(old_column_id, new_column_id))) {
      LOG_WARN("failed to get new column id", K(ret), K(old_column_id));
    } else {
      set_column_id(new_column_id);
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    const uint64_t old_column_id = get_prev_column_id();
    uint64_t new_column_id = 0;
    if (old_column_id == BORDER_COLUMN_ID || old_column_id == UINT64_MAX) {
      // do nothing
    } else if (OB_FAIL(column_id_map.get_refactored(old_column_id, new_column_id))) {
      LOG_WARN("failed to get new prev column id", K(ret), K(old_column_id));
    } else {
      set_prev_column_id(new_column_id);
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    const uint64_t old_column_id = get_next_column_id();
    uint64_t new_column_id = 0;
    if (old_column_id == BORDER_COLUMN_ID || old_column_id == UINT64_MAX) {
      // do nothing
    } else if (OB_FAIL(column_id_map.get_refactored(old_column_id, new_column_id))) {
      LOG_WARN("failed to get new next column id", K(ret), K(old_column_id));
    } else {
      set_next_column_id(new_column_id);
    }
  }
  return ret;
}

int ObColumnSchemaV2::serialize_extended_type_info(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialize_string_array(buf, buf_len, pos, extended_type_info_))) {
    LOG_WARN("fail to serialize extended type info", K(ret));
  }
  return ret;
}

int ObColumnSchemaV2::deserialize_extended_type_info(const char *buf,
                                                      const int64_t data_len,
                                                      int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deserialize_string_array(buf, data_len, pos, extended_type_info_))) {
    LOG_WARN("fail to deserialize extended type info", K(ret));
  }
  return ret;
}

int ObColumnSchemaV2::set_geo_type(const int32_t type_val)
{
  int ret = OB_SUCCESS;
  switch (type_val) {
    case static_cast<int32_t>(common::ObGeoType::GEOMETRY): {
      set_geo_type(common::ObGeoType::GEOMETRY);
      break;
    }

    case static_cast<int32_t>(common::ObGeoType::POINT): {
      set_geo_type(common::ObGeoType::POINT);
      break;
    }

    case static_cast<int32_t>(common::ObGeoType::LINESTRING): {
      set_geo_type(common::ObGeoType::LINESTRING);
      break;
    }

    case static_cast<int32_t>(common::ObGeoType::POLYGON): {
      set_geo_type(common::ObGeoType::POLYGON);
      break;
    }

    case static_cast<int32_t>(common::ObGeoType::MULTIPOINT): {
      set_geo_type(common::ObGeoType::MULTIPOINT);
      break;
    }

    case static_cast<int32_t>(common::ObGeoType::MULTILINESTRING): {
      set_geo_type(common::ObGeoType::MULTILINESTRING);
      break;
    }

    case static_cast<int32_t>(common::ObGeoType::MULTIPOLYGON): {
      set_geo_type(common::ObGeoType::MULTIPOLYGON);
      break;
    }

    case static_cast<int32_t>(common::ObGeoType::GEOMETRYCOLLECTION): {
      set_geo_type(common::ObGeoType::GEOMETRYCOLLECTION);
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("undefined geometry type", K(ret), K(type_val));
      break;
    }
  }

  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
