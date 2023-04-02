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

#include "lib/number/ob_number_v2.h"
#include "share/schema/ob_column_schema.h"

#define VARIABLE_BUF_LEN 128

#define COMPARE_VALUE(obj_get_fun, medium_type, value, exist) \
{ \
  medium_type tmp_value = 0; \
  tmp_value = obj.obj_get_fun(); \
  exist = tmp_value == static_cast<medium_type>(value) ? true : false; \
  if(!exist){ \
    STORAGE_LOG(WARN, "value is different", K(tmp_value), K(value)); \
  } \
}

#define COMPARE_NUMBER(allocator, obj_get_fun, value, exist) \
{ \
  ObNumber number; \
  char *buf = NULL; \
  if(NULL == (buf = reinterpret_cast<char*>(allocator->alloc(VARIABLE_BUF_LEN)))){ \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    STORAGE_LOG(WARN, "fail to alloc memory"); \
  } else { \
    snprintf(buf, VARIABLE_BUF_LEN, "%ld", value); \
  } \
  if(OB_SUCC(ret)){ \
    if(OB_SUCCESS != (ret = number.from(buf, *allocator))){ \
      STORAGE_LOG(WARN, "fail to format num", K(ret)); \
    } else if(number != obj.obj_get_fun()){ \
      exist = false; \
      STORAGE_LOG(WARN, "row value is different", K(obj), K(number)); \
    } \
  } \
}

#define SET_VALUE(rowkey_pos, obj_set_fun, type, seed, value) \
{ \
  if (rowkey_pos > 0) { \
    obj.obj_set_fun(static_cast<type>(seed)); \
  } else { \
    obj.obj_set_fun(static_cast<type>(value)); \
  } \
}

#define SET_CHAR(allocator, rowkey_pos, obj_set_fun, seed, value) \
{ \
  char *buf = NULL; \
  if(NULL == (buf = reinterpret_cast<char*>(allocator->alloc(VARIABLE_BUF_LEN)))){ \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    STORAGE_LOG(WARN, "fail to alloc memory"); \
  } else if(rowkey_pos > 0){ \
    snprintf(buf, VARIABLE_BUF_LEN, "%064ld", seed); \
  } else { \
    snprintf(buf, VARIABLE_BUF_LEN, "%064ld", value); \
  } \
  if(OB_SUCC(ret)){ \
    ObString str; \
    str.assign_ptr(buf, static_cast<int32_t>(strlen(buf))); \
    obj.obj_set_fun(str); \
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI); \
  } \
}

#define SET_NUMBER(allocator, rowkey_pos, obj_set_fun, seed, value) \
{ \
  ObNumber number; \
  char *buf = NULL; \
  if(NULL == (buf = reinterpret_cast<char*>(allocator->alloc(VARIABLE_BUF_LEN)))){ \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    STORAGE_LOG(WARN, "fail to alloc memory"); \
  } else if(rowkey_pos > 0){ \
    snprintf(buf, VARIABLE_BUF_LEN, "%ld", seed); \
  } else { \
    snprintf(buf, VARIABLE_BUF_LEN, "%ld", value); \
  } \
  if(OB_SUCC(ret)){ \
    if(OB_SUCCESS != (ret = number.from(buf, *allocator))){ \
      STORAGE_LOG(WARN, "fail to format num", K(ret)); \
    } else { \
      obj.obj_set_fun(number); \
    } \
  } \
}

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace number;
namespace blocksstable
{

ObRowGenerate::ObRowGenerate()
  : allocator_(ObModIds::TEST)
  , p_allocator_(NULL)
  , schema_()
  , seed_(0)
  , is_multi_version_row_(false)
  , is_inited_(false)
  , is_reused_(false)
{

}

ObRowGenerate::~ObRowGenerate()
{

}

int ObRowGenerate::init(const share::schema::ObTableSchema &src_schema, const bool is_multi_version_row)
{
  int ret = OB_SUCCESS;
  if(is_inited_){
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "already inited");
  } else if (!src_schema.is_valid()) {
    STORAGE_LOG(WARN, "schema is invalid.", K(src_schema));
  } else if (is_multi_version_row) {
    if (OB_SUCCESS != (ret = src_schema.get_multi_version_column_descs(column_list_))) {
      STORAGE_LOG(WARN, "fail to get column ids.", K(ret));
    }
  } else {
    if (OB_SUCCESS != (ret = src_schema.get_column_ids(column_list_))) {
      STORAGE_LOG(WARN, "fail to get column ids.", K(ret));
    }
  }
  STORAGE_LOG(INFO, "init row gen", "column_count", column_list_.count(), K(column_list_));
  if (OB_SUCC(ret)) {
    is_multi_version_row_ = is_multi_version_row;
    p_allocator_ = &allocator_;
    is_inited_ = true;
    is_reused_ = true;
    if (OB_FAIL(schema_.assign(src_schema))) {
      STORAGE_LOG(WARN, "fail to assign schema", K(ret));
    } else {
      STORAGE_LOG(INFO, "init row gen", K(is_multi_version_row), K(is_multi_version_row_), "column_count", column_list_.count(), K(column_list_));
    }
  }
  return ret;
}

int ObRowGenerate::init(const share::schema::ObTableSchema &src_schema, ObArenaAllocator *allocator,
    const bool is_multi_version_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(src_schema, is_multi_version_row))) {
    STORAGE_LOG(WARN, "failed to init ObRowGenerate", K(ret));
  } else {
    p_allocator_ = allocator;
    is_reused_ = false;
  }
  return ret;
}

int ObRowGenerate::get_next_row(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    if (is_reused_) {
      p_allocator_->reuse();
    }
    if (OB_FAIL(generate_one_row(row, seed_))) {
      STORAGE_LOG(WARN, "fail to generate one row.", K(ret), K(row));
    } else {
      ++seed_;
    }
  }
  return ret;
}

int ObRowGenerate::get_next_row(const int64_t seed, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    if (is_reused_) {
      p_allocator_->reuse();
    }
    if (OB_FAIL(generate_one_row(row, seed))) {
      STORAGE_LOG(WARN, "fail to generate one row.", K(ret), K(row));
    }
  }
  return ret;
}
int ObRowGenerate::get_next_row(const int64_t seed,
                                const int64_t trans_version,
                                const ObDmlFlag dml_flag,
                                ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    if (is_reused_) {
      p_allocator_->reuse();
    }
    if (OB_FAIL(generate_one_row(row, seed, dml_flag, trans_version))) {
      STORAGE_LOG(WARN, "failed to generate_one_row", K(ret));
    }
  }
  return ret;
}

int ObRowGenerate::get_next_row(const int64_t seed,
                          const int64_t trans_version,
                          const ObDmlFlag dml_flag,
                          const bool is_compacted_row,
                          const bool is_last_row,
                          const bool is_first_row,
                          ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else if (OB_FAIL(get_next_row(seed, trans_version, dml_flag, row))) {
    STORAGE_LOG(WARN, "failed to get next row", K(ret));
  } else {
    row.mvcc_row_flag_.reset();
    if (is_compacted_row) {
      row.mvcc_row_flag_.set_compacted_multi_version_row(true);
    }
    if (is_last_row) {
      row.mvcc_row_flag_.set_last_multi_version_row(true);
    }
    if (is_first_row) {
      row.mvcc_row_flag_.set_first_multi_version_row(true);
    }
  }
  return ret;
}

int ObRowGenerate::get_next_row(
    const int64_t seed,
    const common::ObArray<uint64_t> &nop_column_idxs,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObRowGenerate has not been inited", K(ret));
  } else {
    if (is_reused_) {
      p_allocator_->reuse();
    }
    if (OB_FAIL(generate_one_row(row, seed))) {
      STORAGE_LOG(WARN, "fail to generate one row", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < nop_column_idxs.count(); ++i) {
        const uint64_t idx = nop_column_idxs.at(i);
        row.storage_datums_[idx].set_nop();
      }
    }
  }
  return ret;
}

// attention: collation type of objs generated is utf8
int ObRowGenerate::generate_one_row(ObDatumRow &row, const int64_t seed, const ObDmlFlag dml_flag, const int64_t trans_version)
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else if ((!is_multi_version_row_ && schema_.get_column_count() > row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(schema_.get_column_count()), K(row.count_));
  } else {
    ObObj obj;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_list_.count(); ++i) {
      const uint64_t column_id = column_list_.at(i).col_id_;
      ObObjType column_type = column_list_.at(i).col_type_.get_type();
      // ObCollationType column_collation_type = column_list_.at(i).col_type_.get_collation_type();
      if (OB_SUCCESS != (ret = set_obj(column_type, column_id, seed, obj, trans_version))) {
        STORAGE_LOG(WARN, "fail to set obj", K(ret), K(i), K(seed));
      } else if (OB_FAIL(row.storage_datums_[i].from_obj_enhance(obj))) {
        STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret), K(i), K(obj));
      }
    }
    row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    if (ObDmlFlag::DF_DELETE == dml_flag) {
      row.row_flag_.set_flag(ObDmlFlag::DF_DELETE);
    }
    row.count_ = column_list_.count();
  }
  return ret;
}

int ObRowGenerate::set_obj(const ObObjType &column_type,
                           const uint64_t column_id,
                           const int64_t seed,
                           ObObj &obj,
                           const int64_t trans_version)
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    int64_t rowkey_pos = 0;
    int64_t value = 0;
    bool init_flag = false;
    if (is_multi_version_row_) {
      if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_id) { // pos = 0 | value = -trans_version
        value = -trans_version;
        init_flag = true;
      } else if (OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column_id) { // pos = 0 | value = 0
        value = 0;
        init_flag = true;
      }
    }
    if (!init_flag) {
      rowkey_pos = schema_.get_column_schema(column_id)->get_rowkey_position();
      value = seed * column_type + column_id;
    }
    switch(column_type) {
    case ObNullType:
      if(rowkey_pos > 0){
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "ObNULLType should not be rowkey column");
      } else {
        obj.set_null();
      }
      break;
    case ObTinyIntType:
      SET_VALUE(rowkey_pos, set_tinyint, int8_t, seed, value);
      break;
    case ObSmallIntType:
      SET_VALUE(rowkey_pos, set_smallint, int16_t, seed, value);
      break;
    case ObMediumIntType:
      SET_VALUE(rowkey_pos, set_mediumint, int32_t, seed, value);
      break;
    case ObInt32Type:
      SET_VALUE(rowkey_pos, set_int32, int32_t, seed, value);
      break;
    case ObIntType:
      SET_VALUE(rowkey_pos, set_int, int64_t, seed, value);
      break;
    case ObUTinyIntType:
      SET_VALUE(rowkey_pos, set_utinyint, uint8_t, seed, value);
      break;
    case ObUSmallIntType:
      SET_VALUE(rowkey_pos, set_usmallint, uint16_t, seed, value);
      break;
    case ObUMediumIntType:
      SET_VALUE(rowkey_pos, set_umediumint, uint32_t, seed, value);
      break;
    case ObUInt32Type:
      SET_VALUE(rowkey_pos, set_uint32, uint32_t, seed, value);
      break;
    case ObUInt64Type:
      SET_VALUE(rowkey_pos, set_uint64, uint64_t, seed, value);
      break;
    case ObFloatType:
      SET_VALUE(rowkey_pos, set_float, float, seed, value);
      break;
    case ObUFloatType:
      SET_VALUE(rowkey_pos, set_ufloat, float, seed, value);
      break;
    case ObDoubleType:
      SET_VALUE(rowkey_pos, set_double, double, seed, value);
      break;
    case ObUDoubleType:
      SET_VALUE(rowkey_pos, set_udouble, double, seed, value);
      break;
    case ObNumberType: {
      SET_NUMBER(p_allocator_, rowkey_pos, set_number, seed, value);
      break;
    }
    case ObUNumberType: {
      SET_NUMBER(p_allocator_, rowkey_pos, set_unumber, seed, value);
      break;
    }
    case ObDateType:
      SET_VALUE(rowkey_pos, set_date, int32_t, seed, value);
      break;
    case ObDateTimeType:
      SET_VALUE(rowkey_pos, set_datetime, int64_t, seed, value);
      break;
    case ObTimestampType:
      SET_VALUE(rowkey_pos, set_timestamp, int64_t, seed, value);
      break;
    case ObTimeType:
      SET_VALUE(rowkey_pos, set_time, int64_t, seed, value);
      break;
    case ObYearType:
      SET_VALUE(rowkey_pos, set_year, uint8_t, seed, value);
      break;
    case ObVarcharType: {
      SET_CHAR(p_allocator_, rowkey_pos, set_varchar, seed, value);
      break;
    }
    case ObCharType: {
      SET_CHAR(p_allocator_, rowkey_pos, set_char, seed, value);
      break;
    }
    case ObRawType: {
      SET_CHAR(p_allocator_, rowkey_pos, set_raw, seed, value);
      break;
    }
    case ObEnumInnerType: {
      SET_CHAR(p_allocator_, rowkey_pos, set_enum_inner, seed, value);
      break;
    }
    case ObSetInnerType: {
      SET_CHAR(p_allocator_, rowkey_pos, set_set_inner, seed, value);
      break;
    }
    case ObNVarchar2Type: {
      SET_CHAR(p_allocator_, rowkey_pos, set_nvarchar2, seed, value);
      break;
    }
    case ObNCharType: {
      SET_CHAR(p_allocator_, rowkey_pos, set_nchar, seed, value);
      break;
    }
    case ObHexStringType: {
      char *buf = NULL;
      if(NULL == (buf = reinterpret_cast<char*>(p_allocator_->alloc(VARIABLE_BUF_LEN)))){
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory");
      } else if(rowkey_pos > 0){
        snprintf(buf, 128, "%0127ld", seed);//not change this
      } else {
        snprintf(buf, 128, "%0127ld", value);//not change this
      }
      if(OB_SUCC(ret)){
        ObString str;
        str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
        obj.set_hex_string(str);
      }
      //SET_CHAR(p_allocator_, rowkey_pos, set_hex_string, seed, value);
      break;
    }
    case ObExtendType:
      if(rowkey_pos > 0){
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "ObExtendType should not be rowkey column");
      } else {
        //TODO just set ObActionFlag::OP_NOP
        obj.set_nop_value();
      }
      break;
    case ObUnknownType:
      if (rowkey_pos > 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "ObUnknownType should not be rowkey column");
      } else {
        obj.set_unknown(seed);
      }
      break;
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: 
    case ObJsonType:
    case ObGeometryType: {
      ObLobCommon *value = NULL;
      void *buf = NULL;
      if (OB_ISNULL(buf = p_allocator_->alloc(sizeof(ObLobCommon) + 10))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to allocate memory for ObLobData", K(ret));
      } else {
        // ObLobIndex index;
        value = new (buf) ObLobCommon();
        int64_t byte_size = 10;
        if (column_type == ObTinyTextType) {
          byte_size = 2;
        }
        obj.meta_.set_collation_type(column_type == ObGeometryType ? CS_TYPE_BINARY
                                     :CS_TYPE_UTF8MB4_GENERAL_CI);
        obj.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
        obj.set_type(column_type);
        obj.set_lob_value(column_type, value, value->get_handle_size(byte_size));
      }
      break;
    }
    case ObBitType: {
    	SET_VALUE(rowkey_pos, set_bit, uint64_t, seed, value);
    	break;
    }
    case ObEnumType: {
    	SET_VALUE(rowkey_pos, set_enum, uint64_t, seed, value);
    	break;
    }
    case ObSetType: {
    	SET_VALUE(rowkey_pos, set_set, uint64_t, seed, value);
    	break;
    }
    case ObTimestampTZType: {
    	obj.set_otimestamp_value(ObTimestampTZType, value, static_cast<uint16_t>(12));
    	break;
    }
    case ObTimestampLTZType: {
    	obj.set_otimestamp_value(ObTimestampLTZType, value, static_cast<uint16_t>(12));
    	break;
    }
    case ObTimestampNanoType: {
    	obj.set_otimestamp_value(ObTimestampNanoType, value, static_cast<uint16_t>(12));
    	break;
    }
    case ObIntervalYMType: {
    	obj.set_interval_ym(ObIntervalYMValue(value));
    	break;
    }
    case ObIntervalDSType: {
  		obj.set_interval_ds(ObIntervalDSValue(value, 14));
  		break;
    }
    case ObNumberFloatType: {
      uint32_t digits = value;
  		obj.set_number_float(ObNumber(3, &digits));
  		break;
    }
    case ObURowIDType: {
      if (OB_FAIL(generate_urowid_obj(rowkey_pos, seed, value, obj))) {
        STORAGE_LOG(WARN, "fail to generate urowid obj");
      }
      break;
    }
    default:
      STORAGE_LOG(WARN, "not support this data type.", K(column_type));
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObRowGenerate::check_one_row(const ObDatumRow& row, bool &exist)
{
  int ret = OB_SUCCESS;
  int64_t seed = -1;

  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    //first get seed from rowkey
    ObObjType column_type;
    ObObjMeta obj_meta;
    int64_t pos = -1;
    uint64_t column_id = 0;
    ret = OB_ENTRY_NOT_EXIST;
    for(int64_t i = 0; i < column_list_.count(); ++ i){
      column_id = column_list_.at(i).col_id_;
      if(schema_.get_column_schema(column_id)->get_rowkey_position() > 0){
        column_type = schema_.get_column_schema(column_id)->get_data_type();
        obj_meta = schema_.get_column_schema(column_id)->get_meta_type();
        pos = i;
        ret = OB_SUCCESS;
        break;
      }
    }
    if(OB_SUCC(ret)){
      //ObObj obj = row.row_val_.cells_[column_id];
      ObObj obj;
      if (OB_FAIL(row.storage_datums_[pos].to_obj_enhance(obj, obj_meta))) {
        STORAGE_LOG(WARN, "Failed to transfer datum to obj", K(ret), K(pos), K(row.storage_datums_[pos]));
      } else if(OB_SUCCESS != (ret = get_seed(column_type, obj, seed))){
        STORAGE_LOG(WARN, "fail to get seed.", K(ret));
      }
    }
  }
  //second compare the value
  if(OB_SUCC(ret)){
    ObObjType column_type;
    ObObjMeta obj_meta;
    int64_t value = 0;
    uint64_t column_id = 0;
    for(int64_t i = 0; i < column_list_.count(); ++ i){
      column_id = column_list_.at(i).col_id_;
      if(0 == schema_.get_column_schema(column_id)->get_rowkey_position()){
        column_type = schema_.get_column_schema(column_id)->get_data_type();
        obj_meta = schema_.get_column_schema(column_id)->get_meta_type();
        value = seed * static_cast<uint8_t>(column_type) + column_id;
        ObObj obj;
        if (OB_FAIL(row.storage_datums_[i].to_obj_enhance(obj, obj_meta))) {
          STORAGE_LOG(WARN, "Failed to transfer datum to obj", K(ret), K(i), K(row.storage_datums_[i]));
        } else if(OB_SUCCESS != (ret = compare_obj(column_type, value, obj, exist))){
          STORAGE_LOG(WARN, "compare obobj error", K(ret));
        }
        if(!exist){
          break;
        }
      }
    }
  }
  return ret;
}

int ObRowGenerate::compare_obj(const ObObjType &column_type, const int64_t value, const ObObj obj, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = true;

  switch(column_type) {
  case ObNullType:
    break;
  case ObTinyIntType:
    COMPARE_VALUE(get_tinyint, int8_t, value, exist);
    break;
  case ObSmallIntType:
    COMPARE_VALUE(get_smallint, int16_t, value, exist);
    break;
  case ObMediumIntType:
    COMPARE_VALUE(get_mediumint, int32_t, value, exist);
    break;
  case ObInt32Type:
    COMPARE_VALUE(get_int32, int32_t, value, exist);
    break;
  case ObIntType:
    COMPARE_VALUE(get_int, int64_t, value, exist);
    break;
  case ObUTinyIntType:
    COMPARE_VALUE(get_utinyint, uint8_t, value, exist);
    break;
  case ObUSmallIntType:
    COMPARE_VALUE(get_usmallint, uint16_t, value, exist);
    break;
  case ObUMediumIntType:
    COMPARE_VALUE(get_umediumint, uint32_t, value, exist);
    break;
  case ObUInt32Type:
    COMPARE_VALUE(get_uint32, uint32_t, value, exist);
    break;
  case ObUInt64Type:
    COMPARE_VALUE(get_uint64, uint64_t, value, exist);
    break;
  case ObFloatType:
    COMPARE_VALUE(get_float, float, value, exist);
    break;
  case ObUFloatType:
    COMPARE_VALUE(get_ufloat, float, value, exist);
    break;
  case ObDoubleType:
    COMPARE_VALUE(get_double, double, value, exist);
    break;
  case ObUDoubleType:
    COMPARE_VALUE(get_udouble, double, value, exist);
    break;
  case ObNumberType:
    COMPARE_NUMBER(p_allocator_, get_number, value, exist);
    break;
  case ObUNumberType:
    COMPARE_NUMBER(p_allocator_, get_unumber, value, exist);
    break;
  case ObDateType:
    COMPARE_VALUE(get_date, int32_t, value, exist);
    break;
  case ObDateTimeType:
    COMPARE_VALUE(get_datetime, int64_t, value, exist);
    break;
  case ObTimestampType:
    COMPARE_VALUE(get_timestamp, int64_t, value, exist);
    break;
  case ObTimeType:
    COMPARE_VALUE(get_time, int64_t, value, exist);
    break;
  case ObYearType:
    COMPARE_VALUE(get_year, uint8_t, value, exist);
    break;
  case ObVarcharType:
  case ObCharType:
  case ObRawType:
  case ObNVarchar2Type:
  case ObNCharType:
  {
    char *buf = NULL;
    if(NULL == (buf = reinterpret_cast<char*>(p_allocator_->alloc(VARIABLE_BUF_LEN)))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory");
    } else {
      snprintf(buf, VARIABLE_BUF_LEN, "%064ld", value);
    }
    if(OB_SUCC(ret)){
      ObString str;
      str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
      if(str != obj.get_string()){
        exist = false;
        STORAGE_LOG(WARN, "row value is different", K(str), K(obj));
      }
    }
    break;
  }
  case ObHexStringType:
  {
    char *buf = NULL;
    if(NULL == (buf = reinterpret_cast<char*>(p_allocator_->alloc(VARIABLE_BUF_LEN)))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory");
    } else {
      snprintf(buf, VARIABLE_BUF_LEN, "%0127ld", value);//not change this
    }
    if(OB_SUCC(ret)){
      ObString str;
      str.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
      if(str != obj.get_string()){
        exist = false;
        STORAGE_LOG(WARN, "row value is different", K(str), K(obj));
      }
    }
    break;
  }
  case ObExtendType:
    //just check value to OP_NOP
    if(obj.get_ext() != ObActionFlag::OP_NOP){
      exist = false;
      STORAGE_LOG(WARN, "row value is different", K(obj.get_ext()), K(static_cast<int64_t>(value)));
    }
    break;
  case ObTinyTextType:
  case ObTextType:
  case ObMediumTextType:
  case ObLongTextType: 
  case ObJsonType:
  case ObGeometryType: {
    break;
  }
  case ObBitType: {
    COMPARE_VALUE(get_bit, uint64_t, value, exist);
    break;
  }
  case ObEnumType: {
    COMPARE_VALUE(get_enum, uint64_t, value, exist);
    break;
  }
  case ObSetType: {
    COMPARE_VALUE(get_set, uint64_t, value, exist);
    break;
  }
  case ObTimestampTZType:
  case ObTimestampLTZType:
  case ObTimestampNanoType: {
    exist = (value == obj.get_otimestamp_value().time_us_);
    break;
  }
  case ObIntervalYMType: {
    exist = (value == obj.get_interval_ym().nmonth_);
    break;
  }
  case ObIntervalDSType: {
    exist = (value == obj.get_interval_ds().nsecond_);
    break;
  }
  case ObNumberFloatType: {
    exist = (value == *obj.get_number_float().get_digits());
    break;
  }
  case ObURowIDType: {
    ObObj urowid_obj;
    int64_t rowkey_pos = 0;
    int64_t seed = 0;
    if (OB_FAIL(generate_urowid_obj(rowkey_pos, seed, value, urowid_obj))) {
      STORAGE_LOG(WARN, "fail to generate urowid obj", K(ret), K(value));
    } else {
      exist = (urowid_obj == obj);
    }
    break;
  }
  default:
    STORAGE_LOG(WARN, "don't support this data type.", K(column_type));
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObRowGenerate::get_seed(const ObObjType &column_type, const ObObj obj, int64_t &seed)
{
  int ret = OB_SUCCESS;
  switch(column_type) {
  case ObTinyIntType:
    seed = static_cast<int64_t>(obj.get_tinyint());
    break;
  case ObSmallIntType:
    seed = static_cast<int64_t>(obj.get_smallint());
    break;
  case ObMediumIntType:
    seed = static_cast<int64_t>(obj.get_mediumint());
    break;
  case ObInt32Type:
    seed = static_cast<int64_t>(obj.get_int32());
    break;
  case ObIntType:
    seed = obj.get_int();
    break;
  case ObUTinyIntType:
    seed = static_cast<int64_t>(obj.get_utinyint());
    break;
  case ObUSmallIntType:
    seed = static_cast<int64_t>(obj.get_usmallint());
    break;
  case ObUMediumIntType:
    seed = static_cast<int64_t>(obj.get_umediumint());
    break;
  case ObUInt32Type:
    seed = static_cast<int64_t>(obj.get_uint32());
    break;
  case ObUInt64Type:
    seed = static_cast<int64_t>(obj.get_uint64());
    break;
  case ObFloatType:
  case ObUFloatType:
    seed = static_cast<int64_t>(obj.get_float());
    break;
  case ObDoubleType:
  case ObUDoubleType:
    seed = static_cast<int64_t>(obj.get_double());
    break;
  case ObNumberType: {
    const char *value = obj.get_number().format();
    seed = static_cast<int64_t>(strtoll(value, NULL, 10));
    break;
  }
  case ObUNumberType: {
    const char *value = obj.get_unumber().format();
    seed = static_cast<int64_t>(strtoll(value, NULL, 10));
    break;
  }
  case ObDateType:
    seed = static_cast<int64_t>(obj.get_date());
    break;
  case ObDateTimeType:
    seed = static_cast<int64_t>(obj.get_datetime());
    break;
  case ObTimestampType:
    seed = static_cast<int64_t>(obj.get_timestamp());
    break;
  case ObTimeType:
    seed = static_cast<int64_t>(obj.get_time());
    break;
  case ObYearType:
    seed = static_cast<int64_t>(obj.get_year());
    break;
  case ObVarcharType:
  case ObCharType:
  case ObHexStringType:
  case ObRawType:
    seed = static_cast<int64_t>(strtoll(obj.get_string().ptr(), NULL, 10));
    break;
  case ObURowIDType: {
    ObURowIDData urowid_data;
    ObSEArray<ObObj, 2> obj_arr;
    if (OB_FAIL(obj.get_urowid(urowid_data))) {
      STORAGE_LOG(WARN, "fail to get urowid", K(ret), K(obj));
    } else if (OB_FAIL(urowid_data.get_pk_vals(obj_arr))) {
      STORAGE_LOG(WARN, "fail to get pk vals", K(ret), K(obj));
    } else if (OB_UNLIKELY(2 != obj_arr.count()) || OB_UNLIKELY(!obj_arr.at(0).is_int())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected obj count or obj type", K(ret), K(obj_arr));
    } else {
      seed = obj_arr.at(0).get_int();
    }
    break;
  }
  case ObExtendType:
  default:
    STORAGE_LOG(WARN, "don't support this data type.", K(column_type));
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObRowGenerate::generate_urowid_obj(const int64_t rowkey_pos,
                                       const int64_t seed,
                                       const int64_t value,
                                       ObObj &urowid_obj)
{
  int ret = OB_SUCCESS;
  int64_t real_value = rowkey_pos > 0 ? seed : value;
  ObObj int_obj;
  int_obj.set_int(real_value);
  ObObj str_obj;
  char *buf = NULL;
  if(OB_ISNULL(p_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "alloc is NULL");
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(p_allocator_->alloc(VARIABLE_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory");
  } else {
    snprintf(buf, VARIABLE_BUF_LEN, "%064ld", real_value);
    str_obj.set_varchar(buf, static_cast<int32_t>(strlen(buf)));
    str_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  if (OB_SUCC(ret)) {
    common::ObURowIDData urowid_data;
    ObSEArray<ObObj, 2> obj_arr;
    if (OB_FAIL(obj_arr.push_back(int_obj)) || OB_FAIL(obj_arr.push_back(str_obj))) {
      STORAGE_LOG(WARN, "fail to push back obj", K(ret), K(int_obj), K(str_obj));
    } else if (OB_FAIL(urowid_data.set_rowid_content(obj_arr,
                                                     ObURowIDData::PK_ROWID_VERSION,
                                                     *p_allocator_))) {
      STORAGE_LOG(WARN, "fail to setup urowid content", K(ret), K(obj_arr));
    } else {
      urowid_obj.set_urowid(urowid_data);
    }
  }
  return ret;
}
//TODO @hanhui to be removed
int ObRowGenerate::get_next_row(ObStoreRow &row)
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    if (is_reused_) {
      p_allocator_->reuse();
    }
    if (OB_FAIL(generate_one_row(row, seed_))) {
      STORAGE_LOG(WARN, "fail to generate one row.", K(ret), K(row));
    } else {
      ++seed_;
    }
  }
  return ret;
}

int ObRowGenerate::get_next_row(const int64_t seed, ObStoreRow &row)
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    if (is_reused_) {
      p_allocator_->reuse();
    }
    if (OB_FAIL(generate_one_row(row, seed))) {
      STORAGE_LOG(WARN, "fail to generate one row.", K(ret), K(row));
    }
  }
  return ret;
}
int ObRowGenerate::get_next_row(const int64_t seed,
                                const int64_t trans_version,
                                const ObDmlFlag dml_flag,
                                ObStoreRow &row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    if (is_reused_) {
      p_allocator_->reuse();
    }
    if (OB_FAIL(generate_one_row(row, seed, dml_flag, trans_version))) {
      STORAGE_LOG(WARN, "failed to generate_one_row", K(ret));
    }
  }
  return ret;
}

int ObRowGenerate::get_next_row(const int64_t seed,
                          const int64_t trans_version,
                          const ObDmlFlag dml_flag,
                          const bool is_compacted_row,
                          const bool is_last_row,
                          const bool is_first_row,
                          ObStoreRow &row)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else if (OB_FAIL(get_next_row(seed, trans_version, dml_flag, row))) {
    STORAGE_LOG(WARN, "failed to get next row", K(ret));
  } else {
    row.row_type_flag_.reset();
    if (is_compacted_row) {
      row.row_type_flag_.set_compacted_multi_version_row(true);
    }
    if (is_last_row) {
      row.row_type_flag_.set_last_multi_version_row(true);
    }
    if (is_first_row) {
      row.row_type_flag_.set_first_multi_version_row(true);
    }
  }
  return ret;
}

int ObRowGenerate::get_next_row(
    const int64_t seed,
    const common::ObArray<uint64_t> &nop_column_idxs,
    ObStoreRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObRowGenerate has not been inited", K(ret));
  } else {
    if (is_reused_) {
      p_allocator_->reuse();
    }
    if (OB_FAIL(generate_one_row(row, seed))) {
      STORAGE_LOG(WARN, "fail to generate one row", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < nop_column_idxs.count(); ++i) {
        const uint64_t idx = nop_column_idxs.at(i);
        row.row_val_.cells_[idx].set_nop_value();
      }
    }
  }
  return ret;
}

// attention: collation type of objs generated is utf8
int ObRowGenerate::generate_one_row(ObStoreRow &row, const int64_t seed, const ObDmlFlag dml_flag, const int64_t trans_version)
{
  int ret = OB_SUCCESS;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else if ((!is_multi_version_row_ && schema_.get_column_count() != row.row_val_.count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.",
        K(schema_.get_column_count()), K(row.row_val_.count_), K(row.column_ids_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_list_.count(); ++i) {
      const uint64_t column_id = column_list_.at(i).col_id_;
      ObObjType column_type = column_list_.at(i).col_type_.get_type();
      // ObCollationType column_collation_type = column_list_.at(i).col_type_.get_collation_type();
      if (OB_SUCCESS != (ret = set_obj(column_type, column_id, seed, row.row_val_.cells_[i], trans_version))) {
        STORAGE_LOG(WARN, "fail to set obj.", K(ret), K(i), K(seed));
      } else {
        // row.row_val_.cells_[i].set_collation_type(column_collation_type);
        if (ObTinyTextType == column_type || ObTextType == column_type || ObMediumTextType == column_type
            || ObLongTextType == column_type || ObNVarchar2Type == column_type || ObNCharType == column_type) {
          row.row_val_.cells_[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          row.row_val_.cells_[i].set_collation_level(CS_LEVEL_IMPLICIT);
        } else if (ObVarcharType == column_type || ObCharType == column_type || ob_is_text_tc(column_type)) {
          row.row_val_.cells_[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          row.row_val_.cells_[i].set_collation_level(CS_LEVEL_IMPLICIT);
        } else if (ObNullType == column_type) {
          row.row_val_.cells_[i].set_collation_type(CS_TYPE_BINARY);
          row.row_val_.cells_[i].set_collation_level(CS_LEVEL_IGNORABLE);
        } else {
          row.row_val_.cells_[i].set_collation_type(CS_TYPE_BINARY);
          row.row_val_.cells_[i].set_collation_level(CS_LEVEL_NUMERIC);
        }
      }
    }
    row.flag_.set_flag(ObDmlFlag::DF_INSERT);
    if (ObDmlFlag::DF_DELETE == dml_flag) {
      row.flag_.set_flag(ObDmlFlag::DF_DELETE);
    }
  }
  return ret;
}

int ObRowGenerate::check_one_row(const ObStoreRow& row, bool &exist)
{
  int ret = OB_SUCCESS;
  int64_t seed = -1;

  if(!is_inited_){
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    //first get seed from rowkey
    ObObjType column_type;
    int64_t pos = -1;
    uint64_t column_id = 0;
    ret = OB_ENTRY_NOT_EXIST;
    for(int64_t i = 0; i < column_list_.count(); ++ i){
      column_id = column_list_.at(i).col_id_;
      if(schema_.get_column_schema(column_id)->get_rowkey_position() > 0){
        column_type = schema_.get_column_schema(column_id)->get_data_type();
        pos = i;
        ret = OB_SUCCESS;
        break;
      }
    }
    if(OB_SUCC(ret)){
      //ObObj obj = row.row_val_.cells_[column_id];
      ObObj obj = row.row_val_.cells_[pos];
      if(OB_SUCCESS != (ret = get_seed(column_type, obj, seed))){
        STORAGE_LOG(WARN, "fail to get seed.", K(ret));
      }
    }
  }
  //second compare the value
  if(OB_SUCC(ret)){
    ObObjType column_type;
    int64_t value = 0;
    uint64_t column_id = 0;
    for(int64_t i = 0; i < column_list_.count(); ++ i){
      column_id = column_list_.at(i).col_id_;
      if(0 == schema_.get_column_schema(column_id)->get_rowkey_position()){
        column_type = schema_.get_column_schema(column_id)->get_data_type();
        value = seed * static_cast<uint8_t>(column_type) + column_id;
        if(OB_SUCCESS != (ret = compare_obj(column_type, value, row.row_val_.cells_[i], exist))){
          STORAGE_LOG(WARN, "compare obobj error", K(ret));
        }
        if(!exist){
          break;
        }
      }
    }
  }
  return ret;
}


}//blocksstable
}//oceanbase

