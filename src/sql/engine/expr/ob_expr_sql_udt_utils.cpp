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
 * This file is for implement of expr sql udt utils
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_sql_udt_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_user_type.h"
#include "src/pl/ob_pl_resolver.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
bool ObSqlUdtNullBitMap::is_valid()
{
  return (OB_NOT_NULL(bitmap_) && bitmap_len_ > 0);
}

int ObSqlUdtNullBitMap::check_bitmap_pos(uint32_t pos, bool &is_set)
{
  int ret = OB_SUCCESS;
  is_set = false;
  uint32 index = pos / 8;
  uint32 byte_index = pos % 8;
  if (index >= bitmap_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check udt bitmap overflow", K(ret), K(*this), K(index), K(byte_index), K(pos));
  } else {
    is_set = bitmap_[index] & (1 << byte_index);
  }
  return ret;
}

int ObSqlUdtNullBitMap::check_current_bitmap_pos(bool &is_set)
{
  return check_bitmap_pos(pos_, is_set);
}

int ObSqlUdtNullBitMap::set_bitmap_pos(uint32_t pos)
{
  int ret = OB_SUCCESS;
  uint32 index = pos / 8;
  uint32 byte_index = pos % 8;
  if (index >= bitmap_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set udt bitmap overflow", K(ret), K(*this), K(index), K(byte_index), K(pos));
  } else {
    bitmap_[index] |= (1 << byte_index);
  }
  return ret;
}

int ObSqlUdtNullBitMap::set_current_bitmap_pos()
{
  return set_bitmap_pos(pos_);
}

int ObSqlUdtNullBitMap::reset_bitmap_pos(uint32_t pos)
{
  int ret = OB_SUCCESS;
  uint32 index = pos / 8;
  uint32 byte_index = pos % 8;
  if (index >= bitmap_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reset udt bitmap overflow", K(ret), K(*this), K(index), K(byte_index), K(pos));
  } else {
    bitmap_[index] &= ~(1 << byte_index);
  }
  return ret;
}

int ObSqlUdtNullBitMap::reset_current_bitmap_pos()
{
  return reset_bitmap_pos(pos_);
}

int ObSqlUdtNullBitMap::assign(ObSqlUdtNullBitMap &src, uint32_t pos, uint32_t bit_len)
{
  int ret = OB_SUCCESS;
  uint32_t src_pos = src.get_pos();
  for (int i = 0; i < bit_len && OB_SUCC(ret); i++) {
    bool is_set = false;
    if (OB_FAIL(src.check_bitmap_pos(pos + i, is_set))) {
      LOG_WARN("failed to check nested udt bitmap", K(ret));
    } else if (is_set && OB_FAIL(set_current_bitmap_pos())) {
      LOG_WARN("failed to set nested udt bitmap", K(ret));
    } else {
      get_pos()++;
    }
  }
  src.set_bitmap_pos(src_pos);
  return ret;
}

int ObSqlUdtUtils::ob_udt_flattern_varray(const ObObj **flattern_objs,
                                          const int32_t &flattern_object_count,
                                          int32_t &pos,
                                          const ObObj *obj,
                                          ObExecContext &exec_context,
                                          ObSqlUDT &sql_udt)
{
  int ret = OB_SUCCESS;
  flattern_objs[pos++] = obj; // varray in sql udt is a single obj
  return ret;
}

int ObSqlUdtUtils::ob_udt_flattern_record(const ObObj **flattern_objs,
                                          const int32_t &flattern_object_count,
                                          int32_t &pos,
                                          ObSqlUdtNullBitMap &nested_udt_bitmap,
                                          const ObObj *obj,
                                          ObExecContext &exec_context,
                                          ObSqlUDT &sql_udt) {
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  bool is_null_result = false;
  pl::ObPLRecord *record = NULL;
  if (OB_ISNULL(obj) || obj->get_ext() == 0) {
    is_null_result = true;
  } else if (FALSE_IT(record = reinterpret_cast<pl::ObPLRecord *>(obj->get_ext()))) {
  } else if (record->is_null()) {
    is_null_result = true;
  } else {
    const ObObj *elements = record->get_element();
    const ObObj *cur_element = NULL;
    ObSqlUDTAttrMeta *&attrs = sql_udt.get_udt_meta().child_attrs_meta_;
    if (OB_ISNULL(attrs)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("Unexpected NULL toplevel attributes meta", K(ret));
    } else {
      uint32_t toplevel_attrs_count = sql_udt.get_udt_meta().child_attr_cnt_;
      for (int32_t i = 0; OB_SUCC(ret) && i < toplevel_attrs_count; i++) {
        ObObjType element_type = attrs[i].type_info_.get_type();
        cur_element = &elements[i];
        if (element_type >= ObMaxType) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected record element type", K(ret), K(element_type));
        } else if (ob_is_user_defined_sql_type(element_type) || // nested udt type
                   ob_is_collection_sql_type(element_type)) {
          const uint16_t subschema_id = attrs[i].type_info_.get_subschema_id();
          ObSqlUDTMeta udt_meta;
          ObSqlUDT sql_udt;
          if (OB_FAIL(exec_context.get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
            LOG_WARN("failed to get udt meta", K(ret), K(subschema_id));
          } else if (udt_meta.udt_id_ == T_OBJ_XML) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("nested xmltype not supported", K(ret), K(udt_meta.udt_id_));
          } else if (!ObObjUDTUtil::ob_is_supported_sql_udt(udt_meta.udt_id_)) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("inconsistent datatypes", K(ret), K(subschema_id), K(udt_meta.udt_id_));
          } else {
            sql_udt.set_udt_meta(udt_meta);
            int32_t element_pos = 0; // Notice: udt leaf is flattern
            if (OB_FAIL(ob_udt_flattern_pl_extend(&flattern_objs[pos],
                                                  udt_meta.leaf_attr_cnt_,
                                                  element_pos,
                                                  nested_udt_bitmap,
                                                  cur_element,
                                                  exec_context,
                                                  sql_udt))) {
              LOG_WARN("failed to flattern nested record", K(ret), K(subschema_id), K(udt_meta.udt_id_));
            } else {
              pos += element_pos;
            }
          }
        } else if (cur_element == NULL || cur_element->is_null()) { // basic type with null value
          flattern_objs[pos] = NULL;
          pos++;
        } else if (!ob_udt_util_check_same_type(element_type, cur_element->get_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("record element type mismarch", K(ret), K(element_type), K(cur_element->get_type()));
        } else {
          flattern_objs[pos] = cur_element;
          pos++;
        }
      }
    }
  }

  if (OB_SUCC(ret) && is_null_result) {
    // Notice: 1. null extend data is null UDT; 2. bitmap pos is 1 + nested_udt_number_
    for (int32_t i = 0; OB_SUCC(ret) && i <= sql_udt.get_udt_meta().nested_udt_number_; i++) {
      if (OB_FAIL(nested_udt_bitmap.set_current_bitmap_pos())) {
        LOG_WARN("failed to set nested udt bitmap", K(ret));
      } else {
        nested_udt_bitmap.get_pos()++;
      }
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < sql_udt.get_udt_meta().attribute_cnt_; i++) {
      if (pos >= flattern_object_count) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("flatter objs number error", K(ret), K(pos), K(flattern_object_count));
      } else {
        flattern_objs[pos] = NULL;
        pos++;
      }
    }
  }
#endif
  return ret;
}

int ObSqlUdtUtils::ob_udt_flattern_pl_extend(const ObObj **flattern_objs,
                                             const int32_t &flattern_object_count,
                                             int32_t &pos,
                                             ObSqlUdtNullBitMap &nested_udt_bitmap,
                                             const ObObj *obj,
                                             ObExecContext &exec_context,
                                             ObSqlUDT &sql_udt)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  if (sql_udt.get_udt_meta().udt_id_ == T_OBJ_XML) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nested xmltype not supported", K(ret));
  } else if (sql_udt.get_udt_meta().pl_type_ == pl::PL_RECORD_TYPE) {
    if (OB_FAIL(ob_udt_flattern_record(flattern_objs, flattern_object_count, pos,
                                       nested_udt_bitmap,
                                       obj, exec_context, sql_udt))) {
      LOG_WARN("failed to cast sql record to pl record", K(ret), K(sql_udt.get_udt_meta().udt_id_));
    }
  } else if (sql_udt.get_udt_meta().pl_type_ == pl::PL_VARRAY_TYPE) {
    if (OB_FAIL(ob_udt_flattern_varray(flattern_objs, flattern_object_count, pos,
                                       obj, exec_context, sql_udt))) {
      LOG_WARN("failed to cast sql varray to pl varrayd",
               K(ret), K(sql_udt.get_udt_meta().udt_id_));
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent datatypes", K(ret), K(sql_udt.get_udt_meta().udt_id_));
  }
#endif
  return ret;
}

int ObSqlUdtUtils::ob_udt_reordering_leaf_objects(const ObObj **flattern_objs,
                                                  const ObObj **sorted_objs,
                                                  const int32_t &flattern_object_count,
                                                  ObSqlUDT &sql_udt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < flattern_object_count; i++) {
    int64_t index = sql_udt.get_udt_meta().leaf_attrs_meta_[i].order_;
    sorted_objs[i] = flattern_objs[index];
  }
  return ret;
}

int ObSqlUdtUtils::ob_udt_calc_sql_varray_length(const ObObj *cur_obj,
                                                 int64_t &sql_udt_total_len,
                                                 bool with_lob_header)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  uint32_t count = 0;
  uint32_t varray_len = 0;
  pl::ObPLVArray *varray = reinterpret_cast<pl::ObPLVArray *>(cur_obj->get_ext());
  if (OB_ISNULL(varray) || OB_ISNULL(varray->get_data())) {
    // empty array, do nothing
  } else {
    count = varray->get_count();

    // count length
    varray_len += sizeof(uint32);

    // null count
    varray_len += sizeof(uint32);

    // null bitmap length
    varray_len += ObSqlUDT::get_offset_array_len(count);

    // offset array length
    varray_len += ObSqlUDT::get_null_bitmap_len(count);

    // elements length
    const ObObj *cur_obj = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      cur_obj = &varray->get_data()[i];
      if (cur_obj == NULL || cur_obj->is_null()) {
        // do nothing
      } else if (cur_obj->is_ext() || cur_obj->is_user_defined_sql_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("varray with udt type is not supported", K(ret), K(cur_obj));
      } else { // serialize basic types
        int64_t val_len = 0;
        if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_get_serialize_size(*cur_obj, val_len))) {
          LOG_WARN("Failed to calc object val length", K(ret), K(*cur_obj));
        } else {
          varray_len += val_len;
        }
      }
    }

    // varray is a lob, calc lob locator len
    int64_t templob_len = 0;
    if (OB_FAIL(ret)) {
    } else if (!with_lob_header) {
      sql_udt_total_len += varray_len;
    } else {
      if (OB_FAIL(ObTextStringResult::calc_inrow_templob_len(varray_len, templob_len))) {
        LOG_WARN("calc sql varray lob length failed", K(ret), K(*cur_obj));
      } else {
        sql_udt_total_len += templob_len;
      }
    }
  }
#endif
  return ret;
}

int ObSqlUdtUtils::ob_udt_calc_total_len(const ObObj **sorted_objs,
                                         const int32_t &flattern_object_count,
                                         int64_t &sql_udt_total_len,
                                         ObSqlUDT &sql_udt)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  if (OB_ISNULL(sorted_objs) || flattern_object_count == 0) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("null objs", K(ret), KP(sorted_objs), K(flattern_object_count));
  } else {
    int64_t leaf_attr_count = sql_udt.get_udt_meta().leaf_attr_cnt_ + 1;
    if (sql_udt.get_udt_meta().pl_type_ == pl::PL_RECORD_TYPE) {
      sql_udt_total_len += sql_udt.get_null_bitmap_len(leaf_attr_count);
      sql_udt_total_len += sql_udt.get_offset_array_len(leaf_attr_count);
    }
    const ObObj *cur_obj = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < flattern_object_count; i++) {
      cur_obj = sorted_objs[i];
      if (cur_obj == NULL) {
      } else if (cur_obj->is_ext()) {
        pl::ObPLType pl_type = static_cast<pl::ObPLType>(cur_obj->get_meta().get_extend_type());
        if (pl_type == pl::PL_VARRAY_TYPE) {
          if (OB_FAIL(ob_udt_calc_sql_varray_length(cur_obj, sql_udt_total_len))) {
            LOG_WARN("Failed to calc sql varray length", K(ret), K(i), K(pl_type));
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Unexpected supported pl type", K(ret), K(i), K(pl_type));
        }
      } else { // serialize basic types
        int64_t val_len = 0;
        if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_get_serialize_size(*cur_obj, val_len))) {
          LOG_WARN("Failed to calc object val length", K(ret), K(*cur_obj));
        } else {
          sql_udt_total_len += val_len;
        }
      }
    }
  }
#endif
  return ret;
}

int ObSqlUdtUtils::ob_udt_convert_pl_varray_to_sql_varray(const ObObj *cur_obj,
                                                          char *buf,
                                                          const int64_t buf_len,
                                                          int64_t &pos,
                                                          bool with_lob_header)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  uint32_t count = 0;
  pl::ObPLVArray *varray = reinterpret_cast<pl::ObPLVArray *>(cur_obj->get_ext());
  if (OB_ISNULL(varray) || OB_ISNULL(varray->get_data())) {
    // empty array, do nothing
  } else {
    int64_t locator_header_offset = pos;
    int64_t locator_header_len = with_lob_header
                                 ? ObTextStringResult::calc_inrow_templob_locator_len()
                                 : 0;

    // reserve length for templob header;
    pos += locator_header_len;

    // related_start for calculating array element offset
    int64_t related_start = pos;

    // encoding count
    count = varray->get_count();
    *(reinterpret_cast<uint32_t *>(&buf[pos])) = count;

    // null count
    pos += sizeof(uint32_t);
    uint32_t null_count_offset = pos;
    *(reinterpret_cast<uint32_t *>(&buf[pos])) = 0;

    // bitmap
    pos += sizeof(uint32_t);
    char *leaf_bitmap_ptr = buf + pos;
    uint32_t leaf_bitmap_len = ObSqlUDT::get_null_bitmap_len(count);
    MEMSET(leaf_bitmap_ptr, 0, leaf_bitmap_len);

    // reserve bitmap
    pos += leaf_bitmap_len;
    uint32_t *offset_ptr = reinterpret_cast<uint32_t *>(buf + pos);

    // reserve offset array
    pos += ObSqlUDT::get_offset_array_len(count);

    // elements length
    const ObObj *cur_obj = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      cur_obj = &varray->get_data()[i];
      if (cur_obj == NULL|| cur_obj->is_null()) {
        offset_ptr[i] = pos - related_start;
        if (OB_FAIL(ObSqlUDT::set_null_bitmap_pos(leaf_bitmap_ptr, leaf_bitmap_len, i))) {
          LOG_WARN("Failed to set varray element null bitmap",
                   K(ret), K(leaf_bitmap_ptr), K(leaf_bitmap_len), K(i));
        } else {
          ObSqlUDT::increase_varray_null_count(buf + null_count_offset);
        }
      } else if (cur_obj->is_ext() || cur_obj->is_user_defined_sql_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("varray with udt type is not supported", K(ret), K(cur_obj));
      } else { // serialize basic types
        uint64_t offset = pos - related_start;
        if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_serialize(*cur_obj, buf, buf_len, pos))) {
          LOG_WARN("Failed to serialize object value", K(ret), K(*cur_obj));
        } else {
          offset_ptr[i] = offset;
        }
      }
    }

    if (with_lob_header) {
      int64_t templob_inrow_data_len = pos - locator_header_len - locator_header_offset;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTextStringResult::fill_inrow_templob_header(templob_inrow_data_len,
                                                                      buf + locator_header_offset,
                                                                      pos - locator_header_offset))) {
        LOG_WARN("Failed to set templob header for sql varray", K(ret), K(*cur_obj));
      }
    }
  }
#endif
  return ret;
}

// convert pl extend to sql udt by type
int ObSqlUdtUtils::ob_udt_convert_sorted_objs_array_to_udf_format(const ObObj **sorted_objs,
                                                                  char *buf,
                                                                  const int64_t buf_len,
                                                                  int64_t &pos,
                                                                  ObSqlUDT &sql_udt)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  if (OB_ISNULL(sorted_objs)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("Unexpected NULL element", K(ret));
  } else { // record
    int64_t leaf_attr_count = sql_udt.get_udt_meta().leaf_attr_cnt_ + 1;
    char *leaf_bitmap_ptr = buf + pos;
    uint32_t leaf_bitmap_len = ObSqlUDT::get_null_bitmap_len(leaf_attr_count);
    MEMSET(leaf_bitmap_ptr, 0, leaf_bitmap_len);

    pos += sql_udt.get_null_bitmap_len(leaf_attr_count);
    uint32_t *offset_ptr = reinterpret_cast<uint32_t *>(buf + pos);

    pos += sql_udt.get_offset_array_len(leaf_attr_count);

    if (pos > buf_len) { // pos could equal to buf_len, when all elements are NULL
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Buffer overflow", K(ret), K(pos), K(buf_len));
    }

    const ObObj *cur_element = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < leaf_attr_count; i++) {
      cur_element = sorted_objs[i];
      ObObjType cur_element_type = ObMaxType;
      offset_ptr[i] = 0;
      if (OB_ISNULL(cur_element)) {
        offset_ptr[i] = pos;
        if (OB_FAIL(ObSqlUDT::set_null_bitmap_pos(leaf_bitmap_ptr, leaf_bitmap_len, i))) {
          LOG_WARN("Failed to set varray element null bitmap",
                   K(ret), K(leaf_bitmap_ptr), K(leaf_bitmap_len), K(i));
        }
      } else if (FALSE_IT(cur_element_type = cur_element->get_type())) {
      } else if (cur_element_type == ObUserDefinedSQLType
                 || cur_element_type == ObCollectionSQLType
                 || cur_element_type >= ObMaxType) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected record element type", K(ret), K(cur_element_type));
      } else if (cur_element_type == ObExtendType) {
        pl::ObPLType pl_type = static_cast<pl::ObPLType>(cur_element->get_meta().get_extend_type());
        if (pl_type == pl::PL_VARRAY_TYPE) {
          uint64_t offset = pos;
          if (OB_FAIL(ob_udt_convert_pl_varray_to_sql_varray(cur_element, buf, buf_len, pos))) {
            LOG_WARN("convert pl varray to sql varray failed", K(ret), K(i), K(pl_type));
          } else {
            offset_ptr[i] = offset;
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Unexpected supported pl type", K(ret), K(i), K(pl_type));
        }
      } else {
        // serialize basic types
        uint64_t offset = pos;
        if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_serialize(*cur_element, buf, buf_len, pos))) {
          LOG_WARN("Failed to serialize object value", K(ret), K(*cur_element));
        } else {
          offset_ptr[i] = offset;
        }
      }
    }

    if (OB_SUCC(ret) && (pos != buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql udt content length error", K(ret), K(pos), K(buf_len));
    }
  }
#endif
  return ret;
}

// convert sql_udt to string by type
int ObSqlUdtUtils::covert_sql_udt_varray_to_string(ObStringBuffer &buf,
                                                   ObString &udt_varray_buf,
                                                   ObSqlUDTMeta &root_meta)
{
  int ret = OB_SUCCESS;
  ObSqlUDT varray_handler;
  varray_handler.set_udt_meta(root_meta);
  ObString varray_data = udt_varray_buf;
  ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(&lob_allocator,
                                                        ObLongTextType,
                                                        CS_TYPE_BINARY,
                                                        true, varray_data))) {
    LOG_WARN("fail to get real string data", K(ret), K(varray_data));
  } else if (FALSE_IT(varray_handler.set_data(varray_data))) {
  } else if (varray_data.empty()) {
    if (OB_FAIL(buf.append("NULL"))) {
      LOG_WARN("fail to print empty value", K(ret));
    }
  } else if (OB_FAIL(buf.append(varray_handler.get_udt_meta().udt_name_,
                                varray_handler.get_udt_meta().udt_name_len_))) {
    LOG_WARN("fail to print varray name start", K(ret));
  } else if (OB_FAIL(buf.append("("))){
    LOG_WARN("fail to print (", K(ret));
  } else {
    uint64_t element_count = varray_handler.get_varray_element_count();
    ObString ser_element_data;
    char number_buff[256];
    for (int64_t i = 0; OB_SUCC(ret) && i < element_count; i++) {
      if (OB_FAIL(varray_handler.access_attribute(i, ser_element_data, true))) {
        LOG_WARN("fail to access attribute buffer", K(ret));
      } else if (ser_element_data.empty()) {
        if (OB_FAIL(buf.append("NULL"))) {
          LOG_WARN("fail to print empty value", K(ret));
        }
      } else {
        ObObj obj;
        obj.set_meta_type(varray_handler.get_udt_meta().child_attrs_meta_[0].type_info_);
        int64_t pos = 0;
        if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(obj,
                                                               ser_element_data.ptr(),
                                                               ser_element_data.length(),
                                                               pos))) {
          LOG_WARN("Failed to serialize object value", K(ret), K(ser_element_data));
        } else {
          // ToDo: different types
          if (obj.get_type_class() == ObNumberTC) {
            int64_t str_len = 0;
            const number::ObNumber &val = obj.get_number();
            int16_t scale = obj.get_scale();
            if (OB_FAIL(val.format(number_buff, sizeof(number_buff), str_len, scale))) {
              LOG_WARN("fail to format number with oracle limit", K(ret), K(str_len), K(scale));
            } else if (OB_FAIL(buf.append(number_buff, str_len))) {
              LOG_WARN("fail to print number value", K(ret), K(val));
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unsupported udt element type", K(ret), K(obj));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if ((i < element_count - 1) && OB_FAIL(buf.append(", "))) {
          LOG_WARN("fail to print ,", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(buf.append(")"))) {
      LOG_WARN("fail to print )", K(ret));
    }
  }
  return ret;
}

int ObSqlUdtUtils::convert_sql_udt_attributes_to_string(ObStringBuffer &buf,
                                                        ObString *attrs,
                                                        int64_t &pos,
                                                        sql::ObExecContext *exec_context,
                                                        ObSqlUDTMeta &root_meta,
                                                        ObSqlUdtNullBitMap &nested_udt_bitmap)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  if (OB_FAIL(buf.append(root_meta.udt_name_, root_meta.udt_name_len_))) {
    LOG_WARN("fail to print point type start", K(ret));
  } else if (OB_FAIL(buf.append("("))){
    LOG_WARN("fail to print (", K(ret));
  } else {
    ObObj obj;
    char number_buff[256];
    for (int64_t i = 0; OB_SUCC(ret) && i < root_meta.child_attr_cnt_; i++) {
      ObObjType type = root_meta.child_attrs_meta_[i].type_info_.get_type();
      obj.reset();
      obj.set_meta_type(root_meta.child_attrs_meta_[i].type_info_);
      bool is_nested_record = false;
      if (type == ObUserDefinedSQLType ||
          (type == ObCollectionSQLType && root_meta.child_attr_cnt_ > 1)) {
        const uint16_t subschema_id = obj.get_meta().get_subschema_id();
        ObSqlUDTMeta udt_meta;
        // should be varray or record
        bool is_udt_null = false;
        if (OB_FAIL(exec_context->get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
          LOG_WARN("failed to get udt meta", K(ret), K(subschema_id));
        } else if (nested_udt_bitmap.is_valid()
                   && OB_FAIL(nested_udt_bitmap.check_current_bitmap_pos(is_udt_null))) {
           LOG_WARN("failed to get nested udt null bit", K(ret), K(nested_udt_bitmap));
        } else if (is_udt_null) {
          if (OB_FAIL(buf.append("NULL"))) {
            LOG_WARN("fail to print empty value", K(ret));
          } else {
            pos += udt_meta.leaf_attr_cnt_;
            nested_udt_bitmap.get_pos() += (udt_meta.nested_udt_number_ + 1); // count the nested udt itself
            is_nested_record = true;
          }
        } else if (udt_meta.pl_type_ == pl::PL_VARRAY_TYPE) {
          if (OB_FAIL(covert_sql_udt_varray_to_string(buf,
                                                      attrs[pos],
                                                      udt_meta))) {
            LOG_WARN("failed to convert sql udt varray to string failed",
                     K(ret), K(i), K(pos), K(subschema_id));
          }
        } else if (udt_meta.pl_type_ == pl::PL_RECORD_TYPE) {
          if (OB_FAIL(convert_sql_udt_attributes_to_string(buf,
                                                           attrs,
                                                           pos,
                                                           exec_context,
                                                           udt_meta,
                                                           nested_udt_bitmap))) {
            LOG_WARN("failed to convert to string", K(ret), K(udt_meta.pl_type_));
          }
          is_nested_record = true;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected type to convert pl udt to sql udt format", K(ret), K(udt_meta.pl_type_));
        }
      } else { // basic types
        int64_t buf_pos = 0;
        if (OB_ISNULL(attrs[pos])) {
          if (OB_FAIL(buf.append("NULL"))) {
            LOG_WARN("fail to print empty value", K(ret));
          }
        } else if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(obj,
                                                                      attrs[pos].ptr(),
                                                                      attrs[pos].length(),
                                                                      buf_pos))) {
          LOG_WARN("Failed to serialize object value", K(ret), K(attrs[pos]));
        } else {
          // ToDo: @gehao, different basic types
          int64_t str_len = 0;
          const number::ObNumber &val = obj.get_number();
          int16_t scale = obj.get_scale();
          if (OB_FAIL(val.format(number_buff, sizeof(number_buff), str_len, scale))) {
            LOG_WARN("fail to format number with oracle limit", K(ret), K(str_len), K(scale));
          } else if (OB_FAIL(buf.append(number_buff, str_len))) {
            LOG_WARN("fail to print number value", K(ret), K(val));
          }
        }
      }
      if (OB_SUCC(ret)) { // Notice: not use pos for ',' and ')'
        if ((i < root_meta.child_attr_cnt_ - 1) && OB_FAIL(buf.append(", "))) {
          LOG_WARN("fail to print ,", K(ret));
        } else if ((i == root_meta.child_attr_cnt_ - 1) && OB_FAIL(buf.append(")"))) {
          LOG_WARN("fail to print )", K(ret));
        } else {
          pos += (is_nested_record ? 0 : 1);
        }
      }
    }
  }
#endif
  return ret;
}

int ObSqlUdtUtils::rearrange_sql_udt_record(ObString &sql_udt_data,
                                            ObSqlUDTMeta udt_meta,
                                            common::ObIAllocator *allocator,
                                            ObSqlUdtNullBitMap &nested_udt_bitmap,
                                            ObString *&attrs,
                                            bool &is_null)
{
  int ret = OB_SUCCESS;
  ObString udt_data = sql_udt_data;
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, ObLongTextType, CS_TYPE_BINARY, true, udt_data))) {
    LOG_WARN("fail to get real string data", K(ret), K(udt_data));
  } else {
    ObSqlUDT sql_udt;
    sql_udt.set_udt_meta(udt_meta);
    sql_udt.set_data(udt_data);

    ObString *temp_leaf_attrs = NULL;
    ObString *leaf_attrs = NULL;
    ObSqlUDTAttrMeta *top_attrs_meta = udt_meta.child_attrs_meta_;
    ObSqlUDTAttrMeta *leaf_attrs_meta = udt_meta.leaf_attrs_meta_;
    int64_t leaf_attr_count = udt_meta.leaf_attr_cnt_;

    if (OB_ISNULL(top_attrs_meta) || OB_ISNULL(leaf_attrs_meta) || (leaf_attr_count == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid udt meta", K(ret), KP(top_attrs_meta), KP(leaf_attrs_meta), K(leaf_attr_count));
    } else {
      uint64_t alloc_len = sizeof(ObString) * leaf_attr_count;
      if (OB_ISNULL(temp_leaf_attrs = reinterpret_cast<ObString *>(allocator->alloc(alloc_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for attributes", K(ret), K(alloc_len));
      } else if (OB_ISNULL(leaf_attrs = reinterpret_cast<ObString *>(allocator->alloc(alloc_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for rearranged attributes", K(ret), K(alloc_len));
      } else {
        ObString attribute_data;
        bool root_udt_is_null = false;
        ObObj udt_null_bitmap;
        udt_null_bitmap.reset();
        udt_null_bitmap.set_type(ObHexStringType);
        int64_t buf_pos = 0;

        // get first attribute(nested udt null bitmpa)
        if (OB_FAIL(sql_udt.access_attribute(0, attribute_data))) {
          LOG_WARN("fail to access null bitmap attribute", K(ret));
        } else if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(udt_null_bitmap,
                                                                      attribute_data.ptr(),
                                                                      attribute_data.length(),
                                                                      buf_pos))) {
          LOG_WARN("Failed to serialize object value", K(ret), K(attribute_data));
        } else {
          nested_udt_bitmap.set_bitmap(udt_null_bitmap.get_string().ptr(),
                                       udt_null_bitmap.get_string().length());
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(nested_udt_bitmap.check_bitmap_pos(0, root_udt_is_null))) {
          LOG_WARN("Failed to get root null bit", K(ret));
        } else if (root_udt_is_null) {
          is_null = root_udt_is_null;
        } else {
          nested_udt_bitmap.get_pos()++;
          // flattern all attribuites
          for (int64_t i = 1; OB_SUCC(ret) && i < leaf_attr_count + 1; i++) {
            if (OB_FAIL(sql_udt.access_attribute(i, attribute_data))) {
              LOG_WARN("fail to access attribute buffer", K(ret));
            } else {
              temp_leaf_attrs[i - 1] = attribute_data;
            }
          }
          // rearrange attributes to original order
          for (int64_t i = 0; OB_SUCC(ret) && i < leaf_attr_count; i++) {
            leaf_attrs[i] = temp_leaf_attrs[leaf_attrs_meta[i].order_];
          }
        }
      }
      if (OB_SUCC(ret)) {
        attrs = leaf_attrs;
      }
    }
  }
  return ret;
}

int ObSqlUdtUtils::convert_sql_udt_to_string(ObObj &sql_udt_obj,
                                             common::ObIAllocator *allocator,
                                             sql::ObExecContext *exec_context,
                                             ObSqlUDT &sql_udt,
                                             ObString &res_str)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  ObSqlUDTMeta &udt_meta = sql_udt.get_udt_meta();
  ObStringBuffer buf(allocator);
  ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObString udt_data = sql_udt_obj.get_string();
  ObSqlUdtNullBitMap nested_udt_bitmap;
  bool is_null_record = false;
  ObString *attrs = NULL;

  if (udt_meta.pl_type_ == pl::PL_VARRAY_TYPE) { // single varray
    if (udt_meta.child_attr_cnt_ != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("varray udt meta error", K(ret), K(udt_meta.child_attr_cnt_));
    } else if (OB_FAIL(covert_sql_udt_varray_to_string(buf, udt_data, udt_meta))) {
      LOG_WARN("failed to convert sql udt varray to string failed", K(ret));
    }
  } else { // record
    if (OB_FAIL(rearrange_sql_udt_record(udt_data,
                                         udt_meta,
                                         &lob_allocator,
                                         nested_udt_bitmap,
                                         attrs,
                                         is_null_record))) {
      LOG_WARN("failed to rearrange sql udt record", K(ret));
    } else if (OB_ISNULL(attrs) || is_null_record) {
      if (OB_FAIL(buf.append("NULL"))) {
        LOG_WARN("fail to print empty value", K(ret));
      }
    } else {
      int64_t pos = 0;
      if (OB_FAIL(convert_sql_udt_attributes_to_string(buf,
                                                       attrs,
                                                       pos,
                                                       exec_context,
                                                       udt_meta,
                                                       nested_udt_bitmap))) {
        LOG_WARN("fail to convert sql udt attributes to string", K(ret));
      } else if (pos != udt_meta.leaf_attr_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leaf element count mismarch",
                 K(ret), K(pos), K(udt_meta.leaf_attr_cnt_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    res_str.assign_ptr(buf.ptr(), buf.length());
  }
#endif
  return ret;
}

int ObSqlUdtUtils::cast_pl_varray_to_sql_varray(common::ObIAllocator &res_allocator,
                                                ObString &res,
                                                const ObObj root_obj,
                                                bool with_lob_header)
{
  int ret = OB_SUCCESS;
  // for single varray
  int64_t total_len = 0;
  char *res_ptr = NULL;
  int64_t pos = 0;
  if (OB_FAIL(ob_udt_calc_sql_varray_length(&root_obj, total_len, with_lob_header))) {
    LOG_WARN("Failed to calc sql varray length", K(ret), K(root_obj));
  } else if (OB_ISNULL(res_ptr = static_cast<char *>(res_allocator.alloc(total_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(total_len));
  } else if (OB_FAIL(ob_udt_convert_pl_varray_to_sql_varray(&root_obj, res_ptr, total_len, pos, with_lob_header))) {
    LOG_WARN("convert pl varray to sql varray failed", K(ret), K(pos));
  } else if (total_len != pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("covert single varray failed, length error", K(ret), K(total_len), K(pos));
  } else {
    res.assign_ptr(res_ptr, total_len);
  }
  return ret;
}

int ObSqlUdtUtils::cast_pl_record_to_sql_record(common::ObIAllocator &tmp_allocator,
                                                common::ObIAllocator &res_allocator,
                                                sql::ObExecContext *exec_ctx,
                                                ObString &res,
                                                ObSqlUDT &sql_udt,
                                                const ObObj &root_obj)
{
  int ret = OB_SUCCESS;
  // root udt always has a nested udt bitmap, even if does not has any nested udt attributes
  // the nested udt bitmap is a special attribute always at position 0
  ObObj nested_udt_bitmap_obj;
  nested_udt_bitmap_obj.set_type(ObHexStringType);
  int32_t leaf_attr_count = sql_udt.get_udt_meta().leaf_attr_cnt_ + 1;
  uint32_t nested_udt_count = sql_udt.get_udt_meta().nested_udt_number_ + 1;
  uint32_t nested_udt_bitmap_len = ObSqlUDT::get_null_bitmap_len(nested_udt_count);
  int64_t sql_udt_total_len = 0;

  char * bitmap_buffer = reinterpret_cast<char *>(tmp_allocator.alloc(nested_udt_bitmap_len));
  const ObObj **flattern_objs =
    reinterpret_cast<const ObObj **>(tmp_allocator.alloc(sizeof(ObObj *) * (leaf_attr_count)));
  const ObObj **sorted_objs =
    reinterpret_cast<const ObObj **>(tmp_allocator.alloc(sizeof(ObObj *) * (leaf_attr_count)));

  if (OB_ISNULL(flattern_objs) || OB_ISNULL(sorted_objs) || OB_ISNULL(bitmap_buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for pl extend cast to udt",
             K(ret), KP(flattern_objs), KP(sorted_objs), KP(bitmap_buffer));
  } else {
    MEMSET(bitmap_buffer, 0, nested_udt_bitmap_len);
  }

  ObSqlUdtNullBitMap nested_udt_bitmap(bitmap_buffer, nested_udt_bitmap_len);
  nested_udt_bitmap.get_pos()++; // first bit is used by root udt
  int32_t pos = 1;
  int32_t reorder_start_pos = 1;

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ob_udt_flattern_pl_extend(flattern_objs, leaf_attr_count, pos,
                                               nested_udt_bitmap, &root_obj,
                                               *exec_ctx, sql_udt))) {
    LOG_WARN("failed to flattern pl extend", K(ret), K(root_obj), K(leaf_attr_count));
  } else if (OB_FAIL(ob_udt_reordering_leaf_objects(&flattern_objs[reorder_start_pos],
                                                    &sorted_objs[reorder_start_pos],
                                                    leaf_attr_count - reorder_start_pos,
                                                    sql_udt))) {
    LOG_WARN("failed to reorder pl extend", K(ret), K(root_obj), K(leaf_attr_count));
  } else if (OB_FAIL(ob_udt_build_nested_udt_bitmap_obj(nested_udt_bitmap_obj,
                                                        nested_udt_bitmap))) {
    LOG_WARN("failed to build nested udt bitmap object", K(ret), K(root_obj), K(leaf_attr_count));
  } else if (FALSE_IT(sorted_objs[0] = &nested_udt_bitmap_obj)) {
  } else if (OB_FAIL(ob_udt_calc_total_len(sorted_objs,leaf_attr_count,
                                           sql_udt_total_len, sql_udt))) {
    LOG_WARN("failed to calc total sql udt len", K(ret), K(root_obj), K(leaf_attr_count));
  } else if (sql_udt_total_len == 0) {
    res.reset();
  } else {
    ObTextStringResult blob_res(ObLongTextType, true, &res_allocator);
    char *buf = NULL;
    int64_t buf_len = 0;
    int64_t buf_pos = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(blob_res.init(sql_udt_total_len))) {
      LOG_WARN("failed to alloc temp lob", K(ret), K(sql_udt_total_len));
    } else if (OB_FAIL(blob_res.get_reserved_buffer(buf, buf_len))) {
      LOG_WARN("failed to reserve temp lob buffer", K(ret), K(sql_udt_total_len));
    } else if (sql_udt_total_len != buf_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get reserve len is invalid", K(ret), K(sql_udt_total_len), K(buf_len));
    } else if (OB_FAIL(ob_udt_convert_sorted_objs_array_to_udf_format(sorted_objs, buf,
                                                                      buf_len, buf_pos, sql_udt))) {
      LOG_WARN("faild to convert pl extend to sql udt", K(ret), K(sql_udt.get_udt_meta().udt_id_));
    } else if (OB_FAIL(blob_res.lseek(buf_pos, 0))) {
      LOG_WARN("temp lob lseek failed", K(ret), K(blob_res), K(buf_pos));
    } else {
      blob_res.get_result_buffer(res);
    }
  }
  return ret;
}

int ObSqlUdtUtils::build_empty_record(sql::ObExecContext *exec_ctx, ObObj &result, uint64_t udt_id)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need execute ctx to get subschema map on phyplan ctx", K(ret), K(udt_id));
  }
  ObSQLSessionInfo *session = exec_ctx->get_my_session();
  ObIAllocator &alloc = exec_ctx->get_allocator();
  pl::ObPLPackageGuard package_guard(session->get_effective_tenant_id());
  pl::ObPLResolveCtx resolve_ctx(alloc,
                                 *session,
                                 *(exec_ctx->get_sql_ctx()->schema_guard_),
                                 package_guard,
                                 *(exec_ctx->get_sql_proxy()),
                                 false);
  pl::ObPLINS *ns = NULL;
  if (NULL == session->get_pl_context()) {
    OZ (package_guard.init());
    OX (ns = &resolve_ctx);
  } else {
    ns = session->get_pl_context()->get_current_ctx();
  }
  if (OB_SUCC(ret)) {
    ObObj new_composite;
    int64_t ptr = 0;
    int64_t init_size = OB_INVALID_SIZE;
    ObArenaAllocator tmp_alloc;
    const pl::ObUserDefinedType *user_type = NULL;
    OZ (ns->get_user_type(udt_id, user_type, &tmp_alloc));
    CK (OB_NOT_NULL(user_type));
    OZ (user_type->newx(alloc, ns, ptr));
    OZ (user_type->get_size(pl::PL_TYPE_INIT_SIZE, init_size));
    OX (new_composite.set_extend(ptr, user_type->get_type(), init_size));
    OX (result = new_composite);
  }
#endif
  return ret;
}

int ObSqlUdtUtils::cast_sql_udt_varray_to_pl_varray(sql::ObExecContext *exec_ctx,
                                                    ObString &udt_varray_buf,
                                                    ObSqlUDTMeta &udt_meta,
                                                    ObObj &res_obj)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  ObSqlUDT varray_handler;
  ObString varray_data = udt_varray_buf;
  ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need execute ctx to get subschema map on phyplan ctx", K(ret), K(udt_meta));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&lob_allocator,
                                                        ObLongTextType,
                                                        CS_TYPE_BINARY,
                                                        true, varray_data))) {
    LOG_WARN("fail to get real string data", K(ret), K(varray_data));
  }
  ObIAllocator &alloc = exec_ctx->get_allocator();
  varray_handler.set_data(varray_data);
  varray_handler.set_udt_meta(udt_meta);
  if (OB_FAIL(ret)) {
  } else if (varray_data.empty()) {
    res_obj.set_null();
  } else {
    // alloc collection(varray) hander
    common::ObDataType elem_type;
    pl::ObElemDesc elem_desc;
    pl::ObPLCollection *coll = NULL;
    if (OB_ISNULL(coll = static_cast<pl::ObPLVArray*>(alloc.alloc(sizeof(pl::ObPLVArray) + 8)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for pl collection", K(ret), K(coll));
    } else {
      coll = new(coll)pl::ObPLVArray(udt_meta.udt_id_);
      static_cast<pl::ObPLVArray*>(coll)->set_capacity(udt_meta.varray_capacity_);
      elem_type.set_meta_type(udt_meta.child_attrs_meta_[0].type_info_);
      elem_desc.set_data_type(elem_type); // Notice: accuracty and others not setted
      elem_desc.set_field_count(1); // varray with basic type elements, field count is 1.
      coll->set_element_desc(elem_desc);
      coll->set_not_null(false); // should from udt meta
    }

    ObObj *varray_objs = NULL;
    uint64_t element_count = varray_handler.get_varray_element_count();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObSPIService::spi_set_collection(0, NULL, alloc, *coll, element_count))) {
      LOG_WARN("failed to allocate memory for pl collection", K(ret), K(coll));
    } else if (OB_ISNULL(varray_objs = coll->get_data())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("varray data is null", K(ret));
    }

    ObString attr_data;
    for (int64_t i = 0; OB_SUCC(ret) && i < element_count; i++) {
      if (OB_FAIL(varray_handler.access_attribute(i, attr_data, true))) {
        LOG_WARN("fail to access attribute buffer", K(ret));
      } else if (attr_data.empty()) {
        // do nothing, all element objs in varray is set null in spi_set_collection
      } else {
        ObObj obj;
        obj.set_meta_type(varray_handler.get_udt_meta().child_attrs_meta_[0].type_info_);
        int64_t pos = 0;
        if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(obj,
                                                               attr_data.ptr(),
                                                               attr_data.length(),
                                                               pos))) {
          LOG_WARN("failed to serialize object value", K(ret), K(attr_data));
        } else if (OB_FAIL(deep_copy_obj(*coll->get_allocator(), obj, varray_objs[i]))){
          LOG_WARN("failed to deep copy element object value", K(ret), K(obj), K(i));
        }
      }
    }

    // is nested varray needs add to pl ctx? may not needed for obobj cast
    if (OB_SUCC(ret)) {
      res_obj.set_extend(reinterpret_cast<int64_t>(coll), coll->get_type());
      if (OB_NOT_NULL(coll->get_allocator())) {
        if (OB_ISNULL(exec_ctx->get_pl_ctx())) {
          if (OB_FAIL(exec_ctx->init_pl_ctx() || OB_ISNULL(exec_ctx->get_pl_ctx()))) {
            LOG_ERROR("fail to init pl ctx", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(exec_ctx->get_pl_ctx()->add(res_obj))) {
          LOG_ERROR("fail to collect pl collection allocator, may be exist memory issue", K(ret));
        }
      }
    }
  }
#endif
  return ret;
}

int ObSqlUdtUtils::cast_sql_udt_attributes_to_pl_record(sql::ObExecContext *exec_ctx,
                                                        ObString *attrs,
                                                        int64_t &pos,
                                                        ObSqlUDTMeta &udt_meta,
                                                        ObSqlUdtNullBitMap &nest_udt_bitmap,
                                                        ObObj &res_obj)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need execute ctx to get subschema map on phyplan ctx", K(ret), K(udt_meta));
  } else {
    ObIAllocator &allocator = exec_ctx->get_allocator();
    // null root is handled by the caller
    uint32_t top_level_attr_count = udt_meta.child_attr_cnt_;
    pl::ObPLRecord *record =
      static_cast<pl::ObPLRecord*>(allocator.alloc(pl::ObRecordType::get_init_size(top_level_attr_count)));
    if (OB_ISNULL(record)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      new(record)pl::ObPLRecord(udt_meta.udt_id_, top_level_attr_count);
    }
    ObObj obj;
    for (int64_t i = 0; OB_SUCC(ret) && i < top_level_attr_count; i++) {
      obj.reset();
      obj.set_meta_type(udt_meta.child_attrs_meta_[i].type_info_);
      ObObjType type = obj.get_type();
      bool is_udt_null = false;
      bool is_nested_record = false;

      if (type == ObUserDefinedSQLType || type == ObCollectionSQLType) {
        const uint16_t subschema_id = obj.get_meta().get_subschema_id();
        ObSqlUDTMeta sub_udt_meta;
        // should be varray or record
        if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, sub_udt_meta))) {
          LOG_WARN("failed to get udt meta", K(ret), K(subschema_id));
        }
        if (OB_FAIL(ret)) {
        } else if (nest_udt_bitmap.is_valid()
                    && OB_FAIL(nest_udt_bitmap.check_current_bitmap_pos(is_udt_null))) {
          LOG_WARN("failed to get nested udt null bit", K(ret));
        } else if (sub_udt_meta.pl_type_ == pl::PL_VARRAY_TYPE) {
          if (is_udt_null) {
            obj.set_null();
          } else if (OB_FAIL(cast_sql_udt_varray_to_pl_varray(exec_ctx, attrs[pos], sub_udt_meta, obj))) {
            LOG_WARN("failed to convert nested sql udt varray to pl extend",
                      K(ret), K(i), K(pos), K(subschema_id), K(sub_udt_meta.udt_id_));
          }
        } else if (sub_udt_meta.pl_type_ == pl::PL_RECORD_TYPE) {
          if (is_udt_null) {
            if (OB_FAIL(build_empty_record(exec_ctx, obj, sub_udt_meta.udt_id_))) {
              LOG_WARN("failed to create empty nested udt record", K(ret));
            } else {
              pl::ObPLRecord *child_null_record = reinterpret_cast<pl::ObPLRecord *>(obj.get_ext());
              child_null_record->set_null();
              pos += sub_udt_meta.leaf_attr_cnt_;
              // count the nested udt itself
              nest_udt_bitmap.get_pos() += (sub_udt_meta.nested_udt_number_ + 1);
            }
          } else if (OB_FAIL(cast_sql_udt_attributes_to_pl_record(exec_ctx,
                                                                  attrs,
                                                                  pos,
                                                                  sub_udt_meta,
                                                                  nest_udt_bitmap,
                                                                  obj))) {
            LOG_WARN("failed to convert nestd sql udt record to pl extend", K(ret), K(sub_udt_meta.pl_type_));
          }
          is_nested_record = true;
          // pos is changed inside;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected type to convert sql udt to pl udt format", K(ret), K(sub_udt_meta.pl_type_));
        }
      } else { // basic types
        int64_t buf_pos = 0;
        if (OB_ISNULL(attrs[pos])) {
          obj.set_null();
        } else if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(obj,
                                                                      attrs[pos].ptr(),
                                                                      attrs[pos].length(),
                                                                      buf_pos))) {
          LOG_WARN("Failed to serialize object value", K(ret), K(attrs[pos]));
        }
      }

      if (OB_SUCC(ret)){
        pos += (is_nested_record ? 0 : 1);
        if (type == ObUserDefinedSQLType || type == ObCollectionSQLType) {
          obj.meta_.set_ext();
          obj.meta_.set_extend_type(type == ObUserDefinedSQLType ? pl::PL_RECORD_TYPE : pl::PL_VARRAY_TYPE);
        }
        record->get_element()[i] = obj;
      }
    }
    if (OB_SUCC(ret)) {
      res_obj.set_extend(reinterpret_cast<int64_t>(record),
                        pl::PL_RECORD_TYPE, pl::ObRecordType::get_init_size(top_level_attr_count));
    }
  }
#endif
  return ret;
}

int ObSqlUdtUtils::cast_sql_record_to_pl_record(sql::ObExecContext *exec_ctx,
                                                ObObj &result,
                                                ObString &udt_data,
                                                ObSqlUDTMeta &udt_meta)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  if (udt_meta.pl_type_ == pl::PL_VARRAY_TYPE) { // single varray
    if (udt_meta.child_attr_cnt_ != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("varray udt meta error", K(ret), K(udt_meta.child_attr_cnt_));
    } else if (OB_FAIL(cast_sql_udt_varray_to_pl_varray(exec_ctx, udt_data, udt_meta, result))) {
      LOG_WARN("failed to convert sql udt varray to pl varray", K(ret));
    }
  } else {
    // udt meta already set
    bool is_null_record = false;
    ObSqlUdtNullBitMap nested_udt_bitmap;
    ObString *attrs = NULL;
    ObArenaAllocator lob_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

    if (OB_FAIL(rearrange_sql_udt_record(udt_data,
                                         udt_meta,
                                         &lob_allocator,
                                         nested_udt_bitmap,
                                         attrs,
                                         is_null_record))) {
      LOG_WARN("failed to rearrange sql udt record", K(ret));
    } else if (OB_ISNULL(attrs) || is_null_record) {
      result.set_null();
    } else {
      int64_t pos = 0;
      if (OB_FAIL(cast_sql_udt_attributes_to_pl_record(exec_ctx,
                                                       attrs,
                                                       pos,
                                                       udt_meta,
                                                       nested_udt_bitmap,
                                                       result))) {
        LOG_WARN("fail to cast sql udt record to pl record", K(ret));
      } else if (pos != udt_meta.leaf_attr_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leaf element count mismarch",
                 K(ret), K(pos), K(udt_meta.leaf_attr_cnt_));
      }
    }
  }
#endif
  return ret;
}

int ObSqlUdtUtils::get_sqludt_meta_by_subschema_id(sql::ObExecContext *exec_ctx,
                                                   const uint16_t subschema_id,
                                                   ObSqlUDTMeta &udt_meta) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need execute ctx to get subschema map on phyplan ctx", K(ret), K(subschema_id));
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
    LOG_WARN("failed to get udt meta", K(ret), K(subschema_id));
  }
  return ret;
}

int ObSqlUdtMetaUtils::get_udt_meta_attr_info(ObSchemaGetterGuard *schema_guard,
                                              uint64_t tenant_id,
                                              const ObUDTTypeInfo *parent_udt_info,
                                              uint32_t &nested_udt_number,
                                              common::ObIArray<ObUDTTypeAttr *> &leaf_attr_meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(parent_udt_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null schema guard or parent udt info", K(ret), KP(schema_guard), KP(parent_udt_info));
  } else {
    uint32_t count = parent_udt_info->get_attrs_count();
    for (uint32_t i = 0; i < count; i++) {
      ObUDTTypeAttr *udt_attr = parent_udt_info->get_attrs().at(i);
      if (udt_attr->get_type_attr_id() < ObMaxType) {
        if (OB_FAIL(leaf_attr_meta.push_back(udt_attr))) {
          LOG_WARN("failed to push back leaf attr pointer", K(i), K(count), K(leaf_attr_meta.count()));
        }
      } else {
        uint64_t udt_id = udt_attr->get_type_attr_id();
        const ObUDTTypeInfo *attr_udt_info = NULL;
        if (OB_FAIL(schema_guard->get_udt_info(tenant_id, udt_id, attr_udt_info))) {
          // pl::get_tenant_id_by_object_id
          LOG_WARN("failed to get udt info", K(ret), K(tenant_id), K(udt_id));
        } else if (OB_ISNULL(attr_udt_info) ) { // try system udt
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("udt info not found", K(ret), K(tenant_id), K(udt_id));
        } else if (OB_FAIL(get_udt_meta_attr_info(schema_guard,
                                                  tenant_id,
                                                  attr_udt_info,
                                                  nested_udt_number,
                                                  leaf_attr_meta))) {
          LOG_WARN("failed to get nested udt meta attr info",
                   K(ret), K(tenant_id), K(parent_udt_info->get_type_id()), K(udt_id));
        } else if (attr_udt_info->is_object_type()) {
          nested_udt_number++;
        } else if (attr_udt_info->is_varray()) { // varray is special to other udts, it is a basic type in sql
          if (OB_FAIL(leaf_attr_meta.push_back(udt_attr))) {
            LOG_WARN("failed to push back leaf attr pointer", K(i), K(count), K(leaf_attr_meta.count()));
          }
        }
      }
    }
  }
  return ret;
}

int ObSqlUdtMetaUtils::fill_udt_meta_attr_info(ObSchemaGetterGuard *schema_guard,
                                               ObSubSchemaCtx *subschema_ctx,
                                               uint64_t tenant_id,
                                               const common::ObIArray<ObUDTTypeAttr *> &src,
                                               ObSqlUDTAttrMeta *dst,
                                               const uint32 dst_cnt,
                                               bool is_leaf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null src or dest for udt type attr", K(src), KP(dst));
  } else if (src.count() != dst_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attr count mismarch", K(src), K(src.count()), K(dst_cnt));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < dst_cnt; i++) {
      ObUDTTypeAttr *udt_attr = src.at(i);
      ObSqlUDTAttrMeta *udt_attr_meta = &dst[i];
      udt_attr_meta->order_ = i; // order of orignal leaf attributes
      udt_attr_meta->type_info_.reset();

      if (OB_ISNULL(udt_attr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null udt attribute", K(i), K(src.count()));
      } else if (udt_attr->get_type_attr_id() < ObMaxType) {
        udt_attr_meta->type_info_.set_type(static_cast<ObObjType>(udt_attr->get_type_attr_id()));
        udt_attr_meta->type_info_.set_collation_type(static_cast<const ObCollationType>(udt_attr->get_coll_type()));
        udt_attr_meta->type_info_.set_scale(static_cast<ObScale>(udt_attr->get_scale()));
      } else { // udt attributes
        const ObUDTTypeInfo *attr_udt_info = NULL;
        uint64_t udt_id = udt_attr->get_type_attr_id();
        if (OB_ISNULL(schema_guard)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null schema guard", K(ret));
        } else if (OB_FAIL(schema_guard->get_udt_info(tenant_id, udt_id, attr_udt_info))) {
          LOG_WARN("failed to get udt info", K(ret), K(tenant_id), K(udt_id));
        } else if (OB_ISNULL(attr_udt_info) ) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("udt info not found", K(ret), K(tenant_id), K(udt_id));
        } else if (attr_udt_info->is_object_type()) {
          udt_attr_meta->type_info_.set_type(ObUserDefinedSQLType);
        } else if (attr_udt_info->is_varray()) {
          udt_attr_meta->type_info_.set_type(ObCollectionSQLType);
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported udt type", K(ret), K(*attr_udt_info));
        }

        uint16_t subschema_id = ObMaxSystemUDTSqlType;
        if (OB_FAIL(ret)) {
        } else if (is_leaf
                   && udt_attr->get_type_attr_id() > ObMaxType
                   && !attr_udt_info->is_varray()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected non-leaf attrs", K(ret), K(*udt_attr), K(i), K(src.count()));
        } else if (OB_ISNULL(subschema_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null subschema ctx", K(ret));
        } else if (OB_FAIL(subschema_ctx->get_subschema_id(udt_id,
                                                           OB_SUBSCHEMA_UDT_TYPE,
                                                           subschema_id))) {
          // ToDo: @gehao, refine
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("failed to get subschema id by udt_id", K(ret), K(udt_id));
          } else { // build new meta
            ret = OB_SUCCESS;
            uint16 new_subschema_id = ObMaxSystemUDTSqlType;
            ObSqlUDTMeta *udt_meta = NULL;
            ObSubSchemaValue value;
            ObIAllocator &allocator = subschema_ctx->get_allocator();
            if (OB_ISNULL(schema_guard)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("No schema gurad to generate udt_meta", K(ret), K(udt_id));
            } else if (FALSE_IT(udt_meta = reinterpret_cast<ObSqlUDTMeta *>(allocator.alloc(sizeof(ObSqlUDTMeta))))) {
            } else if (OB_ISNULL(udt_meta)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloc udt meta", K(ret), K(udt_id), K(sizeof(ObSqlUDTMeta)));
            } else if (FALSE_IT(MEMSET(udt_meta, 0, sizeof(ObSqlUDTMeta)))) {
            } else if (OB_FAIL(ObSqlUdtMetaUtils::generate_udt_meta_from_schema(schema_guard,
                                                                                subschema_ctx,
                                                                                allocator, // use allocator in physical plan
                                                                                tenant_id,
                                                                                udt_id,
                                                                                *udt_meta))) {
              LOG_WARN("generate udt_meta failed", K(ret), K(tenant_id), K(udt_id));
            } else if (OB_FAIL(subschema_ctx->get_subschema_id_from_fields(udt_id, new_subschema_id))) {
              LOG_WARN("failed to get subschema id from result fields", K(ret), K(tenant_id), K(udt_id));
            } else if (new_subschema_id == ObInvalidSqlType // not get from fields, generate new
                       && OB_FAIL(subschema_ctx->get_new_subschema_id(new_subschema_id))) {
              LOG_WARN("failed to get new subschema id", K(ret), K(tenant_id), K(udt_id));
            } else {
              value.type_ = OB_SUBSCHEMA_UDT_TYPE;
              value.signature_ = udt_id;
              value.value_ = static_cast<void *>(udt_meta);
              if (OB_FAIL(subschema_ctx->set_subschema(new_subschema_id, value))) {
                LOG_WARN("failed to set new subschema", K(ret), K(new_subschema_id), K(udt_id), K(value));
              } else {
                subschema_id = new_subschema_id;
                udt_attr_meta->type_info_.set_subschema_id(subschema_id);
              }
            }
          }
        } else {
          udt_attr_meta->type_info_.set_subschema_id(subschema_id);
        }
        if (OB_FAIL(ret)) {
        } else if (udt_attr_meta->type_info_.is_collection_sql_type()) {
          udt_attr_meta->type_info_.set_inrow();
        } else if (is_lob_storage(udt_attr_meta->type_info_.get_type())) {
          udt_attr_meta->type_info_.set_has_lob_header();
        }
      }
    }
  }
  return ret;
}

int ObSqlUdtMetaUtils::generate_udt_meta_from_schema(ObSchemaGetterGuard *schema_guard,
                                                     ObSubSchemaCtx *subschema_ctx,
                                                     common::ObIAllocator &allocator,
                                                     uint64_t tenant_id,
                                                     uint64_t udt_id,
                                                     ObSqlUDTMeta &udt_meta)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  const ObUDTTypeInfo *root_udt_info = NULL;
  ObSEArray<ObUDTTypeAttr *, 4> temp_leaf_attr_meta;
  uint32_t child_attrs_cnt = 0;
  uint32_t leaf_attrs_cnt = 0;
  uint32_t fixed_len_attr_cnt = 0;
  uint32_t fixed_attr_cnt = 0; // not used leaf an empty interface
  int32_t fixed_offset_ = 0;
  int32_t pl_type = 0;
  uint32_t nested_udt_number = 0;
  uint32_t varray_capacity = 0;
  ObString type_name;

  if (is_inner_pl_object_id(udt_id)) {
    tenant_id = OB_SYS_TENANT_ID;
  }
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_udt_info(tenant_id, udt_id, root_udt_info))) {
    // pl::get_tenant_id_by_object_id
    LOG_WARN("failed to get udt info", K(ret), K(tenant_id), K(udt_id));
  } else if (OB_ISNULL(root_udt_info) ) { // try system udt
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt info not found", K(ret), K(tenant_id), K(udt_id));
  } else if (root_udt_info->get_type_name().empty()) { // copy name
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt info without name", K(ret), K(tenant_id), K(udt_id));
  } else if (OB_FAIL(ob_write_string(allocator, root_udt_info->get_type_name(), type_name))) {
    LOG_WARN("failed to copy udt name", K(ret), K(tenant_id), K(udt_id));
  } else {
    udt_meta.set_name(type_name);
    udt_meta.udt_id_ = udt_id;
    if (root_udt_info->is_object_type()) {
      pl_type = static_cast<int32_t>(pl::PL_RECORD_TYPE);
      child_attrs_cnt = root_udt_info->get_local_attrs();
    } else if (root_udt_info->is_varray()) {
      pl_type = static_cast<int32_t>(pl::PL_VARRAY_TYPE);
      if (OB_NOT_NULL(root_udt_info->get_coll_info())) {
        varray_capacity = root_udt_info->get_coll_info()->get_upper_bound();
      }
      child_attrs_cnt = 1;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported udt type", K(ret), K(*root_udt_info));
    }
    if (OB_SUCC(ret)) {
      udt_meta.varray_capacity_ = varray_capacity;
      udt_meta.pl_type_ = pl_type;
      udt_meta.child_attr_cnt_ = child_attrs_cnt;
      if (OB_FAIL(get_udt_meta_attr_info(schema_guard,
                                          tenant_id,
                                          root_udt_info,
                                          nested_udt_number,
                                          temp_leaf_attr_meta))) {
        LOG_WARN("failed to fill udt meta info");
      } else {
        leaf_attrs_cnt = temp_leaf_attr_meta.count();
        udt_meta.nested_udt_number_ = nested_udt_number;
        udt_meta.leaf_attr_cnt_ = leaf_attrs_cnt;
        udt_meta.attribute_cnt_ = leaf_attrs_cnt;
        udt_meta.fixed_attr_cnt_ = fixed_attr_cnt;
        udt_meta.fixed_offset_ = fixed_offset_;
        udt_meta.child_attrs_meta_ = NULL;
        udt_meta.leaf_attrs_meta_ = NULL;
        ObSqlUDTAttrMeta *leaf_attrs_meta = NULL;
        uint32_t child_attrs_size = sizeof(ObSqlUDTAttrMeta) * child_attrs_cnt;
        ObSqlUDTAttrMeta *child_attrs_meta
            = reinterpret_cast<ObSqlUDTAttrMeta *>(allocator.alloc(child_attrs_size));
        if (child_attrs_cnt > 0 && OB_ISNULL(child_attrs_meta)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for child attrs failed",
                   K(ret), K(child_attrs_cnt), K(child_attrs_size));
        } else if (root_udt_info->is_varray()) {
          if (child_attrs_cnt != 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error child attribute count for varray", K(ret), K(child_attrs_cnt));
          } else {
            ObObjType elem_type = static_cast<ObObjType>(root_udt_info->get_coll_info()->get_elem_type_id());
            ObCollationType coll_type = static_cast<const ObCollationType>(
                                          root_udt_info->get_coll_info()->get_coll_type());
            child_attrs_meta->order_ = 0;
            child_attrs_meta->type_info_.set_type(elem_type);
            // child_attrs_meta->type_info_.set_collation_level(CS_LEVEL_IMPLICIT); // setted in set_type
            child_attrs_meta->type_info_.set_collation_type(coll_type);
            child_attrs_meta->type_info_.set_scale(static_cast<ObScale>(root_udt_info->get_coll_info()->get_scale()));
            udt_meta.child_attrs_meta_ = child_attrs_meta;
          }
        } else if (OB_FAIL(fill_udt_meta_attr_info(schema_guard,
                                                   subschema_ctx,
                                                   tenant_id,
                                                   root_udt_info->get_attrs(),
                                                   child_attrs_meta,
                                                   child_attrs_cnt,
                                                   false))) { // record type
          LOG_WARN("failed to fill udt meta attr info", K(ret), K(child_attrs_cnt), K(*root_udt_info));
        } else {
          udt_meta.child_attrs_meta_ = child_attrs_meta;
        }

        uint32_t leaf_attrs_size = sizeof(ObSqlUDTAttrMeta) * leaf_attrs_cnt;
        if (OB_FAIL(ret) || leaf_attrs_cnt == 0) {
        } else if (FALSE_IT(leaf_attrs_meta = reinterpret_cast<ObSqlUDTAttrMeta *>(
                                                allocator.alloc(leaf_attrs_size)))) {
        } else if (OB_ISNULL(leaf_attrs_meta)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed for leaf attrs failed",
                   K(ret), K(leaf_attrs_cnt), K(leaf_attrs_size));
        } else if (OB_FAIL(fill_udt_meta_attr_info(schema_guard,
                                                   subschema_ctx,
                                                   tenant_id,
                                                   temp_leaf_attr_meta,
                                                   leaf_attrs_meta,
                                                   leaf_attrs_cnt,
                                                   true))) {
          LOG_WARN("failed to fill udt meta attr info",
                   K(ret), K(leaf_attrs_cnt), K(temp_leaf_attr_meta));
        } else {
          udt_meta.leaf_attrs_meta_ = leaf_attrs_meta;
        }
      }
    }
  }
#endif
  return ret;
}

}
}
