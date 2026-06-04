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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_obj_access.h"
#include "pl/ob_pl_resolver.h"
#include "src/sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

namespace
{
inline bool is_pl_type_collection(int32_t t)
{
  return t == static_cast<int32_t>(pl::PL_NESTED_TABLE_TYPE)
      || t == static_cast<int32_t>(pl::PL_ASSOCIATIVE_ARRAY_TYPE)
      || t == static_cast<int32_t>(pl::PL_VARRAY_TYPE);
}
inline bool is_pl_type_composite(int32_t t)
{
  return is_pl_type_collection(t)
      || t == static_cast<int32_t>(pl::PL_RECORD_TYPE)
      || t == static_cast<int32_t>(pl::PL_OPAQUE_TYPE);
}
} // namespace

OB_SERIALIZE_MEMBER(ObExprObjAccess::ExtraInfo::SqlUdtAccessIdx,
                    var_index_,
                    elem_user_type_id_,
                    var_pl_type_,
                    is_const_);

int ObExprObjAccess::ExtraInfo::get_obj_access_param(const ParamStore &param_store,
                                                     const ObObj *params,
                                                     const int64_t param_num,
                                                     const int64_t param_idx,
                                                     const ObObj *&obj) const
{
  int ret = OB_SUCCESS;
  obj = NULL;
  if (param_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid object access param index", K(ret), K(param_idx));
  } else if (param_idx < param_idxs_.count()) {
    int64_t store_idx = param_idxs_.at(param_idx);
    if (OB_UNLIKELY(store_idx < 0 || store_idx >= param_store.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param store index", K(ret), K(param_idx), K(store_idx), K(param_store.count()));
    } else {
      obj = &param_store.at(store_idx);
    }
  } else {
    int64_t stack_idx = param_idx - param_idxs_.count();
    if (OB_UNLIKELY(stack_idx < 0 || stack_idx >= param_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expression param index", K(ret), K(param_idx), K(stack_idx), K(param_num));
    } else {
      obj = &params[stack_idx];
    }
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::get_int64_from_obj(const ObObj &obj, int64_t &val, bool skip_null_check)
{
  int ret = OB_SUCCESS;
  val = 0;
  if (obj.is_integer_type() || obj.is_ext()) {
    val = obj.get_int();
  } else if (obj.is_number()) {
    number::ObNumber number = obj.get_number();
    if (!number.is_valid_int64(val)) {
      if (OB_FAIL(number.round(0))) {
        LOG_WARN("failed to round number", K(ret), K(number));
      } else if (!number.is_valid_int64(val)) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("array index is out of range", K(ret), K(number));
      }
    }
  } else if (obj.is_decimal_int()) {
    number::ObNumber numb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(wide::to_number(obj.get_decimal_int(), obj.get_int_bytes(),
                                obj.get_scale(), tmp_alloc, numb))) {
      LOG_WARN("fail to cast decimal int to number", K(ret));
    } else if (!numb.is_valid_int64(val)) {
      if (OB_FAIL(numb.round(0))) {
        LOG_WARN("failed to round number", K(ret), K(numb));
      } else if (!numb.is_valid_int64(val)) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("array index is out of range", K(ret), K(numb));
      }
    }
  } else if (obj.is_null()) {
    if (skip_null_check) {
      val = OB_INVALID_INDEX;
    } else {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      LOG_WARN("OBE-06502: PL/SQL: numeric or value error: NULL index table key value",
               K(ret), K(obj));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj param is invalid type", K(ret), K(obj));
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::get_obj_access_int_param(const ParamStore &param_store,
                                                         const ObObj *params,
                                                         const int64_t param_num,
                                                         const int64_t param_idx,
                                                         int64_t &val) const
{
  int ret = OB_SUCCESS;
  const ObObj *obj = NULL;
  if (OB_FAIL(get_obj_access_param(param_store, params, param_num, param_idx, obj))) {
    LOG_WARN("failed to get object access param", K(ret), K(param_idx));
  } else if (OB_ISNULL(obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object access param is null", K(ret), K(param_idx));
  } else if (OB_FAIL(get_int64_from_obj(*obj, val))) {
    LOG_WARN("failed to get int param", K(ret), KPC(obj), K(param_idx));
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::parse_serialized_pl_header(const SerializedPLObjSlice &slice,
                                                           int64_t &pos,
                                                           pl::ObPLType &pl_type,
                                                           uint64_t &id,
                                                           bool &is_null)
{
  int ret = OB_SUCCESS;
  int64_t version = OB_INVALID_VERSION;
  pos = 0;
  pl_type = pl::PL_INVALID_TYPE;
  id = OB_INVALID_ID;
  is_null = false;
  if (OB_ISNULL(slice.data_) || slice.len_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid serialized pl object slice", K(ret), KP(slice.data_), K(slice.len_));
  } else if (OB_FAIL(serialization::decode(slice.data_, slice.len_, pos, version))) {
    LOG_WARN("failed to decode serialized pl version", K(ret), K(slice.len_));
  } else if (OB_FAIL(serialization::decode(slice.data_, slice.len_, pos, pl_type))) {
    LOG_WARN("failed to decode serialized pl type", K(ret), K(slice.len_), K(pos));
  } else if (pl_type != pl::PL_RECORD_TYPE
             && pl_type != pl::PL_VARRAY_TYPE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported serialized pl type for direct access", K(ret), K(pl_type));
  } else if (OB_FAIL(serialization::decode(slice.data_, slice.len_, pos, id))) {
    LOG_WARN("failed to decode serialized pl id", K(ret), K(pl_type), K(pos));
  } else if (OB_FAIL(serialization::decode(slice.data_, slice.len_, pos, is_null))) {
    LOG_WARN("failed to decode serialized pl null flag", K(ret), K(pl_type), K(pos));
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::get_serialized_obj_payload(const SerializedPLObjSlice &obj_slice,
                                                           SerializedPLObjSlice &payload_slice)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObObj obj;
  payload_slice = SerializedPLObjSlice();
  if (OB_FAIL(obj.meta_.deserialize(obj_slice.data_, obj_slice.len_, pos))) {
    LOG_WARN("failed to deserialize object meta", K(ret), K(obj_slice.len_));
  } else if (OB_UNLIKELY(obj.is_invalid_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid serialized object type", K(ret), K(obj));
  } else if (obj.is_ext()) {
    int64_t ext_val = 0;
    if (OB_FAIL(serialization::decode(obj_slice.data_, obj_slice.len_, pos, ext_val))) {
      LOG_WARN("failed to decode ext value", K(ret), K(pos), K(obj_slice.len_));
    } else if (0 == ext_val) {
      payload_slice.is_null_ = true;
    } else if (!ObObj::is_ext_val(ext_val)) {
      int64_t composite_len = 0;
      if (OB_FAIL(serialization::decode(obj_slice.data_, obj_slice.len_, pos, composite_len))) {
        LOG_WARN("failed to decode composite length", K(ret), K(pos), K(obj_slice.len_));
      } else if (OB_UNLIKELY(composite_len < 0 || pos + composite_len > obj_slice.len_)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("invalid composite length", K(ret), K(pos), K(composite_len), K(obj_slice.len_));
      } else {
        payload_slice.data_ = obj_slice.data_ + pos;
        payload_slice.len_ = composite_len;
      }
    } else {
      ret = OB_READ_NOTHING;
      LOG_WARN("accessing deleted element, no data found", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialized object is not composite", K(ret), K(obj));
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::deserialize_obj_value(ObIAllocator &calc_alloc,
                                                                 const SerializedPLObjSlice &obj_slice,
                                                                 ObObj &result,
                                                                 uint16_t subschema_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObObj src_obj;
  result.reset();
  if (obj_slice.is_null_) {
    result.set_null();
  } else if (OB_FAIL(src_obj.meta_.deserialize(obj_slice.data_, obj_slice.len_, pos))) {
    LOG_WARN("failed to deserialize object meta", K(ret), K(obj_slice.len_));
  } else if (OB_UNLIKELY(src_obj.is_invalid_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid serialized object type", K(ret), K(src_obj));
  } else if (src_obj.is_ext()) {
    int64_t ext_val = 0;
    if (OB_FAIL(serialization::decode(obj_slice.data_, obj_slice.len_, pos, ext_val))) {
      LOG_WARN("failed to decode ext value for serialized composite field", K(ret), K(pos), K(obj_slice.len_));
    } else if (0 == ext_val) {
      result.set_ext(ext_val);
    } else if (ObObj::is_ext_val(ext_val)) {
      ret = OB_READ_NOTHING;
      LOG_WARN("accessing deleted composite field, no data found", K(ret));
    } else {
      int64_t composite_len = 0;
      if (OB_FAIL(serialization::decode(obj_slice.data_, obj_slice.len_, pos, composite_len))) {
        LOG_WARN("failed to decode composite length for serialized field", K(ret), K(pos), K(obj_slice.len_));
      } else if (OB_UNLIKELY(composite_len < 0 || pos + composite_len > obj_slice.len_)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("invalid composite length for serialized field", K(ret), K(pos), K(composite_len), K(obj_slice.len_));
      } else if (pl::PL_OPAQUE_TYPE == src_obj.get_meta().get_extend_type()) {
        if (OB_FAIL(convert_pl_opaque_to_sql_udt(calc_alloc,
                                                 obj_slice.data_ + pos,
                                                 composite_len,
                                                 subschema_id,
                                                 result))) {
          LOG_WARN("failed to convert pl opaque to sql udt", K(ret), K(composite_len), K(subschema_id));
        } else {
          pos += composite_len;
        }
      } else {
        ObTextStringResult blob_res(ObLongTextType, true, &calc_alloc);
        char *buf = NULL;
        int64_t buf_len = 0;
        int64_t buf_pos = 0;
        if (OB_FAIL(blob_res.init(composite_len))) {
          LOG_WARN("failed to init temp lob for sql udt composite field", K(ret), K(composite_len));
        } else if (OB_FAIL(blob_res.get_reserved_buffer(buf, buf_len))) {
          LOG_WARN("failed to reserve temp lob buffer for sql udt composite field", K(ret), K(composite_len));
        } else if (composite_len != buf_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get reserve len is invalid for sql udt composite field", K(ret), K(composite_len), K(buf_len));
        } else {
          const int64_t payload_start = pos;
          MEMCPY(buf, obj_slice.data_ + payload_start, composite_len);
          buf_pos = composite_len;
          if (OB_FAIL(blob_res.lseek(buf_pos, 0))) {
            LOG_WARN("temp lob lseek failed for sql udt composite field", K(ret), K(buf_pos));
          } else {
            ObString tmp;
            blob_res.get_result_buffer(tmp);
            result.reset();
            result.set_sql_udt(tmp.ptr(), static_cast<int32_t>(tmp.length()), subschema_id);
            if (tmp.length() > 0) {
              result.set_has_lob_header();
            }
            pos = payload_start + composite_len;
          }
        }
      }
    }
  } else if (OB_FAIL(ObObjUDTUtil::ob_udt_obj_value_deserialize(src_obj, obj_slice.data_, obj_slice.len_, pos))) {
    LOG_WARN("failed to deserialize object value", K(ret), K(src_obj), K(pos), K(obj_slice.len_));
  } else {
    result = src_obj;
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::convert_pl_opaque_to_sql_udt(ObIAllocator &calc_alloc,
                                                             const char *buf,
                                                             int64_t buf_len,
                                                             uint16_t subschema_id,
                                                             ObObj &result)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSED(calc_alloc);
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(subschema_id);
  UNUSED(result);
  ret = OB_NOT_SUPPORTED;
#else
  ObObj pl_obj;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer for pl opaque deserialization", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(pl::ObUserDefinedType::do_deserialize_obj(calc_alloc, pl_obj,
                                                               buf, buf_len, pos, true))) {
    LOG_WARN("failed to deserialize pl opaque obj", K(ret), K(buf_len));
  } else {
    pl::ObPLOpaque *opaque = reinterpret_cast<pl::ObPLOpaque*>(pl_obj.get_ext());
    if (OB_ISNULL(opaque) || pl::ObPLOpaqueType::PL_INVALID == opaque->get_type()) {
      result.set_null();
    } else if (pl::ObPLOpaqueType::PL_XML_TYPE == opaque->get_type()) {
      pl::ObPLXmlType *xmltype = static_cast<pl::ObPLXmlType*>(opaque);
      ObObj *blob_obj = xmltype->get_data();
      if (OB_ISNULL(blob_obj) || blob_obj->is_null()) {
        result.set_sql_udt("", 0, subschema_id);
      } else {
        ObString xml_bin = blob_obj->get_string();
        char *xml_bin_buf = static_cast<char *>(calc_alloc.alloc(xml_bin.length()));
        if (OB_ISNULL(xml_bin_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for xml binary", K(ret), K(xml_bin.length()));
        } else {
          MEMCPY(xml_bin_buf, xml_bin.ptr(), xml_bin.length());
          result.set_sql_udt(xml_bin_buf, static_cast<int32_t>(xml_bin.length()), subschema_id);
          if (xml_bin.length() > 0) {
            result.set_has_lob_header();
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported opaque type for sql udt access", K(ret), K(opaque->get_type()));
    }
    int tmp_ret = pl::ObUserDefinedType::destruct_obj(pl_obj, nullptr);
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      LOG_WARN("failed to destruct pl opaque obj on cleanup", K(tmp_ret));
    }
  }
#endif
  return ret;
}

int ObExprObjAccess::ExtraInfo::locate_record_attr_slice(const SerializedPLObjSlice &record_slice,
                                                              const SqlUdtAccessIdx &current_access,
                                                              SerializedPLObjSlice &attr_slice)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  pl::ObPLType pl_type = pl::PL_INVALID_TYPE;
  uint64_t id = OB_INVALID_ID;
  bool is_null = false;
  int32_t count = OB_INVALID_COUNT;
  int64_t metadata_len = 0;
  int64_t data_start = 0;
  int64_t begin_offset = 0;
  int64_t end_offset = 0;
  attr_slice = SerializedPLObjSlice();
  if (OB_FAIL(parse_serialized_pl_header(record_slice, pos, pl_type, id, is_null))) {
    LOG_WARN("failed to parse serialized record header", K(ret));
  } else if (pl_type != pl::PL_RECORD_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialized pl object is not record", K(ret), K(pl_type));
  } else if (is_null) {
    attr_slice.is_null_ = true;
  } else if (OB_FAIL(serialization::decode(record_slice.data_, record_slice.len_, pos, count))) {
    LOG_WARN("failed to decode record count", K(ret), K(pos));
  } else if (OB_UNLIKELY(count < 0 || current_access.var_index_ < 0 || current_access.var_index_ >= count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid record attr index", K(ret), K(count), K(current_access.var_index_));
  } else if (OB_FAIL(serialization::decode(record_slice.data_, record_slice.len_, pos, metadata_len))) {
    LOG_WARN("failed to decode record metadata length", K(ret), K(pos));
  } else if (OB_UNLIKELY(metadata_len < 0 || metadata_len > record_slice.len_ - pos)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid record metadata range", K(ret), K(pos), K(metadata_len), K(record_slice.len_));
  } else {
    pos += metadata_len;
    const int64_t bm_bytes = pl::ObPLComposite::member_null_bitmap_bytes(count);
    if (pl::ObPLComposite::member_null_bitmap_at(record_slice.data_ + pos, bm_bytes,
                                                       current_access.var_index_)) {
      attr_slice.is_null_ = true;
    } else {
      pos += bm_bytes;
      int64_t offset_array_len = 0;
      if (OB_FAIL(serialization::decode(record_slice.data_, record_slice.len_, pos, offset_array_len))) {
        LOG_WARN("failed to decode record offset array length", K(ret), K(pos));
      } else {
        data_start = pos + offset_array_len;
        OX (begin_offset = 0);
        for (int64_t i = 0; OB_SUCC(ret) && i <= current_access.var_index_; ++i) {
          int64_t offset = 0;
          if (OB_FAIL(serialization::decode(record_slice.data_, record_slice.len_, pos, offset))) {
            LOG_WARN("failed to decode record data offset", K(ret), K(i), K(pos));
          } else if (i == current_access.var_index_ - 1) {
            begin_offset = offset;
          } else if (i == current_access.var_index_) {
            end_offset = offset;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !attr_slice.is_null_) {
    if (OB_UNLIKELY(end_offset < begin_offset || data_start + end_offset > record_slice.len_)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid record attr data range", K(ret), K(data_start), K(begin_offset), K(end_offset), K(record_slice.len_));
    } else {
      attr_slice.data_ = record_slice.data_ + data_start + begin_offset;
      attr_slice.len_ = end_offset - begin_offset;
    }
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::locate_collection_elem_slice(const ParamStore &param_store,
                                                                  const ObObj *params,
                                                                  const int64_t param_num,
                                                                  const SerializedPLObjSlice &coll_slice,
                                                                  const SqlUdtAccessIdx &current_access,
                                                                  SerializedPLObjSlice &elem_slice) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  pl::ObPLType pl_type = pl::PL_INVALID_TYPE;
  uint64_t id = OB_INVALID_ID;
  bool is_null = false;
  int64_t capacity = 0;
  int64_t count = 0;
  int64_t metadata_len = 0;
  int64_t element_idx = 0;
  int64_t begin_offset = 0;
  int64_t end_offset = 0;
  int64_t data_start = 0;
  elem_slice = SerializedPLObjSlice();
  if (OB_FAIL(parse_serialized_pl_header(coll_slice, pos, pl_type, id, is_null))) {
    LOG_WARN("failed to parse serialized collection header", K(ret));
  } else if (pl_type != pl::PL_VARRAY_TYPE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported serialized collection type for direct access", K(ret), K(pl_type));
  } else if (is_null) {
    elem_slice.is_null_ = true;
  } else if (pl_type == pl::PL_VARRAY_TYPE
             && OB_FAIL(serialization::decode(coll_slice.data_, coll_slice.len_, pos, capacity))) {
    LOG_WARN("failed to decode varray capacity", K(ret), K(pos));
  } else if (OB_FAIL(serialization::decode(coll_slice.data_, coll_slice.len_, pos, count))) {
    LOG_WARN("failed to decode collection count", K(ret), K(pos));
  } else if (OB_INVALID_COUNT == count) {
    ret = OB_ERR_COLLECION_NULL;
    LOG_WARN("Reference to uninitialized collection", K(ret), K(id));
  } else if (OB_FAIL(serialization::decode(coll_slice.data_, coll_slice.len_, pos, metadata_len))) {
    LOG_WARN("failed to decode collection metadata length", K(ret), K(pos));
  } else if (OB_UNLIKELY(metadata_len < 0 || metadata_len > coll_slice.len_ - pos)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid collection metadata range", K(ret), K(pos), K(metadata_len), K(coll_slice.len_));
  } else {
    pos += metadata_len;
    if (current_access.is_const_) {
      element_idx = current_access.var_index_ - 1;
    } else if (OB_FAIL(get_obj_access_int_param(param_store, params, param_num, current_access.var_index_, element_idx))) {
      LOG_WARN("failed to get collection index", K(ret), K(current_access));
    } else {
      element_idx -= 1;
    }
    if (OB_SUCC(ret)) {
      if (element_idx < 0 || element_idx >= count) {
        ret = OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT;
        LOG_WARN("collection index out of range", K(ret), K(element_idx), K(count), K(capacity));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t bm_bytes = pl::ObPLComposite::member_null_bitmap_bytes(count);
      if (OB_UNLIKELY(pos + bm_bytes > coll_slice.len_)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("invalid collection member null bitmap range", K(ret), K(pos), K(bm_bytes), K(coll_slice.len_));
      } else if (pl::ObPLComposite::member_null_bitmap_at(coll_slice.data_ + pos, bm_bytes, element_idx)) {
        elem_slice.is_null_ = true;
      } else {
        pos += bm_bytes;
        int64_t offset_array_len = 0;
        if (OB_FAIL(serialization::decode(coll_slice.data_, coll_slice.len_, pos, offset_array_len))) {
          LOG_WARN("failed to decode collection offset array length", K(ret), K(pos));
        } else {
          data_start = pos + offset_array_len;
          OX (begin_offset = 0);
          for (int64_t i = 0; OB_SUCC(ret) && i <= element_idx; ++i) {
            int64_t offset = 0;
            if (OB_FAIL(serialization::decode(coll_slice.data_, coll_slice.len_, pos, offset))) {
              LOG_WARN("failed to decode collection data offset", K(ret), K(i), K(pos));
            } else if (OB_UNLIKELY(offset < 0 || data_start + offset > coll_slice.len_)) {
              ret = OB_DESERIALIZE_ERROR;
              LOG_WARN("invalid collection data offset", K(ret), K(i), K(offset), K(data_start), K(pos));
            } else if (i == element_idx - 1) {
              begin_offset = offset;
            } else if (i == element_idx) {
              end_offset = offset;
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !elem_slice.is_null_) {
    if (OB_UNLIKELY(end_offset < begin_offset || data_start + end_offset > coll_slice.len_)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid collection elem data range", K(ret), K(data_start), K(begin_offset), K(end_offset), K(coll_slice.len_));
    } else {
      elem_slice.data_ = coll_slice.data_ + data_start + begin_offset;
      elem_slice.len_ = end_offset - begin_offset;
    }
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::access_serialized_obj_by_idxs(ObEvalCtx &ctx,
                                                              ObIAllocator &calc_alloc,
                                                              const ParamStore &param_store,
                                                              const ObObj *params,
                                                              const int64_t param_num,
                                                              const ObString &udt_data,
                                                              ObObj &result) const
{
  int ret = OB_SUCCESS;
  SerializedPLObjSlice current(udt_data.ptr(), udt_data.length());
  SerializedPLObjSlice value_slice;
  result.reset();
  if (OB_UNLIKELY(sql_udt_access_idxs_.count() <= 1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid direct object access path", K(ret), K(sql_udt_access_idxs_.count()));
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < sql_udt_access_idxs_.count(); ++i) {
    const SqlUdtAccessIdx &parent_access = sql_udt_access_idxs_.at(i - 1);
    const SqlUdtAccessIdx &current_access = sql_udt_access_idxs_.at(i);
    value_slice = SerializedPLObjSlice();
    if (is_pl_type_collection(parent_access.var_pl_type_)) {
      if (OB_FAIL(locate_collection_elem_slice(param_store, params, param_num,
                                                    current, current_access, value_slice))) {
        LOG_WARN("failed to locate serialized collection element", K(ret), K(i), K(current_access));
      }
    } else if (OB_FAIL(locate_record_attr_slice(current, current_access, value_slice))) {
      LOG_WARN("failed to locate serialized record attr", K(ret), K(i), K(current_access));
    }
    if (OB_FAIL(ret)) {
    } else if (value_slice.is_null_) {
      result.set_null();
      break;
    } else if (i == sql_udt_access_idxs_.count() - 1) {
      uint16_t subschema_id = ObInvalidSqlType;
      if (is_pl_type_composite(current_access.var_pl_type_)) {
        const uint64_t udt_id = current_access.elem_user_type_id_;
        if (OB_FAIL(ctx.exec_ctx_.get_subschema_id_by_udt_id(udt_id, subschema_id))) {
          LOG_WARN("failed to get subschema id for sql udt composite field", K(ret), K(udt_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(deserialize_obj_value(calc_alloc, value_slice, result, subschema_id))) {
        LOG_WARN("failed to deserialize final composite access result", K(ret), K(i), K(current_access));
      }
    } else if (is_pl_type_composite(current_access.var_pl_type_)) {
      if (OB_FAIL(get_serialized_obj_payload(value_slice, current))) {
        LOG_WARN("failed to get serialized composite payload", K(ret), K(i), K(current_access));
      } else if (current.is_null_) {
        result.set_null();
        break;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("non-composite intermediate object access", K(ret), K(i), K(current_access));
    }
  }
  return ret;
}

ObExprObjAccess::ExtraInfo::ExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
    : ObIExprExtraInfo(alloc, type),
    get_attr_func_(0),
    param_idxs_(alloc),
    access_idx_cnt_(0),
    for_write_(false),
    property_type_(pl::ObCollectionType::INVALID_PROPERTY),
    coll_idx_(OB_INVALID_INDEX),
    extend_size_(0),
    sql_udt_access_(false),
    access_idxs_(alloc),
    sql_udt_access_idxs_(alloc)
{
}

OB_SERIALIZE_MEMBER(ObExprObjAccess::ExtraInfo,
                    get_attr_func_,
                    param_idxs_,
                    access_idx_cnt_,
                    for_write_,
                    property_type_,
                    coll_idx_,
                    extend_size_,
                    sql_udt_access_,
                    sql_udt_access_idxs_);

OB_SERIALIZE_MEMBER((ObExprObjAccess, ObExprOperator),
                    info_.get_attr_func_,
                    info_.param_idxs_,
                    info_.access_idx_cnt_,
                    info_.for_write_,
                    info_.property_type_,
                    info_.coll_idx_,
                    info_.sql_udt_access_,
                    info_.sql_udt_access_idxs_);
                    // extend_size_ is not needed here, we got extend size from result_type_

ObExprObjAccess::ObExprObjAccess(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_OBJ_ACCESS_REF, N_OBJ_ACCESS, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                   INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE),
    info_(alloc, T_OBJ_ACCESS_REF)
{
}

ObExprObjAccess::~ObExprObjAccess()
{
}

ObExprObjAccess::ObjAccessExprAllocatorCtx::~ObjAccessExprAllocatorCtx()
{
}

void ObExprObjAccess::ExtraInfo::reset()
{
  get_attr_func_ = 0;
  param_idxs_.reset();
  access_idx_cnt_ = 0;
  for_write_ = false;
  property_type_  = pl::ObCollectionType::INVALID_PROPERTY;
  coll_idx_ = OB_INVALID_INDEX;
  extend_size_ = 0;
  sql_udt_access_ = false;
  access_idxs_.reset();
  sql_udt_access_idxs_.reset();
}

void ObExprObjAccess::reset()
{
  info_.reset();
  ObExprOperator::reset();
}

int ObExprObjAccess::ExtraInfo::deep_copy(common::ObIAllocator &allocator,
                               const ObExprOperatorType type,
                               ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ExtraInfo &other = *static_cast<ExtraInfo *>(copied_info);
  if (OB_SUCC(ret)) {
    OZ(other.assign(*this));
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::assign(const ObExprObjAccess::ExtraInfo &other)
{
  int ret = OB_SUCCESS;
  get_attr_func_ = other.get_attr_func_;
  access_idx_cnt_ = other.access_idx_cnt_;
  for_write_ = other.for_write_;
  property_type_ = other.property_type_;
  coll_idx_ = other.coll_idx_;
  extend_size_ = other.extend_size_;
  sql_udt_access_ = other.sql_udt_access_;
  OZ(param_idxs_.assign(other.param_idxs_));
  OZ(access_idxs_.assign(other.access_idxs_));
  OZ(sql_udt_access_idxs_.assign(other.sql_udt_access_idxs_));
  return ret;
}

int ObExprObjAccess::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OBJ_ACCESS_REF) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
    const ObExprObjAccess &other_expr = static_cast<const ObExprObjAccess &>(other);
    OZ(info_.assign(other_expr.info_));
  }
  return ret;
}

#define GET_VALID_INT64_PARAM(obj, skip_check_error) \
  do { \
    if (OB_SUCC(ret)) { \
      int64_t param_value = 0; \
      if (OB_FAIL(get_int64_from_obj(obj, param_value, skip_check_error))) { \
        LOG_WARN("failed to get int64 from obj", K(ret), K(obj), K(i)); \
      } else if (OB_FAIL(param_array.push_back(param_value))) { \
        LOG_WARN("store param array failed", K(ret), K(i)); \
      } \
    } \
  } while (0)

int ObExprObjAccess::ExtraInfo::init_param_array(const ParamStore &param_store,
                                                 const ObObj *objs_stack,
                                                 int64_t param_num,
                                                 ParamArray &param_array) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_idxs_.count(); ++i) {
    CK (param_idxs_.at(i) >= 0 && param_idxs_.at(i) < param_store.count());
    if (OB_SUCC(ret)) {
      const ObObjParam &obj_param = param_store.at(param_idxs_.at(i));
      GET_VALID_INT64_PARAM(obj_param, false);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    const ObObj &obj = objs_stack[i];
    GET_VALID_INT64_PARAM(obj,
      (i == param_num - 1
        && (pl::ObCollectionType::NEXT_PROPERTY == property_type_
            || pl::ObCollectionType::EXISTS_PROPERTY == property_type_
            || pl::ObCollectionType::PRIOR_PROPERTY == property_type_)));
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::update_coll_first_last(
  const ParamStore &param_store, const ObObj *objs_stack, int64_t param_num) const
{
  int ret = OB_SUCCESS;
  bool found = false;

#define SEARCH_AND_UPDATE_COLL(get_elem, count) \
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < count; ++i) { \
    const ObObj &obj = get_elem; \
    if (obj.is_pl_extend() \
        && (pl::PL_NESTED_TABLE_TYPE == obj.get_meta().get_extend_type() \
            || pl::PL_VARRAY_TYPE == obj.get_meta().get_extend_type())) { \
      found = true; \
      pl::ObPLCollection *coll = reinterpret_cast<pl::ObPLCollection *>(obj.get_ext()); \
      CK (OB_NOT_NULL(coll)); \
      OX (coll->set_first(OB_INVALID_INDEX)); \
      OX (coll->set_last(OB_INVALID_INDEX)); \
      OX (found = true); \
    } \
  }

  SEARCH_AND_UPDATE_COLL(objs_stack[i], param_num);
  SEARCH_AND_UPDATE_COLL(param_store.at(i), param_store.count());

#undef SEARCH_AND_UPDATE_COLL
  return ret;
}

int ObExprObjAccess::calc_result(ObObj &result,
                                 ObIAllocator &alloc,
                                 const ObObj *objs_stack,
                                 int64_t param_num,
                                 const ParamStore &param_store,
                                 ObEvalCtx *ctx) const
{

  return info_.calc(result,
                    alloc,
                    get_result_type(),
                    info_.extend_size_,
                    param_store,
                    objs_stack,
                    param_num,
                    ctx);
}

int ObExprObjAccess::ExtraInfo::get_collection_attr(int64_t* params,
                                                    const pl::ObObjAccessIdx &current_access,
                                                    bool for_write,
                                                    bool is_assoc_array,
                                                    void *&current_value,
                                                    void *&current_allocator,
                                                    ObSQLSessionInfo *session_info) const
{
  int ret = OB_SUCCESS;
  pl::ObPLCollection *current_coll = reinterpret_cast<pl::ObPLCollection*>(current_value);
  int64_t element_idx;
  CK (OB_NOT_NULL(current_coll));
  if (OB_SUCC(ret) && !current_coll->is_inited()) {
    ret = OB_ERR_COLLECION_NULL;
    LOG_WARN("Reference to uninitialized collection", K(ret), KPC(current_coll));
  }
  if (OB_SUCC(ret) && !current_access.is_property()) {
    if (current_access.is_const()) {
      element_idx = current_access.var_index_ - 1;
    } else {
      element_idx = params[current_access.var_index_] - 1;
    }
    if (element_idx < 0 || element_idx >= current_coll->get_count()) {
      ret = is_assoc_array ? OB_READ_NOTHING : OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT;
      LOG_WARN("", K(ret), K(element_idx));
    }
  }
  if (OB_SUCC(ret) && !current_access.is_property()) {
    ObObj &element_obj = current_coll->get_data()[element_idx];
    if (for_write_) {
      current_allocator = current_coll->get_allocator();
    }
    if (ObMaxType == element_obj.get_type()) {
      if (!for_write) {
        ret = OB_READ_NOTHING;
        LOG_WARN("", K(ret), KPC(current_coll));
      } else {
        if (current_access.var_type_.is_composite_type()) {
          const pl::ObUserDefinedType *type = nullptr;
          int64_t ptr = 0;
          int64_t init_size = OB_INVALID_SIZE;
          const pl::ObPLINS *ns = session_info->get_pl_top_context()->get_current_ctx();
          CK (OB_NOT_NULL(ns));
          OZ (ns->get_user_type(current_access.var_type_.get_user_type_id(), type));
          CK (OB_NOT_NULL(type));
          CK (type->is_composite_type());
          OZ (type->newx(*current_coll->get_allocator(), ns, ptr));
          if (OB_FAIL(ret)) {
          } else if (type->is_collection_type()) {
            pl::ObPLCollection *collection = NULL;
            CK (OB_NOT_NULL(collection = reinterpret_cast<pl::ObPLCollection*>(ptr)));
            OX (collection->set_count(0));
          } else if (type->is_record_type()) {
            pl::ObPLRecord *record = NULL;
            CK (OB_NOT_NULL(record = reinterpret_cast<pl::ObPLRecord*>(ptr)));
            OX (record->set_null());
          }
          OZ (type->get_size(pl::PL_TYPE_INIT_SIZE, init_size));
          OX (element_obj.set_extend(ptr, type->get_type(), init_size));
        }
      }
    }
    OX (current_value = &element_obj);
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::get_record_attr(const pl::ObObjAccessIdx &current_access,
                                                uint64_t udt_id,
                                                bool for_write,
                                                void *&current_value,
                                                ObEvalCtx &ctx,
                                                void *&current_allocator) const
{
  int ret = OB_SUCCESS;

  ObArenaAllocator alloc;
  const pl::ObUserDefinedType *user_type = NULL;
  const pl::ObRecordType *record_type = NULL;
  pl::ObPLComposite *current_composite = reinterpret_cast<pl::ObPLComposite*>(current_value);
  pl::ObPLRecord* current_record = static_cast<pl::ObPLRecord*>(current_composite);
  ObObj* element_obj = NULL;
  CK (OB_NOT_NULL(current_composite));
  CK (current_composite->is_record());
  CK (OB_NOT_NULL(current_record));
  CK (OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
  CK (OB_NOT_NULL(ctx.exec_ctx_.get_sql_ctx()));
  CK (OB_NOT_NULL(ctx.exec_ctx_.get_sql_ctx()->schema_guard_));
  CK (OB_NOT_NULL(ctx.exec_ctx_.get_sql_proxy()));
  if (OB_FAIL(ret)) {
  } else if (ctx.exec_ctx_.get_my_session()->get_pl_top_context()) {
    pl::ObPLINS *ns = ctx.exec_ctx_.get_my_session()->get_pl_top_context()->get_current_ctx();
    CK (OB_NOT_NULL(ns));
    OZ (ns->get_user_type(udt_id, user_type));
  } else {
    pl::ObPLPackageGuard *package_guard = NULL;
    OZ (ctx.exec_ctx_.get_package_guard(package_guard));
    CK (OB_NOT_NULL(package_guard));
    if (OB_SUCC(ret)) {
      pl::ObPLResolveCtx resolve_ctx(alloc,
                                    *ctx.exec_ctx_.get_my_session(),
                                    *ctx.exec_ctx_.get_sql_ctx()->schema_guard_,
                                    *package_guard,
                                    *ctx.exec_ctx_.get_sql_proxy(),
                                    false);
      OZ (resolve_ctx.get_user_type(udt_id, user_type));
    }
  }
  CK (OB_NOT_NULL(user_type));
  CK (user_type->is_record_type());
  CK (OB_NOT_NULL(record_type = static_cast<const pl::ObRecordType*>(user_type)));
  CK (current_access.is_const());
  if (for_write_) {
    OX (current_allocator = current_record->get_allocator());
  }
  if (OB_SUCC(ret) && user_type->is_object_type() && for_write_ && current_composite->is_null()) {
    ret = OB_ERR_ACCESS_INTO_NULL;
    LOG_WARN("", K(ret), KPC(current_composite));
  }
  OZ (current_record->get_element(current_access.var_index_, element_obj));
  CK (OB_NOT_NULL(current_value = element_obj));

  return ret;
}

int ObExprObjAccess::ExtraInfo::get_attr_func(int64_t param_cnt,
                                              int64_t *params,
                                              int64_t *element_val,
                                              ObEvalCtx &ctx,
                                              int64_t *allocator_addr,
                                              ObSQLSessionInfo *session_info) const
{
  int ret = OB_SUCCESS;
  void *current_value = NULL;
  void *current_allocator = NULL;
  CK (OB_NOT_NULL(element_val));
  CK (OB_NOT_NULL(allocator_addr));
  CK (access_idxs_.count() > 0);
  if (OB_SUCC(ret)) {
    pl::ObPLComposite *composite_addr
    = reinterpret_cast<pl::ObPLComposite*>(params[access_idxs_.at(0).var_index_]);
    for (int64_t i = 1; OB_SUCC(ret) && i < access_idxs_.count(); ++i) {
      const pl::ObPLDataType &parent_type = access_idxs_.at(i - 1).var_type_;
      const pl::ObObjAccessIdx &parent_access = access_idxs_.at(i - 1);
      const pl::ObObjAccessIdx &current_access = access_idxs_.at(i);
      current_value = composite_addr;
      if (parent_type.is_collection_type()) {
        OZ (get_collection_attr(params,
                                current_access,
                                for_write_,
                                parent_type.is_associative_array_type(),
                                current_value,
                                current_allocator,
                                session_info));
      } else {
        OZ (get_record_attr(current_access,
                            parent_type.get_user_type_id(),
                            for_write_,
                            current_value,
                            ctx,
                            current_allocator));
      }
      if (OB_FAIL(ret)) {
      } else if (current_access.var_type_.is_composite_type()) {
        ObObj* value = reinterpret_cast<ObObj*>(current_value);
        CK (OB_NOT_NULL(value));
        CK (value->is_ext());
        OX (composite_addr = reinterpret_cast<pl::ObPLComposite*>(value->get_ext()));
        CK (OB_NOT_NULL(composite_addr));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (pl::ObObjAccessIdx::get_final_type(access_idxs_).is_obj_type()) {
      *element_val = reinterpret_cast<int64_t>(current_value);
    } else {
      *element_val = reinterpret_cast<int64_t>(composite_addr);
    }
    if (OB_SUCC(ret) &&
        for_write_ &&
        (access_idxs_.at(0).var_type_.is_record_type() ||
         access_idxs_.at(0).var_type_.is_collection_type())) {
      if (1 == access_idxs_.count()) {
        pl::ObPLAllocator1 *alloc = nullptr;
        CK (OB_NOT_NULL(composite_addr->get_allocator()));
        OX (alloc = dynamic_cast<pl::ObPLAllocator1 *>(composite_addr->get_allocator()));
        CK (OB_NOT_NULL(alloc));
        CK (OB_NOT_NULL(alloc->get_parent_allocator()));
        OX (*allocator_addr = reinterpret_cast<int64_t>(alloc->get_parent_allocator()));
      } else {
        *allocator_addr = reinterpret_cast<int64_t>(current_allocator);
      }
    }
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::calc_sql_udt_access(ObObj &result,
                                                           ObIAllocator &alloc,
                                                           const ParamStore &param_store,
                                                           const common::ObObj *params,
                                                           int64_t param_num,
                                                           ObEvalCtx &ctx) const
{
  int ret = OB_SUCCESS;
  const ObObj *root_obj = NULL;
  if (OB_UNLIKELY(sql_udt_access_idxs_.count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_udt_access_idxs_ is empty in sql udt access path",
             K(ret), K(sql_udt_access_idxs_.count()),
             K(access_idxs_.count()), K(sql_udt_access_));
  } else if (OB_FAIL(get_obj_access_param(param_store, params, param_num,
                                   sql_udt_access_idxs_.at(0).var_index_, root_obj))) {
    LOG_WARN("failed to get object access root param", K(ret), K(sql_udt_access_idxs_));
  } else if (OB_ISNULL(root_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root object is null", K(ret));
  } else if (root_obj->is_null()) {
    result.set_null();
  } else if (!root_obj->is_user_defined_sql_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("non-sql udt is not supported for sql udt access", K(ret), KPC(root_obj));
  } else {
    ObString udt_data = root_obj->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&alloc,
                                                          ObLongTextType,
                                                          CS_TYPE_BINARY,
                                                          true,
                                                          udt_data))) {
      LOG_WARN("failed to read sql udt data", K(ret), KPC(root_obj));
    } else if (udt_data.empty()) {
      result.set_null();
    } else if (OB_FAIL(access_serialized_obj_by_idxs(ctx, alloc, param_store,
                                                     params, param_num,
                                                     udt_data, result))) {
      LOG_WARN("failed to access serialized sql udt", K(ret), K(sql_udt_access_idxs_));
    }
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::calc_pl_extend_access(ObObj &result,
                                                      ObIAllocator &alloc,
                                                      const ObObjMeta &res_type,
                                                      const int32_t extend_size,
                                                      const ParamStore &param_store,
                                                      const common::ObObj *params,
                                                      int64_t param_num,
                                                      ObEvalCtx &ctx,
                                                      ObjAccessExprAllocatorCtx *access_obj_ctx) const
{
  int ret = OB_SUCCESS;
  typedef int32_t (*GetAttr)(int64_t, int64_t [], int64_t *, int64_t *);
  GetAttr get_attr = reinterpret_cast<GetAttr>(get_attr_func_);
  ParamArray param_array(&alloc);

  OZ (init_param_array(param_store, params, param_num, param_array));

  if (OB_SUCC(ret)) {
    int64_t *param_ptr = const_cast<int64_t *>(param_array.head());
    int64_t attr_addr = 0;
    int64_t allocator_addr = 0;
    if (!for_write_ && OB_NOT_NULL(get_attr)) {
      OZ (get_attr(param_array.count(), param_ptr, &attr_addr, &allocator_addr));
    } else {
      OZ (get_attr_func(param_array.count(), param_ptr, &attr_addr, ctx,
                        &allocator_addr, ctx.exec_ctx_.get_my_session()));
    }
    if (OB_FAIL(ret)) {
      if (OB_ERR_COLLECION_NULL == ret && pl::ObCollectionType::EXISTS_PROPERTY == property_type_) {
        ret = OB_SUCCESS;
        result.set_tinyint(0);
      }
    } else if (OB_UNLIKELY(0 >= attr_addr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get attribute failed", K(ret), K(attr_addr));
    } else if (for_write_ || res_type.is_ext()) {
      // 当对collection元素赋值的时候，强制设置collection的first和last为invalid
      // 在获取first，last的时候，会根据这个标记进行更新。
      // 主要是因为collection有删除操作的时候，而又赋值了，需要一个地方给出提示
      // 这儿就是一个标记，需要明确的，这可能会导致first last暂时不准确。但get_first会自动更新
      // 所有需要使用first，last的地方应该调用get_first这样的函数，而不能直接使用first的值
      // assoc array 不需要处理，因为在accociative_index里面更新了first，last
      if (for_write_ && OB_INVALID_INDEX != coll_idx_) {
        if (0 != param_array.at(coll_idx_)) {
          pl::ObPLCollection *coll = reinterpret_cast<pl::ObPLCollection*>(param_array.at(coll_idx_));
          if (pl::PL_NESTED_TABLE_TYPE == coll->get_type()
              || pl::PL_VARRAY_TYPE == coll->get_type()) {
            OX (coll->set_first(OB_INVALID_INDEX));
            OX (coll->set_last(OB_INVALID_INDEX));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (for_write_) {
        pl::ObPlCompiteWrite *composite_write = nullptr;
        if (access_obj_ctx == NULL) {
          // alloc is ctx->get_top_expr_allocator() in ObSPIService::calc_obj_access_expr
          if (OB_ISNULL(composite_write =
              static_cast<pl::ObPlCompiteWrite *>(alloc.alloc(sizeof(pl::ObPlCompiteWrite))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloca memory", K(ret));
          }
        } else {
          // here expr is from ObExprObjAccess::eval_obj_access, use obj_access_ctx.allocator_
          if (OB_ISNULL(composite_write =
              reinterpret_cast<pl::ObPlCompiteWrite *>(access_obj_ctx->get_allocator().alloc(
                        sizeof(pl::ObPlCompiteWrite))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloca memory", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          composite_write->allocator_ = allocator_addr;
          composite_write->value_addr_ = attr_addr;
          result.set_extend(reinterpret_cast<int64_t>(composite_write), res_type.get_extend_type(), extend_size);
        }
      } else {
        OX(result.set_extend(attr_addr, res_type.get_extend_type(), extend_size));
      }
#ifdef OB_BUILD_ORACLE_PL
    } else if (pl::ObCollectionType::INVALID_PROPERTY != property_type_) {
      pl::ObPLCollection *coll = reinterpret_cast<pl::ObPLCollection*>(attr_addr);
      pl::ObPLAssocArray *assoc =
        (OB_NOT_NULL(coll) && coll->is_associative_array())
            ? static_cast<pl::ObPLAssocArray *>(coll) : NULL;
      CK (OB_NOT_NULL(coll));
      if (OB_SUCC(ret)) {
        switch (property_type_) {
          case pl::ObCollectionType::FIRST_PROPERTY: {
            OZ (OB_NOT_NULL(assoc) ? assoc->first(result) : coll->first(result));
          } break;
          case pl::ObCollectionType::LAST_PROPERTY: {
            OZ (OB_NOT_NULL(assoc) ? assoc->last(result) : coll->last(result));
          } break;
          case pl::ObCollectionType::PRIOR_PROPERTY: {
            CK (param_array.count() > 1);
            int64_t idx = param_array.count() - 1;
            OZ (OB_NOT_NULL(assoc)
              ? assoc->prior(param_array.at(idx), result) : coll->prior(param_array.at(idx), result));
          } break;
          case pl::ObCollectionType::NEXT_PROPERTY: {
            CK (param_array.count() > 1);
            int64_t idx = param_array.count() - 1;
            int64_t index_val = param_array.at(idx);
            if (OB_SUCC(ret)) {
              if(OB_INVALID_INDEX == param_array.at(idx)) {
                if (OB_NOT_NULL(assoc)) {
                  OZ (assoc->next(index_val, result));
                } else {
                  result.set_null();
                }
              } else {
                OZ (OB_NOT_NULL(assoc) ? assoc->next(index_val, result) : coll->next(index_val, result));
              }
            }
          } break;
          case pl::ObCollectionType::EXISTS_PROPERTY: {
            CK (param_array.count() > 1);
            int64_t idx = param_array.count() - 1;
            OZ (OB_NOT_NULL(assoc)
              ? assoc->exist(param_array.at(idx), result) : coll->exist(param_array.at(idx), result));
          } break;
          case pl::ObCollectionType::COUNT_PROPERTY: {
            result.set_int(coll->get_actual_count());
          } break;
          case pl::ObCollectionType::LIMIT_PROPERTY: {
            if (coll->is_varray()) {
              pl::ObPLVArray *varr = static_cast<pl::ObPLVArray *>(coll);
              CK (OB_NOT_NULL(varr));
              OX (result.set_int(varr->get_capacity()));
            } else {
              result.set_null();
            }
          } break;
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid property type", K(ret), K(property_type_));
          } break;
        }
      }
#endif
    } else {
      ObObj *datum = reinterpret_cast<ObObj*>(attr_addr);
      if (ObMaxType == datum->get_type()) { //means has been deleted
        ret = OB_READ_NOTHING;
        LOG_WARN("accessing deleted element, no data found", K(ret), KPC(datum), K(result));
      } else if (res_type.is_number() && datum->is_decimal_int()) {
        ObCastCtx cast_ctx(&alloc, NULL, CM_NONE, res_type.get_collation_type(), NULL);
        const ObObj *res_obj = nullptr;
        if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, *datum, result, res_obj))) {
          LOG_WARN("failed to cast decimal int to number", K(ret));
        }
      } else if (OB_FAIL(result.apply(*datum))) {
        LOG_WARN("apply failed", K(ret), KPC(datum), K(result), K(res_type));
      }
      if (OB_SUCC(ret)) {
        if ((ObLongTextType == result.get_meta().get_type()
            && res_type.is_lob_locator())
        || (result.get_meta().is_lob_locator() && ObLongTextType == res_type.get_type())) {
        } else if (!result.is_null()
                  && result.get_meta().get_type() != res_type.get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("obj access result meta not equel to expr type",
            K(ret), KPC(datum), K(result), K(res_type));
        }
      }
    }
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::calc(ObObj &result,
                                     ObIAllocator &alloc,
                                     const ObObjMeta &res_type,
                                     const int32_t extend_size,
                                     const ParamStore &param_store,
                                     const common::ObObj *params,
                                     int64_t param_num,
                                     ObEvalCtx *ctx,
                                     ObjAccessExprAllocatorCtx *access_obj_ctx) const
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx));

  if (OB_FAIL(ret)) {
  } else if (!sql_udt_access_) {
    OZ (calc_pl_extend_access(result, alloc, res_type, extend_size, param_store,
      params, param_num, *ctx, access_obj_ctx));
  } else {
    OZ (calc_sql_udt_access(result, alloc, param_store, params, param_num, *ctx));
  }
  return ret;
}

int ObExprObjAccess::ExtraInfo::from_raw_expr(const ObObjAccessRawExpr &raw_access)
{
  int ret = 0;
  if (OB_SUCC(ret)) {
    extend_size_ = raw_access.get_extend_size();
    get_attr_func_ = raw_access.get_get_attr_func_addr();
    for_write_ = raw_access.for_write();
    property_type_ = raw_access.get_property();
    access_idx_cnt_ = raw_access.get_access_idxs().count();
    sql_udt_access_ = raw_access.is_sql_udt_access();
    coll_idx_ = OB_INVALID_INDEX;
    if (raw_access.get_access_idxs().at(0).elem_type_.is_collection_type()) {
      coll_idx_ = raw_access.get_access_idxs().at(0).var_index_;
    }
    OZ(param_idxs_.init(raw_access.get_var_indexs().count()));
    OZ(param_idxs_.assign(raw_access.get_var_indexs()));
    OZ(access_idxs_.init(raw_access.get_access_idxs().count()));
    OZ(access_idxs_.assign(raw_access.get_access_idxs()));
    if (OB_SUCC(ret) && sql_udt_access_) {
      const ObIArray<pl::ObObjAccessIdx> &raw_idxs = raw_access.get_access_idxs();
      OZ(sql_udt_access_idxs_.init(raw_idxs.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < raw_idxs.count(); ++i) {
        const pl::ObObjAccessIdx &src = raw_idxs.at(i);
        SqlUdtAccessIdx idx;
        idx.var_index_ = src.var_index_;
        idx.elem_user_type_id_ = src.elem_type_.get_user_type_id();
        idx.var_pl_type_ = static_cast<int32_t>(src.var_type_.get_type());
        idx.is_const_ = src.is_const();
        OZ(sql_udt_access_idxs_.push_back(idx));
      }
    }
  }
  return ret;
}

int ObExprObjAccess::cg_expr(ObExprCGCtx &op_cg_ctx,
                             const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  ExtraInfo *info = OB_NEWx(ExtraInfo, (&alloc), alloc, T_OBJ_ACCESS_REF);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    const ObObjAccessRawExpr &raw_access = static_cast<const ObObjAccessRawExpr &>(raw_expr);
    if (OB_SUCC(ret)) {
      info->extend_size_ = raw_access.get_extend_size();
      info->get_attr_func_ = raw_access.get_get_attr_func_addr();
      info->for_write_ = raw_access.for_write();
      info->property_type_ = raw_access.get_property();
      info->access_idx_cnt_ = raw_access.get_access_idxs().count();
      info->sql_udt_access_ = raw_access.is_sql_udt_access();
      int64_t coll_idx = OB_INVALID_INDEX;
      if (raw_access.get_access_idxs().at(0).elem_type_.is_collection_type()) {
        coll_idx = raw_access.get_access_idxs().at(0).var_index_;
      }
      info->coll_idx_ = coll_idx;
      OZ(info->param_idxs_.init(raw_access.get_var_indexs().count()));
      OZ(info->param_idxs_.assign(raw_access.get_var_indexs()));
      OZ(info->access_idxs_.init(raw_access.get_access_idxs().count()));
      OZ(info->access_idxs_.assign(raw_access.get_access_idxs()));
      if (OB_SUCC(ret) && info->sql_udt_access_) {
        const ObIArray<pl::ObObjAccessIdx> &raw_idxs = raw_access.get_access_idxs();
        OZ(info->sql_udt_access_idxs_.init(raw_idxs.count()));
        for (int64_t i = 0; OB_SUCC(ret) && i < raw_idxs.count(); ++i) {
          const pl::ObObjAccessIdx &src = raw_idxs.at(i);
          ExtraInfo::SqlUdtAccessIdx idx;
          idx.var_index_ = src.var_index_;
          idx.elem_user_type_id_ = src.elem_type_.get_user_type_id();
          idx.var_pl_type_ = static_cast<int32_t>(src.var_type_.get_type());
          idx.is_const_ = src.is_const();
          OZ(info->sql_udt_access_idxs_.push_back(idx));
        }
      }
    }
    if (OB_SUCC(ret)) {
      rt_expr.extra_info_ = info;
      rt_expr.eval_func_ = eval_obj_access;
    }
  }
  return ret;
}

int ObExprObjAccess::eval_obj_access(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  const ExtraInfo *info = static_cast<const ExtraInfo *>(expr.extra_info_);
  ObPhysicalPlanCtx *plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();

  OZ(expr.eval_param_value(ctx));
  ObObj params[expr.arg_cnt_];
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    const ObExpr *e = expr.args_[i];
    const ObDatum &d = e->locate_expr_datum(ctx);
    OZ(d.to_obj(params[i], e->obj_meta_, e->obj_datum_map_));
  }

  ObObj result;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObjAccessExprAllocatorCtx *access_obj_ctx = NULL;
  OZ(build_obj_access_ctx(expr, ctx.exec_ctx_, access_obj_ctx));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx is null", K(ret));
  } else {
    ParamStore *tmp_param_store = nullptr;
    void *tmp_param_store_buf = nullptr;
    const ParamStore *effective_param_store_ptr = nullptr;
    const ObPhysicalPlan *phy_plan = plan_ctx->get_phy_plan();
    // if the param datum frame is inited and is batched multi stmt, construct param_store from param_frame.
    if (plan_ctx->is_param_datum_frame_inited() && OB_NOT_NULL(phy_plan) && phy_plan->get_is_batched_multi_stmt()) {
      DatumParamStore &datum_param_store = plan_ctx->get_datum_param_store();
      if (OB_ISNULL(tmp_param_store_buf = alloc_guard.get_allocator().alloc(sizeof(ParamStore)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for tmp param store", K(ret));
      } else {
        tmp_param_store = new(tmp_param_store_buf) ParamStore(ObWrapperAllocator(alloc_guard.get_allocator()));
        OZ (tmp_param_store->reserve(datum_param_store.count()));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < datum_param_store.count(); i++) {
        // get the latest datum from param_frame, combine it with the meta from datum_param_store, and construct param_store.
        ObDatum *datum = nullptr;
        ObEvalInfo *eval_info = nullptr;
        VectorHeader *vec_header = nullptr;
        plan_ctx->get_param_frame_info(i, datum, eval_info, vec_header);

        ObDatumObjParam &datum_param = datum_param_store.at(i);
        ObObjParam obj_param;
        ObObjMeta meta;

        if (datum_param.meta_.is_ext_sql_array()) {
          const ObSqlDatumArray *datum_array = datum_param.get_sql_datum_array();
          meta = datum_array->element_.get_meta_type();
          obj_param.set_accuracy(datum_array->element_.get_accuracy());
        } else {
          meta.set_type(datum_param.meta_.type_);
          meta.set_collation_type(datum_param.meta_.cs_type_);
          meta.set_scale(datum_param.meta_.scale_);
          obj_param.set_accuracy(datum_param.get_accuracy());
        }

        if (OB_FAIL(datum->to_obj(obj_param, meta, ObDatum::get_obj_datum_map_type(meta.get_type())))) {
          LOG_WARN("failed to convert datum to obj", K(ret), K(i), K(*datum), K(meta));
        } else {
          if (OB_FAIL(tmp_param_store->push_back(obj_param))) {
            LOG_WARN("failed to push back param", K(ret), K(i));
          }
        }
      }
      effective_param_store_ptr = tmp_param_store;
    } else {
      effective_param_store_ptr = &plan_ctx->get_param_store();
    }

    CK (OB_NOT_NULL(effective_param_store_ptr));
    if (OB_SUCC(ret)) {
      OZ(info->calc(result,
                    alloc_guard.get_allocator(),
                    expr.obj_meta_,
                    info->extend_size_,
                    *effective_param_store_ptr,
                    params,
                    expr.arg_cnt_,
                    &ctx,
                    access_obj_ctx));
    }
  }

  OZ(expr_datum.from_obj(result, expr.obj_datum_map_));
  if (OB_SUCC(ret) && is_lob_storage(result.get_type())) {
    OZ (ob_adjust_lob_datum(result, expr.obj_meta_, ctx.exec_ctx_.get_allocator(), expr_datum));
  }
  if (OB_SUCC(ret) && info->sql_udt_access_) {
    OZ(expr.deep_copy_datum(ctx, expr_datum));
  }
  return ret;
}

int ObExprObjAccess::build_obj_access_ctx(const ObExpr &expr,
                      ObExecContext &exec_ctx,
                      ObjAccessExprAllocatorCtx *&obj_access_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t obj_access_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  if (OB_ISNULL(obj_access_ctx =
      static_cast<ObjAccessExprAllocatorCtx *>(exec_ctx.get_expr_op_ctx(obj_access_ctx_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(obj_access_ctx_id, obj_access_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(obj_access_ctx_id));
    } else if (OB_ISNULL(obj_access_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected obj_access ctx", K(ret), KP(obj_access_ctx));
    }
  } else {
    obj_access_ctx->reuse();
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
