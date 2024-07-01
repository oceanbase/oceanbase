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
 * This file is for implement of expr lob utils
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

int ObTextStringObObjResult::init(int64_t res_len, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (is_init_) {
    LOG_WARN("Lob: textstring result init already", K(ret), K(*this));
  } else if (OB_ISNULL(allocator) && OB_ISNULL(params_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Lob: invalid arguments", K(ret), K(type_), KP(params_), KP(allocator));
  } else if (OB_ISNULL(res_obj_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Lob: invalid arguments", K(ret), K(type_), KP(res_obj_));
  } else if (OB_FAIL(ObTextStringResult::calc_buffer_len(res_len))) {
    LOG_WARN("Lob: calc buffer len failed", K(ret), K(type_), KP(res_len));
  } else if (buff_len_ == 0) {
    OB_ASSERT(has_lob_header_ == false); // empty result without header
  } else {
    buffer_ = OB_ISNULL(allocator)
              ? static_cast<char*>(params_->alloc(buff_len_)) : static_cast<char*>(allocator->alloc(buff_len_));
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Lob: alloc buffer failed", K(ret), KP(params_), KP(allocator), K(buff_len_));
    } else if (OB_FAIL(fill_temp_lob_header(res_len))) {
      LOG_WARN("Lob: fill_temp_lob_header failed", K(ret), K(type_), K(res_len));
    }
    if (OB_SUCC(ret)) {
      is_init_ = true;
    }
  }
  return ret;
}

void ObTextStringObObjResult::set_result()
{
  res_obj_->set_lob_value(type_, buffer_, pos_);
  if (is_lob_storage(type_) && has_lob_header_) {
    // Notice: should not be null or nop
    res_obj_->set_has_lob_header();
  }
}

int ObTextStringHelper::build_text_iter(
    ObTextStringIter &text_iter,
    ObExecContext *exec_ctx,
    const sql::ObBasicSessionInfo *session,
    ObIAllocator *res_allocator,
    ObIAllocator *tmp_allocator)
{
  int ret = OB_SUCCESS;
  ObLobAccessCtx *lob_access_ctx = nullptr;
  if (OB_NOT_NULL(exec_ctx) && OB_FAIL(exec_ctx->get_lob_access_ctx(lob_access_ctx))) {
    LOG_WARN("get_lob_access_ctx fail", K(ret));
  } else if (OB_FAIL(text_iter.init(0/*buffer_len*/, session, res_allocator, tmp_allocator, lob_access_ctx))) {
    LOG_WARN("init lob str iter fail", K(ret), K(text_iter));
  }
  return ret;
}

int ObTextStringHelper::read_real_string_data(
    ObIAllocator *allocator,
    ObObjType type,
    ObCollationType cs_type,
    bool has_lob_header,
    ObString &str,
    sql::ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  if (is_lob_storage(type)) {
    uint64_t tenant_id = MTL_ID();
    ObArenaAllocator *tmp_alloc_ptr = nullptr;
    ObArenaAllocator tmp_allocator("ObLobRRSD", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
    if (tenant_id != OB_INVALID_TENANT_ID) {
      tmp_alloc_ptr = &tmp_allocator;
    }
    ObTextStringIter str_iter(type, cs_type, str, has_lob_header);
    if (OB_FAIL(build_text_iter(str_iter, exec_ctx, nullptr/*session*/, allocator, tmp_alloc_ptr))) {
      LOG_WARN("Lob: init lob str iter failed ", K(ret), K(str_iter));
    } else if (OB_FAIL(str_iter.get_full_data(str))) {
      COMMON_LOG(WARN, "Lob: str iter get full data failed ", K(ret), K(str_iter));
    }
  }
  return ret;
}

int ObTextStringHelper::read_real_string_data(
    ObIAllocator *allocator,
    const common::ObObj &obj,
    ObString &str,
    sql::ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  const ObObjMeta& meta = obj.get_meta();
  str = obj.get_string();
  if (meta.is_null()) {
    str.reset();
  } else if (OB_FAIL(read_real_string_data(
      allocator,
      meta.get_type(),
      meta.get_collation_type(),
      obj.has_lob_header(),
      str,
      exec_ctx))) {
    COMMON_LOG(WARN, "read_real_string_data fail", K(ret));
  }
  return ret;
}

int ob_adjust_lob_datum(const ObObj &origin_obj,
                        const common::ObObjMeta &obj_meta,
                        const ObObjDatumMapType &obj_datum_map_,
                        ObIAllocator &allocator,
                        ObDatum &out_datum)
{
  int ret = OB_SUCCESS;
  if (!is_lob_storage(origin_obj.get_type())) { // null & nop is not lob
  } else if (origin_obj.has_lob_header() != obj_meta.has_lob_header()) {
    if (origin_obj.has_lob_header()) { // obj_meta does not have lob header, get data only
      // can avoid allocator if no persist lobs call this function,
      OB_ASSERT(origin_obj.is_persist_lob() == false);

      ObString full_data;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator, origin_obj, full_data))) {
        LOG_WARN("Lob: failed to get full data", K(ret));
      } else {
        out_datum.set_string(full_data);
      }
    } else { // origin obj does not have lob header, but meta has, build temp lob header
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect for input obj, input obj should has lob header", K(ret), K(origin_obj), K(obj_meta));
      // ObObj out_obj(origin_obj);
      // if (OB_FAIL(ObTextStringResult::ob_convert_obj_temporay_lob(out_obj, allocator))) {
      //   LOG_WARN("Lob: failed to convert plain lob data to temp lob", K(ret));
      // } else if (OB_FAIL(out_datum.from_obj(out_obj, obj_datum_map_))) {
      //   LOG_WARN("convert lob obj to datum failed", K(ret), K(out_obj));
      // }
    }
  }
  return ret;
}

int ob_adjust_lob_datum(const ObObj &origin_obj,
                        const common::ObObjMeta &obj_meta,
                        ObIAllocator &allocator,
                        ObDatum &out_datum)
{
  return ob_adjust_lob_datum(origin_obj, obj_meta, allocator, &out_datum);
}

int ob_adjust_lob_datum(const ObObj &origin_obj,
                        const common::ObObjMeta &obj_meta,
                        ObIAllocator &allocator,
                        ObDatum *out_datum)
{
  int ret = OB_SUCCESS;
  if (!is_lob_storage(origin_obj.get_type())) { // null & nop is not lob
  } else if (origin_obj.has_lob_header() != obj_meta.has_lob_header()) {
    if (origin_obj.has_lob_header()) { // obj_meta does not have lob header, get data only
      // can avoid allocator if no persist lobs call this function,
      OB_ASSERT(origin_obj.is_persist_lob() == false);

      ObString full_data;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator, origin_obj, full_data))) {
        LOG_WARN("Lob: failed to get full data", K(ret));
      } else {
        out_datum->set_string(full_data);
      }
    } else { // origin obj does not have lob header, but meta has, build temp lob header
      // use by not strict default value add lob header
      ObObj out_obj(origin_obj);
      if (OB_FAIL(ObTextStringResult::ob_convert_obj_temporay_lob(out_obj, allocator))) {
        LOG_WARN("Lob: failed to convert plain lob data to temp lob", K(ret));
      } else if (OB_FAIL(out_datum->from_obj(out_obj))) {
        LOG_WARN("convert lob obj to datum failed", K(ret), K(out_obj));
      }
    }
  }
  return ret;
}

/// only called in ObExprValuesOp::calc_next_row
int ob_adjust_lob_datum(ObDatum &datum,
                        const common::ObObjMeta &in_obj_meta,
                        const common::ObObjMeta &out_obj_meta,
                        ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (!is_lob_storage(in_obj_meta.get_type())) { // null & nop is not lob
  } else if (in_obj_meta.get_type_class() != out_obj_meta.get_type_class()) {
    // lob casted to other type do nothing
  } else if (in_obj_meta.has_lob_header() != out_obj_meta.has_lob_header()) {
    if (in_obj_meta.has_lob_header()) { // obj_meta does not have lob header, get data only
      // can avoid allocator if no persist lobs call this function ?
      ObString full_data = datum.get_string();
      ObLobLocatorV2 lob(full_data, in_obj_meta.has_lob_header());
      OB_ASSERT(lob.is_persist_lob() == false);
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator,
                                                            in_obj_meta.get_type(),
                                                            in_obj_meta.get_collation_type(),
                                                            in_obj_meta.has_lob_header(),
                                                            full_data))) {
        LOG_WARN("Lob: failed to get full data", K(ret));
      } else {
        datum.set_string(full_data);
      }
    } else { // origin obj does not have lob header, but meta has, build temp lob header
      if (OB_FAIL(ObTextStringResult::ob_convert_datum_temporay_lob(datum,
                                                                    in_obj_meta,
                                                                    out_obj_meta,
                                                                    allocator))) {
        LOG_WARN("Lob: failed to convert plain lob data to temp lob", K(ret));
      }
    }
  }
  return ret;
}

}
}
