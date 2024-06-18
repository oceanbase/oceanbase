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
 * This file is for define of expr lob utils
 */

#ifndef OCEANBASE_SQL_OB_EXPR_LOB_UTILS_H_
#define OCEANBASE_SQL_OB_EXPR_LOB_UTILS_H_

#include "share/ob_lob_access_utils.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{

// wrapper class to handle sql expr string/text type result
template <typename VectorType>
class ObTextStringVectorResult : public common::ObTextStringResult
{
public:
  ObTextStringVectorResult(const ObObjType type, const ObExpr* expr, ObEvalCtx *ctx, ObDatum *res_datum) :
    common::ObTextStringResult(type, expr->obj_meta_.has_lob_header(), NULL), expr_(expr), ctx_(ctx), res_datum_(res_datum), res_vec_(NULL), batch_idx_(0)
  {}
  ObTextStringVectorResult(const ObObjType type, bool has_header, ObDatum *res_datum) :
    common::ObTextStringResult(type, has_header, NULL), expr_(NULL), ctx_(NULL), res_datum_(res_datum), res_vec_(NULL), batch_idx_(0)
  {}
  ObTextStringVectorResult(const ObObjType type, const ObExpr* expr, ObEvalCtx *ctx, VectorType *res_vec, int64_t batch_idx) :
    common::ObTextStringResult(type, expr->obj_meta_.has_lob_header(), NULL), expr_(expr), ctx_(ctx), res_datum_(NULL), res_vec_(res_vec), batch_idx_(batch_idx)
  {}

  ~ObTextStringVectorResult(){};

  TO_STRING_KV(KP_(expr), KP_(ctx), KPC_(res_datum));

  int init(int64_t res_len, ObIAllocator *allocator = NULL);
  int init_with_batch_idx(int64_t res_len, int64_t batch_idx);
  void set_result();
  void set_result_null();
private:
  char * buff_alloc (const int64_t size);

private:
  // for exprs
  const ObExpr *expr_;
  ObEvalCtx *ctx_;
  ObDatum *res_datum_;
  VectorType *res_vec_;
  int64_t batch_idx_;
};

template <typename VectorType>
int ObTextStringVectorResult<VectorType>::init(int64_t res_len, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (is_init_) {
    SQL_LOG(WARN, "Lob: textstring result init already", K(ret), K(*this));
  } else if (OB_ISNULL(allocator) && (OB_ISNULL(expr_) || OB_ISNULL(ctx_))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Lob: invalid arguments", K(ret), KP(expr_), KP(ctx_), KP(allocator));
  } else if(OB_ISNULL(res_datum_) && OB_ISNULL(res_vec_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Lob: invalid arguments", K(ret), K(type_), KPC(res_datum_));
  } else if (OB_FAIL(ObTextStringResult::calc_buffer_len(res_len))) {
    SQL_LOG(WARN, "Lob: calc buffer len failed", K(ret), K(type_), K(res_len));
  } else if (buff_len_ == 0) {
    OB_ASSERT(has_lob_header_ == false); // empty result without header
  } else {
    buffer_ = OB_ISNULL(allocator)
              ? expr_->get_str_res_mem(*ctx_, buff_len_) : (char *)allocator->alloc(buff_len_);
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "Lob: alloc buffer failed", K(ret), KP(expr_), KP(allocator), K(buff_len_));
    } else if (OB_FAIL(fill_temp_lob_header(res_len))) {
      SQL_LOG(WARN, "Lob: fill_temp_lob_header failed", K(ret), K(type_));
    }
  }
  if (OB_SUCC(ret)) {
    is_init_ = true;
  }
  return ret;
}

template <typename VectorType>
int ObTextStringVectorResult<VectorType>::init_with_batch_idx(int64_t res_len, int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  if(OB_ISNULL(expr_)
     || OB_ISNULL(ctx_)
     || (OB_ISNULL(res_datum_) && OB_ISNULL(res_vec_))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Lob: invalid arguments", K(ret), K(type_), KP(expr_), KP(ctx_), KP(res_datum_));
  } else if (OB_FAIL(ObTextStringResult::calc_buffer_len(res_len))) {
    SQL_LOG(WARN, "Lob: calc buffer len failed", K(ret), K(type_), KP(expr_), KP(ctx_), KP(res_datum_));
  } else {
    buffer_ = expr_->get_str_res_mem(*ctx_, buff_len_, batch_idx);
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "Lob: alloc buffer failed", K(ret), KP(expr_), K(buff_len_));
    } else if (OB_FAIL(fill_temp_lob_header(res_len))) {
      SQL_LOG(WARN, "Lob: fill_temp_lob_header failed", K(ret), K(type_));
    }
  }
  return ret;
}

template <typename VectorType>
void ObTextStringVectorResult<VectorType>::set_result()
{
  NULL == res_datum_
      ? res_vec_->set_string(batch_idx_, buffer_, pos_)
          : res_datum_->set_string(buffer_, pos_);
}

template <typename VectorType>
void ObTextStringVectorResult<VectorType>::set_result_null()
{
  NULL == res_datum_
      ? res_vec_->set_null(batch_idx_)
          : res_datum_->set_null();
}

using ObTextStringDatumResult = ObTextStringVectorResult<common::ObIVector>;

class ObTextStringObObjResult : public common::ObTextStringResult
{
public:
  ObTextStringObObjResult(const ObObjType type, ObObjCastParams *params, ObObj *res_obj, bool has_header) :
    common::ObTextStringResult(type, has_header, NULL), params_(params), res_obj_(res_obj)
  {}

  ~ObTextStringObObjResult(){};

  TO_STRING_KV(KP_(params), KP_(res_obj));
  int init(int64_t res_len, ObIAllocator *allocator = NULL);
  void set_result();

private:
  char * buff_alloc (const int64_t size);

private:
  ObObjCastParams *params_;
  ObObj *res_obj_;
};

class ObTextStringHelper
{
public:
  static const uint32_t DEAFULT_LOB_PREFIX_BYTE_LEN = 4000;

  static int get_string(const ObExpr &expr, ObIAllocator &allocator, int64_t idx,
                        ObDatum *datum, ObString &str)
  {
    int ret = OB_SUCCESS;
    if (datum == NULL) {
      str.assign_ptr("", 0);
    } else if (datum->is_null()) {
      str.assign_ptr("", 0);
    } else {
      str = datum->get_string();
      const ObDatumMeta &meta = expr.args_[idx]->datum_meta_;
      bool has_lob_header = expr.args_[idx]->obj_meta_.has_lob_header();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, *datum, meta, has_lob_header, str))) {
        COMMON_LOG(WARN, "Lob: fail to get string.", K(ret), K(str));
      }
    }
    return ret;
  };

  template <typename VectorType>
  static int get_string(const ObExpr &expr, ObIAllocator &allocator, int64_t arg_idx,
                        const int64_t idx, VectorType *vector, ObString &str)
  {
    int ret = OB_SUCCESS;
    if (vector == NULL || vector->is_null(idx)) {
      str.assign_ptr("", 0);
    } else {
      str = vector->get_string(idx);
      const ObDatumMeta &meta = expr.args_[arg_idx]->datum_meta_;
      bool has_lob_header = expr.args_[arg_idx]->obj_meta_.has_lob_header();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, vector, meta, has_lob_header, str, idx))) {
        COMMON_LOG(WARN, "Lob: fail to get string.", K(ret), K(str));
      }
    }
    return ret;
  };

  static int read_real_string_data(ObIAllocator &allocator, const ObDatum &datum, const ObDatumMeta &meta,
                                   bool has_lob_header, ObString &str, sql::ObExecContext *exec_ctx = nullptr)
  {
    int ret = OB_SUCCESS;
    str = datum.get_string();
    if (datum.is_null()) {
      str.reset();
    } else if (is_lob_storage(meta.type_)) {
      const ObLobCommon& lob = datum.get_lob_data();
      if (datum.len_ != 0 && !lob.is_mem_loc_ && lob.in_row_) {
        str.assign_ptr(lob.get_inrow_data_ptr(), static_cast<int32_t>(lob.get_byte_size(datum.len_)));
      } else {
        const ObMemLobCommon *memlob = reinterpret_cast<const ObMemLobCommon*>(datum.ptr_);
        if (datum.len_ != 0 && memlob->has_inrow_data_ && memlob->has_extern_ == 0 && (memlob->type_ != ObMemLobType::TEMP_DELTA_LOB)) {
          if (memlob->is_simple_) {
            str.assign_ptr(memlob->data_, static_cast<int32_t>(datum.len_ - sizeof(ObMemLobCommon)));
          } else {
            const ObLobCommon *disklob = reinterpret_cast<const ObLobCommon*>(memlob->data_);
            if (disklob->in_row_) {
              str.assign_ptr(disklob->get_inrow_data_ptr(),
                 static_cast<int32_t>(disklob->get_byte_size(datum.len_ - sizeof(ObMemLobCommon))));
            } else {
              int64_t disk_lob_handle_size = disklob->get_handle_size(0);
              str.assign_ptr(memlob->data_ + disk_lob_handle_size,
                 static_cast<int32_t>(datum.len_ - sizeof(ObMemLobCommon) - disk_lob_handle_size));
            }
          }
        } else {
          // read outrow lob data
          if (OB_FAIL(read_real_string_data(
              &allocator,
              meta.type_,
              meta.cs_type_,
              has_lob_header,
              str,
              exec_ctx))) {
            COMMON_LOG(WARN, "read_real_string_data fail", K(ret), K(meta), K(has_lob_header), K(datum));
          }
        }
      }
    }
    return ret;
  };

  template <typename VectorType>
  static int read_real_string_data(ObIAllocator &allocator, const VectorType &vector,
                                   const ObDatumMeta &meta, bool has_lob_header, ObString &str,
                                   const int64_t idx, sql::ObExecContext *exec_ctx = nullptr)
  {
    int ret = OB_SUCCESS;
    str = vector->get_string(idx);
    if (vector->is_null(idx)) {
      str.reset();
    } else if (is_lob_storage(meta.type_)) {
      const ObLobCommon& lob = vector->get_lob_data(idx);
      if (vector->get_length(idx) != 0 && !lob.is_mem_loc_ && lob.in_row_) {
        str.assign_ptr(lob.get_inrow_data_ptr(),
                       static_cast<int32_t>(lob.get_byte_size(vector->get_length(idx))));
      } else {
        const ObMemLobCommon *memlob =
          reinterpret_cast<const ObMemLobCommon *>(vector->get_payload(idx));
        if (vector->get_length(idx) != 0 && memlob->has_inrow_data_ && memlob->has_extern_ == 0) {
          if (memlob->is_simple_) {
            str.assign_ptr(memlob->data_,
                           static_cast<int32_t>(vector->get_length(idx) - sizeof(ObMemLobCommon)));
          } else {
            const ObLobCommon *disklob = reinterpret_cast<const ObLobCommon*>(memlob->data_);
            if (disklob->in_row_) {
              str.assign_ptr(disklob->get_inrow_data_ptr(),
                             static_cast<int32_t>(disklob->get_byte_size(
                               vector->get_length(idx) - sizeof(ObMemLobCommon))));
            } else {
              int64_t disk_lob_handle_size = disklob->get_handle_size(0);
              str.assign_ptr(memlob->data_ + disk_lob_handle_size,
                             static_cast<int32_t>(vector->get_length(idx) - sizeof(ObMemLobCommon)
                                                  - disk_lob_handle_size));
            }
          }
        } else {
          // read outrow lob data
          if (OB_FAIL(read_real_string_data(
              &allocator,
              meta.type_,
              meta.cs_type_,
              has_lob_header,
              str,
              exec_ctx))) {
            COMMON_LOG(WARN, "read_real_string_data fail", K(ret), K(meta), K(has_lob_header), K(vector));
          }
        }
      }
    }
    return ret;
  };

  static int read_real_string_data(
      ObIAllocator *allocator,
      const common::ObObj &obj,
      ObString &str,
      sql::ObExecContext *exec_ctx = nullptr);

  static int read_real_string_data(
      ObIAllocator *allocator,
      ObObjType type,
      ObCollationType cs_type,
      bool has_lob_header,
      ObString &str,
      sql::ObExecContext *exec_ctx = nullptr);

  static int read_real_string_data(
      ObIAllocator *allocator,
      ObObjType type,
      bool has_lob_header,
      ObString &str,
      sql::ObExecContext *exec_ctx)
  {
    return read_real_string_data(allocator, type, CS_TYPE_BINARY, has_lob_header, str, exec_ctx);
  }


  // get outrow lob prefix or inrow/string tc full data
  static int read_prefix_string_data(ObEvalCtx &ctx,
                                     const ObDatum &datum,
                                     const ObDatumMeta &meta,
                                     const bool has_lob_header,
                                     ObIAllocator *allocator,
                                     ObString &str,
                                     uint32_t prefix_char_len = DEAFULT_LOB_PREFIX_BYTE_LEN)
  {
    int ret = OB_SUCCESS;
    str = datum.get_string();
    if (datum.is_null()) {
      str.reset();
    } else if (ob_is_text_tc(meta.type_)) {
      ObTextStringIter str_iter(meta.type_, meta.cs_type_, str, has_lob_header);
      if (OB_FAIL(str_iter.init(0, NULL, allocator))) {
        COMMON_LOG(WARN, "Lob: init lob str iter failed ", K(ret), K(str_iter));
      } else if (OB_FAIL(str_iter.get_inrow_or_outrow_prefix_data(str, prefix_char_len))) {
        COMMON_LOG(WARN, "Lob: init lob str iter failed ", K(ret), K(str_iter));
      }
    }
    return ret;
  };

  // get outrow lob prefix or inrow/string tc full data
  static int read_prefix_string_data(ObIAllocator *allocator,
                                     const common::ObObj &obj,
                                     ObString &str,
                                     uint32_t prefix_char_len = DEAFULT_LOB_PREFIX_BYTE_LEN)
  {
    int ret = OB_SUCCESS;
    const ObObjMeta& meta = obj.get_meta();
    str = obj.get_string();
    if (meta.is_null()) {
      str.reset();
    } else if (ob_is_text_tc(meta.get_type())) {
      ObTextStringIter str_iter(meta.get_type(), meta.get_collation_type(), str, obj.has_lob_header());
      if (OB_FAIL(str_iter.init(0, NULL, allocator))) {
        COMMON_LOG(WARN, "Lob: init lob str iter failed ", K(ret), K(str_iter));
      } else if (OB_FAIL(str_iter.get_inrow_or_outrow_prefix_data(str, prefix_char_len))) {
        COMMON_LOG(WARN, "Lob: init lob str iter failed ", K(ret), K(str_iter));
      }
    }
    return ret;
  }

  static int get_char_len(ObEvalCtx &ctx, const ObDatum & datum, const ObDatumMeta &meta,
                          const bool has_lob_header, int64_t &char_len)
  {
    int ret = OB_SUCCESS;
    if (datum.is_null()) {
      char_len = 0;
    } else {
      ObString str = datum.get_string();
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObTextStringIter str_iter(meta.type_, meta.cs_type_, str, has_lob_header);
      if (OB_FAIL(str_iter.init(0, NULL, &temp_allocator))) {
        COMMON_LOG(WARN, "Lob: init lob str iter failed ", K(ret), K(str_iter));
      } else if (OB_FAIL(str_iter.get_char_len(char_len))) {
        COMMON_LOG(WARN, "Lob: init lob str iter failed ", K(ret), K(str_iter));
      }
    }
    return ret;
  };

  static int string_to_templob_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, const ObString &str)
  {
    int ret = OB_SUCCESS;
    ObTextStringDatumResult tmp_lob_res(expr.datum_meta_.type_, &expr, &ctx, &res);
    if (OB_FAIL(tmp_lob_res.init(str.length()))) {
      COMMON_LOG(WARN, "Lob: init lob result failed");
    } else if (OB_FAIL(tmp_lob_res.append(str.ptr(), str.length()))) {
      COMMON_LOG(WARN, "Lob: append lob result failed");
    } else {
      tmp_lob_res.set_result();
    }
    return ret;
  };

  static int str_to_lob_storage_obj(ObIAllocator &allocator, const ObString& input, common::ObObj& output)
  {
    INIT_SUCC(ret);
    // pl must has lob header
    bool has_lob_header = true;
    ObTextStringObObjResult text_result(ObLongTextType, nullptr, &output, has_lob_header);
    if (OB_FAIL(text_result.init(input.length(), &allocator))) {
      COMMON_LOG(WARN, "init lob result failed", K(ret));
    } else if (OB_FAIL(text_result.append(input.ptr(), input.length()))) {
      COMMON_LOG(WARN, "failed to append realdata", K(ret), K(input), K(text_result));
    } else {
      text_result.set_result();
    }
    return ret;
  }

  static int build_text_iter(
      ObTextStringIter &text_iter,
      sql::ObExecContext *exec_ctx,
      const sql::ObBasicSessionInfo *session = NULL,
      ObIAllocator *res_allocator = NULL,
      ObIAllocator *tmp_allocator = NULL);

};

int ob_adjust_lob_datum(const ObObj &origin_obj,
                        const common::ObObjMeta &obj_meta,
                        const ObObjDatumMapType &obj_datum_map_,
                        ObIAllocator &allocator, // can avoid allocator if no persist lobs call this function,
                        ObDatum &out_datum);

int ob_adjust_lob_datum(const ObObj &origin_obj,
                        const common::ObObjMeta &obj_meta,
                        ObIAllocator &allocator,
                        ObDatum &out_datum);

int ob_adjust_lob_datum(const ObObj &origin_obj,
                        const common::ObObjMeta &obj_meta,
                        ObIAllocator &allocator,
                        ObDatum *out_datum);

int ob_adjust_lob_datum(ObDatum &datum,
                        const common::ObObjMeta &in_obj_meta,
                        const common::ObObjMeta &out_obj_meta,
                        ObIAllocator &allocator);

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_LOB_UTILS_H_
