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
class ObTextStringDatumResult : public common::ObTextStringResult
{
public:
  ObTextStringDatumResult(const ObObjType type, const ObExpr* expr, ObEvalCtx *ctx, ObDatum *res_datum) :
    common::ObTextStringResult(type, expr->obj_meta_.has_lob_header(), NULL), expr_(expr), ctx_(ctx), res_datum_(res_datum)
  {}
  ObTextStringDatumResult(const ObObjType type, bool has_header, ObDatum *res_datum) :
    common::ObTextStringResult(type, has_header, NULL), expr_(NULL), ctx_(NULL), res_datum_(res_datum)
  {}

  ~ObTextStringDatumResult(){};

  TO_STRING_KV(KP_(expr), KP_(ctx), KPC_(res_datum));

  int init(int64_t res_len, ObIAllocator *allocator = NULL);
  int init_with_batch_idx(int64_t res_len, int64_t batch_idx);
  void set_result();

private:
  char * buff_alloc (const int64_t size);

private:
  // for exprs
  const ObExpr *expr_;
  ObEvalCtx *ctx_;
  ObDatum *res_datum_;
};

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

  static int read_real_string_data(ObIAllocator &allocator, const ObDatum &datum, const ObDatumMeta &meta,
                                   bool has_lob_header, ObString &str)
  {
    int ret = OB_SUCCESS;
    str = datum.get_string();
    if (datum.is_null()) {
      str.reset();
    } else if (is_lob_storage(meta.type_)) {
      ObTextStringIter str_iter(meta.type_, meta.cs_type_, str, has_lob_header);
      if (OB_FAIL(str_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: str iter init failed ", K(ret), K(str_iter));
      } else if (OB_FAIL(str_iter.get_full_data(str))) {
        COMMON_LOG(WARN, "Lob: str iter get full data failed ", K(ret), K(str_iter));
      }
    }
    return ret;
  };

  static int read_real_string_data(ObIAllocator *allocator, const common::ObObj &obj, ObString &str)
  {
    int ret = OB_SUCCESS;
    const ObObjMeta& meta = obj.get_meta();
    str = obj.get_string();
    if (meta.is_null()) {
      str.reset();
    } else if (is_lob_storage(meta.get_type())) {
      ObTextStringIter str_iter(meta.get_type(), meta.get_collation_type(), str, obj.has_lob_header());
      if (OB_FAIL(str_iter.init(0, NULL, allocator))) {
        COMMON_LOG(WARN, "Lob: init lob str iter failed ", K(ret), K(str_iter));
      } else if (OB_FAIL(str_iter.get_full_data(str))) {
        COMMON_LOG(WARN, "Lob: str iter get full data failed ", K(ret), K(str_iter));
      }
    }
    return ret;
  }

  static int read_real_string_data(ObIAllocator *allocator, ObObjType type, ObCollationType cs_type,
                                   bool has_lob_header, ObString &str)
  {
    int ret = OB_SUCCESS;
    if (is_lob_storage(type)) {
      ObTextStringIter str_iter(type, cs_type, str, has_lob_header);
      if (OB_FAIL(str_iter.init(0, NULL, allocator))) {
        COMMON_LOG(WARN, "Lob: init lob str iter failed ", K(ret), K(str_iter));
      } else if (OB_FAIL(str_iter.get_full_data(str))) {
        COMMON_LOG(WARN, "Lob: str iter get full data failed ", K(ret), K(str_iter));
      }
    }
    return ret;
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