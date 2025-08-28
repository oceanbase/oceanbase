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
 * This file contains implementation for ob_array_expr_utils.
 */

#ifndef OCEANBASE_SQL_OB_ARRAY_EXPR_UTILS_H_
#define OCEANBASE_SQL_OB_ARRAY_EXPR_UTILS_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/udt/ob_array_utils.h"
#include "sql/engine/expr/ob_expr.h" // for ObExpr
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_array_map.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;

struct ObVectorCastInfo
{
  ObVectorCastInfo()
    : is_vector_(false),
      is_sparse_vector_(false),
      need_cast_(false),
      subschema_id_(UINT16_MAX),
      dim_cnt_(0)
  {}
  bool is_vector_;
  bool is_sparse_vector_;
  bool need_cast_;
  uint16_t subschema_id_;
  uint16_t dim_cnt_;
};

struct ObCollectionExprCell
{
  static const uint32_t COLLECTION_VEC_FORMAT = 0;
  ObCollectionExprCell() : format_(0), row_idx_(-1), expr_(nullptr), eval_ctx_(nullptr)
  {}
  ObCollectionExprCell(int32_t row_idx, ObExpr *expr, ObEvalCtx *eval_ctx) :
    format_(0), row_idx_(row_idx), expr_(expr), eval_ctx_(eval_ctx)
  {}
  uint32_t format_;  // 0: arr_vec_foramt(data in attrs_expr), > 0: arr_compact_foramt(lob) 
  int32_t row_idx_;
  ObExpr *expr_;
  ObEvalCtx *eval_ctx_;
} __attribute__(( packed ));

class ObArrayExprUtils
{
public:
  ObArrayExprUtils();
  virtual ~ObArrayExprUtils() = default;
  static int set_array_res(ObIArrayType *arr_obj, const int32_t data_len, const ObExpr &expr, ObEvalCtx &ctx, common::ObString &res,
                           const char *data = nullptr);
  static int set_array_res(ObIArrayType *arr_obj, const int32_t data_len, ObIAllocator &allocator, common::ObString &res,
                           const char *data = nullptr);
  static int set_array_obj_res(ObIArrayType *arr_obj, ObObjCastParams *params, ObObj *obj);
  template <typename ResVec>
  static int set_array_res(ObIArrayType *arr_obj, const ObExpr &expr, ObEvalCtx &ctx,
                           ResVec *res_vec, int64_t batch_idx)
  {
    int ret = OB_SUCCESS;
    int32_t res_size = arr_obj->get_raw_binary_len();
    char *res_buf = nullptr;
    int64_t res_buf_len = 0;
    ObTextStringVectorResult<ResVec> str_result(expr.datum_meta_.type_, &expr, &ctx, res_vec, batch_idx);
    if (OB_FAIL(str_result.init_with_batch_idx(res_size, batch_idx))) {
      SQL_ENG_LOG(WARN, "fail to init result", K(ret), K(res_size));
    } else if (OB_FAIL(str_result.get_reserved_buffer(res_buf, res_buf_len))) {
      SQL_ENG_LOG(WARN, "fail to get reserver buffer", K(ret));
    } else if (res_buf_len < res_size) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
    } else if (OB_FAIL(arr_obj->get_raw_binary(res_buf, res_buf_len))) {
      SQL_ENG_LOG(WARN, "get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
    } else if (OB_FAIL(str_result.lseek(res_size, 0))) {
      SQL_ENG_LOG(WARN, "failed to lseek res.", K(ret), K(str_result), K(res_size));
    } else {
      str_result.set_result();
    }
    return ret;
  }
  static int deduce_array_element_type(ObExecContext *exec_ctx, ObExprResType* types_stack, int64_t param_num, ObDataType &elem_type);
  static int deduce_nested_array_subschema_id(ObExecContext *exec_ctx,  ObDataType &elem_type, uint16_t &subschema_id);
  static int deduce_map_subschema_id(ObExecContext *exec_ctx, uint16_t key_subid, uint16_t value_subid, uint16_t &subschema_id);
  static int deduce_array_type(ObExecContext *exec_ctx, ObExprResType &type1, ObExprResType &type2,uint16_t &subschema_id);
  static int check_array_type_compatibility(ObExecContext *exec_ctx, uint16_t l_subid, uint16_t r_subid, bool &is_compatiable);
  static int get_coll_info_by_subschema_id(ObExecContext*exec_ctx, uint16_t subid, const ObSqlCollectionInfo *&coll_info);
  static int get_array_element_type(ObExecContext *exec_ctx, uint16_t subid, ObObjType &obj_type, uint32_t &depth, bool &is_vec);
  static int get_array_element_type(ObExecContext *exec_ctx, uint16_t subid, ObDataType &elem_type, uint32_t &depth, bool &is_vec);
  static int dispatch_array_attrs_inner(ObEvalCtx &ctx, ObIArrayType *arr_obj, ObExpr **attrs, uint32_t attr_count, const int64_t row_idx, bool is_shallow = true);
  static int batch_dispatch_array_attrs(ObEvalCtx &ctx, ObExpr &expr, int64_t begin, int64_t batch_size, const uint16_t *selector = NULL);
  static int transform_coll_to_uniform(ObEvalCtx &ctx, const ObExpr &expr, const int64_t batch_size, const ObBitVector *skip);
  static int get_array_type_by_subschema_id(ObEvalCtx &ctx, const uint16_t subschema_id, ObCollectionArrayType *&arr_type);
  static int get_coll_type_by_subschema_id(ObExecContext *exec_ctx, const uint16_t subschema_id, ObCollectionTypeBase *&coll_type);
  static int construct_array_obj(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t subschema_id, ObIArrayType *&res, bool read_only = true);
  static int calc_nested_expr_data_size(const ObExpr &expr, ObEvalCtx &ctx, const int64_t batch_idx, int64_t &size);
  static int get_array_obj(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t subschema_id, const ObString &raw_data, ObIArrayType *&res);
  static int dispatch_array_attrs_rows(ObEvalCtx &ctx, ObIArrayType *arr_obj, const int64_t row_idx,
                                       ObExpr **attrs, uint32_t attr_count, bool is_shallow = true);
  static int nested_expr_to_rows(const ObExpr &expr, ObEvalCtx &ctx, const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                                 const uint16_t selector[], const int64_t size, const int64_t col_idx);
  static int nested_expr_to_row(const ObExpr &expr, ObEvalCtx &ctx, char *row_buf,
                                const int64_t col_offset, const uint64_t row_idx, int64_t &cell_len, const int64_t *remain_size = nullptr);
  static int assemble_array_attrs(ObEvalCtx &ctx, const ObExpr &expr, int64_t row_idx, ObIArrayType *arr_obj);
  static void set_expr_attrs_null(const ObExpr &expr, ObEvalCtx &ctx, const int64_t idx);
  static int add_elem_to_array(const ObExpr &expr, ObEvalCtx &ctx, ObIAllocator &alloc,
                               ObCollectionArrayType *value_type,  ObIArrayType *value_arr, int args_idx);
  static int add_elem_to_nested_array(ObIAllocator &tmp_allocator, ObEvalCtx &ctx, uint16_t subschema_id,
                                      const ObDatum &datum, ObArrayNested *nest_array);
  static int get_child_subschema_id(ObExecContext *exec_ctx, uint16_t subid, uint16_t &child_subid);
  static int get_collection_payload(ObIAllocator &allocator, ObEvalCtx &ctx, const ObExpr &expr,
                                    const int64_t row_idx, const char *&res_data, int32_t &data_len,
                                    bool with_lob_head = true);
  static int calc_collection_hash_val(const ObObjMeta &meta, const void *data, ObLength len, hash_algo hash_func, uint64_t seed, uint64_t &hash_val);
  static int collection_compare(const ObObjMeta &l_meta, const ObObjMeta &r_meta,
                                const void *l_v, const ObLength l_len,
                                const void *r_v, const ObLength r_len,
                                int &cmp_ret);
  // collection object is read only
  static int get_collection_obj(ObEvalCtx &ctx, const uint16_t subschema_id, ObIArrayType *&res);
  static int calc_collection_rows_size(ObIVector &vec, const uint16_t selector[],
                                       const int64_t size, uint32_t row_size_arr[],
                                       const ObBatchRows *brs = nullptr);
  template <typename T1, typename T>
  static int calc_array_sum_by_type(uint32_t data_len, uint32_t len, const char *data_ptr,
                                    uint8_t *null_bitmaps, T &sum);
  template <typename T>
  static int calc_array_sum(uint32_t len, uint8_t *nullbitmaps, const char *data_ptr,
                            uint32_t data_len, ObCollectionArrayType *arr_type, T &sum);
  static int get_array_data(ObString &data_str, ObCollectionArrayType *arr_type, uint32_t &len,
                            uint8_t *&null_bitmaps, const char *&data, uint32_t &data_len);
  static int get_array_data(ObIVector *len_vec, ObIVector *nullbitmap_vec, ObIVector *data_vec,
                            int64_t idx, ObCollectionArrayType *arr_type, uint32_t &len,
                            uint8_t *&null_bitmaps, const char *&data, uint32_t &data_len);

  // for vector
  static int get_type_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObIAllocator &allocator,
                             ObIArrayType *&result,
                             bool &is_null);
  static int get_type_vector(const ObExpr &expr,
                             const ObDatum &datum,
                             ObEvalCtx &ctx,
                             ObIAllocator &allocator,
                             ObIArrayType *&result);
  static int calc_cast_type(const ObExprOperatorType &expr_type, ObExprResType &type, common::ObExprTypeCtx &type_ctx, const bool only_vector = false);
  static int calc_cast_type2(const ObExprOperatorType &expr_type, ObExprResType &type1, ObExprResType &type2, common::ObExprTypeCtx &type_ctx, uint16_t &res_subschema_id,
                             const bool only_vector = false);
  static int collect_vector_cast_info(ObExprResType &type, ObExecContext &exec_ctx, ObVectorCastInfo &info);
  static bool is_sparse_vector_supported(const ObExprOperatorType &type) { 
    return type == T_FUN_SYS_INNER_PRODUCT || 
           type == T_FUN_SYS_NEGATIVE_INNER_PRODUCT ||
           type == T_FUN_SYS_VECTOR_DIMS; 
  };

  // update inplace
  static int vector_datum_add(ObDatum &res, const ObDatum &data, ObIAllocator &allocator, ObDatum *tmp_res = nullptr, bool negative = false);
  static int get_basic_elem(ObIArrayType *src, uint32_t idx, ObObj &elem_obj, bool &is_null);
  static int set_obj_to_vector(ObIVector *vec, int64_t idx, ObObj obj, ObIAllocator &allocator);

  // check
  template<typename T>
  static int raw_check_add(const T &res, const T &l, const T &r);

  template<typename T>
  static int raw_check_minus(const T &res, const T &l, const T &r);
  static int construct_map(ObIAllocator &allocator, ObIArrayType *src_key_arr, ObIArrayType *src_value_arr, ObMapType *dst_map);
  template <typename T>
  static int calc_fixed_size_key_index(ObIArrayType *src_key_arr, uint32_t *idx_arr, uint32_t &idx_count);
  static int calc_string_key_index(ObIArrayType *src_key_arr, uint32_t *idx_arr, uint32_t &idx_count);

private:
  static const char* DEFAULT_CAST_TYPE_NAME;
  static const ObString DEFAULT_CAST_TYPE_STR;
  static int get_collection_raw_data(ObIAllocator &allocator, const ObObjMeta &meta, const void *data, ObLength len, ObString &bin_str);
};

struct ObVectorArithFunc 
{
  enum ArithType
  {
    ADD = 0,
    MINUS,
    MUL,
    DIV,
  };
};

struct ObVectorVectorArithFunc : public ObVectorArithFunc
{
  
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr, ObEvalCtx &ctx, ArithType type) const;
};

struct ObVectorElemArithFunc : public ObVectorArithFunc
{
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, const ObExpr &expr, ObEvalCtx &ctx, ArithType type) const;
};

class ObNestedVectorFunc
{
public:
  static int construct_attr_param(ObIAllocator &alloc, ObEvalCtx &ctx, ObExpr &param_expr,
                                const uint16_t meta_id, int64_t row_idx, ObIArrayType *&param_obj);
  static int construct_param(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t meta_id,
                              ObString &str_data, ObIArrayType *&param_obj);
  static int construct_res_obj(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t meta_id, ObIArrayType *&res_obj);

  static int construct_params(ObIAllocator &alloc, ObEvalCtx &ctx, const uint16_t left_meta_id,
                                const uint16_t right_meta_id, const uint16_t res_meta_id, ObString &left, ObString right,
                                ObIArrayType *&left_obj, ObIArrayType *&right_obj, ObIArrayType *&res_obj);
};

class ObCollectionExprUtil
{
private:
  // using ATTR0_FMT = ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>;
public:
  OB_INLINE static bool is_compact_fmt_cell(const void *ptr)
  {
    OB_ASSERT(ptr != nullptr);
    // uniform cell is a lob data, first uint32_t is version of lob, must >= 1
    // for discrete/continous cell, we set first uint32_t to 0
    return (reinterpret_cast<const uint32_t *>(ptr))[0] >= 1;
  }

  OB_INLINE static bool is_vector_fmt_cell(const void *ptr)
  {
    OB_ASSERT(ptr != nullptr);
    return (reinterpret_cast<const uint32_t *>(ptr))[0] == 0;
  }

  template <typename ColumnFmt>
  OB_INLINE static void get_attrN_value(const ObCollectionExprCell *cell, int32_t attr_idx,
                                        const char *&out_ptr, int32_t &out_len)
  {
    OB_ASSERT(cell != nullptr && cell->expr_ != nullptr && cell->eval_ctx_ != nullptr);
    OB_ASSERT(cell->expr_->attrs_cnt_ > attr_idx);
    ObIVector *attrN_ivec = cell->expr_->attrs_[attr_idx]->get_vector(*cell->eval_ctx_);
    return static_cast<ColumnFmt *>(attrN_ivec)->get_payload(cell->row_idx_, out_ptr, out_len);
  }

  template <typename ColumnFmt>
  OB_INLINE static void set_attrN_value(ObCollectionExprCell *cell, int32_t attr_idx,
                                        const char *src, int32_t src_len)
  {
    OB_ASSERT(cell != nullptr && cell->expr_ != nullptr && cell->eval_ctx_ != nullptr);
    OB_ASSERT(cell->expr_->attrs_cnt_ > attr_idx);
    ObIVector *attrN_ivec = cell->expr_->attrs_[attr_idx]->get_vector(*cell->eval_ctx_);
    static_cast<ColumnFmt *>(attrN_ivec)->set_payload(cell->row_idx_, src, src_len);
  }

  template <typename ColumnFmt>
  OB_INLINE static void set_attrN_value_shallow(ObCollectionExprCell *cell, int32_t attr_idx,
                                                const char *src, int32_t src_len)
  {
    OB_ASSERT(cell != nullptr && cell->expr_ != nullptr && cell->eval_ctx_ != nullptr);
    OB_ASSERT(cell->expr_->attrs_cnt_ > attr_idx);
    ObIVector *attrN_ivec = cell->expr_->attrs_[attr_idx]->get_vector(*cell->eval_ctx_);
    static_cast<ColumnFmt *>(attrN_ivec)->set_payload_shallow(cell->row_idx_, src, src_len);
  }

  template <typename ColumnFmt>
  OB_INLINE static int write_collection_to_row(const ColumnFmt *column, const sql::RowMeta &row_meta,
                                               sql::ObCompactRow *stored_row,
                                               const uint64_t row_idx, const int64_t col_idx)
  {
    int ret = OB_SUCCESS;
    const char *payload = nullptr;
    int32_t len = 0;
    bool is_null = false;
    column->get_payload(row_idx, is_null, payload, len);
    if (OB_UNLIKELY(is_null)) {
      stored_row->set_null(row_meta, col_idx);
    } else if (is_vector_fmt_cell(payload)) {
      // assemble attr data and write to row
      const ObCollectionExprCell* coll_cell = reinterpret_cast<const ObCollectionExprCell*>(payload);
      OB_ASSERT(coll_cell != nullptr && coll_cell->expr_ != nullptr && coll_cell->eval_ctx_ != nullptr);
      int64_t offset = stored_row->offset(row_meta, col_idx);
      char *row_buf = stored_row->payload();
      int64_t cell_len = 0;
      if (OB_FAIL(ObArrayExprUtils::nested_expr_to_row(*coll_cell->expr_, *coll_cell->eval_ctx_, row_buf,
                                                       offset, row_idx, cell_len))) {
        SQL_LOG(WARN, "nested expr to row failed", K(ret));
      } else {
        stored_row->update_var_offset(row_meta, col_idx, cell_len);
      }
    } else {
      stored_row->set_cell_payload(row_meta, col_idx, payload, len);
    }
    // tonghui TODO: get attr data and build lob, set to stored row
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE static int write_collection_to_row(const ColumnFmt *column, const sql::RowMeta &row_meta,
                                               sql::ObCompactRow *stored_row, const uint64_t row_idx,
                                               const int64_t col_idx, const int64_t remain_size,
                                               const bool is_fixed_length_data, int64_t &row_size)
  {
    int ret = OB_SUCCESS;
    const char *payload = nullptr;
    int32_t len = 0;
    bool is_null = false;
    column->get_payload(row_idx, is_null, payload, len);
    if (OB_UNLIKELY(is_null)) {
      stored_row->set_null(row_meta, col_idx);
    } else if (is_vector_fmt_cell(payload)) {
      // assemble attr datas and write to row
      const ObCollectionExprCell* coll_cell = reinterpret_cast<const ObCollectionExprCell*>(payload);
      OB_ASSERT(coll_cell != nullptr && coll_cell->expr_ != nullptr && coll_cell->eval_ctx_ != nullptr);
      int64_t offset = stored_row->offset(row_meta, col_idx);
      char *row_buf = stored_row->payload();
      int64_t cell_len = 0;
      if (OB_FAIL(ObArrayExprUtils::nested_expr_to_row(*coll_cell->expr_, *coll_cell->eval_ctx_, row_buf,
                                                       offset, row_idx, cell_len, &remain_size))) {
        SQL_LOG(WARN, "nested expr to row failed", K(ret));
      } else {
        stored_row->update_var_offset(row_meta, col_idx, cell_len);
      }
    } else {
      if (len > remain_size) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        row_size += len;
        stored_row->set_cell_payload(row_meta, col_idx, payload, len);
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE static int write_collections_to_rows(const ColumnFmt *column, const sql::RowMeta &row_meta,
                                                sql::ObCompactRow **stored_rows, const uint16_t selector[],
                                                const int64_t size, const int64_t col_idx)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < size; i++) {
      int64_t row_idx = selector[i];
      if (OB_FAIL(column->to_row(row_meta, stored_rows[i], row_idx, col_idx))) {
        SQL_LOG(WARN, "to row failed", K(ret));
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE static int write_collections_to_rows(const ColumnFmt *column,
                                                 const sql::RowMeta &row_meta,
                                                 sql::ObCompactRow **stored_rows,
                                                 const int64_t size, const int64_t col_idx)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < size; i++) {
      int64_t row_idx = i;
      if (OB_FAIL(column->to_row(row_meta, stored_rows[i], row_idx, col_idx))) {
        SQL_LOG(WARN, "to row failed", K(ret));
      }
    }
    return ret;
  }

  static int cast_compact2vector_fmt(ObIVector *column, const int64_t size, const ObBitVector &skip);

  static int cast_vector2compact_fmt(ObIVector *column, const int64_t size, const ObBitVector &skip);
};


} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_ARRAY_EXPR_UTILS_H_
