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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_UTIL_
#define OCEANBASE_STORAGE_OB_STORAGE_UTIL_

#include "lib/allocator/ob_allocator.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObColumnParam;
}
}
namespace common
{
class ObBitmap;
}
namespace sql
{
struct ObBoolMask;
class ObBlackFilterExecutor;
}
namespace blocksstable
{
struct ObStorageDatum;
}
namespace storage
{
class ObTableIterParam;
class ObTableAccessContext;

int pad_column(const ObObjMeta &obj_meta,
               const ObAccuracy accuracy,
               common::ObIAllocator &padding_alloc,
               blocksstable::ObStorageDatum &datum);

int pad_column(const ObAccuracy accuracy,
               common::ObIAllocator &padding_alloc,
               common::ObObj &cell);

int pad_column(const common::ObAccuracy accuracy,
               sql::ObEvalCtx &ctx,
               sql::ObExpr &expr);

int pad_on_datums(const common::ObAccuracy accuracy,
                  const common::ObCollationType cs_type,
                  common::ObIAllocator &padding_alloc,
                  int64_t row_count,
                  common::ObDatum *&datums);

int pad_on_rich_format_columns(const common::ObAccuracy accuracy,
                               const common::ObCollationType cs_type,
                               const int64_t row_cap,
                               const int64_t vec_offset,
                               common::ObIAllocator &padding_alloc,
                               sql::ObExpr &expr,
                               sql::ObEvalCtx &eval_ctx);

int fill_datums_lob_locator(const ObTableIterParam &iter_param,
                            const ObTableAccessContext &context,
                            const share::schema::ObColumnParam &col_param,
                            const int64_t row_cap,
                            ObDatum *datums,
                            bool reuse_lob_locator = true);

int fill_exprs_lob_locator(const ObTableIterParam &iter_param,
                           const ObTableAccessContext &context,
                           const share::schema::ObColumnParam &col_param,
                           sql::ObExpr &expr,
                           sql::ObEvalCtx &eval_ctx,
                           const int64_t vec_offset,
                           const int64_t row_cap);

int check_skip_by_monotonicity(sql::ObBlackFilterExecutor &filter,
                               blocksstable::ObStorageDatum &min_datum,
                               blocksstable::ObStorageDatum &max_datum,
                               const sql::ObBitVector &skip_bit,
                               const bool has_null,
                               ObBitmap *result_bitmap,
                               sql::ObBoolMask &bool_mask);

int cast_obj(const common::ObObjMeta &src_meta, common::ObIAllocator &cast_allocator, common::ObObj &obj);

int init_expr_vector_header(
    sql::ObExpr &expr,
    sql::ObEvalCtx &eval_ctx,
    const int64_t size,
    const VectorFormat format = VectorFormat::VEC_UNIFORM);

OB_INLINE int init_exprs_uniform_header(
    const sql::ObExprPtrIArray *exprs,
    sql::ObEvalCtx &eval_ctx,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  if (nullptr != exprs) {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs->count(); ++i) {
      sql::ObExpr *expr = exprs->at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null expr", K(ret), KPC(exprs));
      } else if (OB_FAIL(init_expr_vector_header(*expr, eval_ctx, size))) {
        STORAGE_LOG(WARN, "Failed to init vector", K(ret), K(i), KPC(expr));
      }
    }
  }
  return ret;
}

int init_exprs_new_format_header(
    const common::ObIArray<int32_t> &cols_projector,
    const sql::ObExprPtrIArray &exprs,
    sql::ObEvalCtx &eval_ctx);

OB_INLINE bool can_do_ascii_optimize(common::ObCollationType cs_type)
{
  return common::CS_TYPE_UTF8MB4_GENERAL_CI == cs_type
      || common::CS_TYPE_UTF8MB4_BIN == cs_type
      || common::CS_TYPE_UTF8MB4_UNICODE_CI == cs_type
      || common::CS_TYPE_GBK_CHINESE_CI == cs_type
      || common::CS_TYPE_GBK_BIN == cs_type;
}

OB_INLINE bool is_ascii_less_8(const char *str, int64_t len)
{
    bool is_not_ascii = true;
    const uint8_t *val = reinterpret_cast<const uint8_t *>(str);
    switch (len) {
    case 0:
      is_not_ascii = false;
        break;
    case 1:
        is_not_ascii = (0x80 & val[0]);
        break;
    case 2:
        is_not_ascii = 0x8080 & *((const uint16_t *)val);
        break;
    case 3:
        is_not_ascii = (0x8080 & *(const uint16_t *)val) | (0x80 & val[2]);
        break;
    case 4:
        is_not_ascii = (0x80808080U & *((const uint32_t *)val));
        break;
    case 5:
        is_not_ascii = (0x80808080U & *((const uint32_t *)val)) | (0x80 & val[4]);
        break;
    case 6:
        is_not_ascii = (0x80808080U & *(const uint32_t *)val) | (0x8080 & *(const uint16_t *)(val + 4));
        break;
    case 7:
        is_not_ascii = (0x80808080U & *(const uint32_t *)val) | (0x80808080U & *(const uint32_t *)(val + 3));
        break;
    }
    return !is_not_ascii;
}

OB_INLINE bool is_ascii_str(const char *str, const int64_t len)
{
  bool bret = true;
  if (len >= 8) {
    const int64_t length = len / 8;
    const uint64_t *vals = reinterpret_cast<const uint64_t *>(str);
    for (int64_t i = 0; bret && i < length; i++) {
      if (vals[i] & 0x8080808080808080UL) {
        bret = false;
      }
    }
    bret = bret && is_ascii_less_8(str + len / 8 * 8, len % 8);
  } else {
    bret = is_ascii_less_8(str, len);
  }
  return bret;
}

class ObObjBufArray final
{
public:
  ObObjBufArray()
      : capacity_(0),
      is_inited_(false),
      data_(NULL),
      allocator_(NULL)
  {
    //MEMSET(local_data_buf_, 0, LOCAL_ARRAY_SIZE * sizeof(common::ObObj));
  }
  ~ObObjBufArray()
  {
    reset();
  }

  int init(common::ObIAllocator *allocator)
  {
    int ret = common::OB_SUCCESS;
    if (IS_INIT) {
      ret = common::OB_INIT_TWICE;
      STORAGE_LOG(WARN, "init twice", K(ret), K(is_inited_));
    } else if (OB_ISNULL(allocator)) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(allocator));
    } else {
      allocator_ = allocator;
      data_ = reinterpret_cast<common::ObObj*>(local_data_buf_);
      capacity_ = LOCAL_ARRAY_SIZE;
      is_inited_ = true;
    }
    return ret;
  }

  inline bool is_inited() const { return is_inited_; }

  inline int reserve(int64_t count)
  {
    int ret = common::OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObObjBufArray not inited", K(ret), K(is_inited_));
    } else if (count > capacity_) {
      int64_t new_size = count * sizeof(common::ObObj);
      common::ObObj *new_data = reinterpret_cast<common::ObObj *>(allocator_->alloc(new_size));
      if (OB_NOT_NULL(new_data)) {
        if ((char *)data_ != local_data_buf_) {
          allocator_->free(data_);
        }
        MEMSET(new_data, 0, new_size);
        data_ = new_data;
        capacity_ = count;
      } else {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "no memory", K(ret), K(new_size), K(capacity_));
      }
    }
    return ret;
  }

  inline int64_t get_count() const { return capacity_; }

  inline common::ObObj *get_data() { return data_; }

  void reset()
  {
    if (NULL != allocator_ && (char *)data_ != local_data_buf_) {
      allocator_->free(data_);
    }
    allocator_ = NULL;
    data_ = NULL;
    capacity_ = 0;
    is_inited_ = false;
  }

  inline common::ObObj &at(int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < capacity_);
    return data_[idx];
  }

protected:
  const static int64_t LOCAL_ARRAY_SIZE = 64;
  int64_t capacity_;
  bool is_inited_;
  common::ObObj *data_;
  char local_data_buf_[LOCAL_ARRAY_SIZE * sizeof(common::ObObj)];
  common::ObIAllocator *allocator_;
};

inline static common::ObDatumCmpFuncType get_datum_cmp_func(const common::ObObjMeta &col_obj_type, const common::ObObjMeta &param_obj_type)
{
  common::ObDatumCmpFuncType cmp_func = nullptr;
  bool is_oracle_mode = lib::is_oracle_mode();
  // if compare lob with non-lob, should use get_nullsafe_cmp_func to get cmp_func
  // especially tinytext, beacause tinytext does not have lob header, but it's type class is TextTC.
  bool not_both_lob_storage = col_obj_type.is_lob_storage() ^ param_obj_type.is_lob_storage();

  if (col_obj_type.get_type_class() != param_obj_type.get_type_class() || not_both_lob_storage) {
    cmp_func = ObDatumFuncs::get_nullsafe_cmp_func(
        col_obj_type.get_type(),
        param_obj_type.get_type(),
        is_oracle_mode ? NULL_LAST : NULL_FIRST,
        col_obj_type.get_collation_type(),
        col_obj_type.get_scale(),
        is_oracle_mode,
        col_obj_type.has_lob_header() || param_obj_type.has_lob_header());
  } else {
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(col_obj_type.get_type(), col_obj_type.get_collation_type());
    cmp_func = is_oracle_mode ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
  }
  return cmp_func;
}

struct ObDatumComparator
{
public:
  ObDatumComparator(const ObDatumCmpFuncType cmp_func, int &ret, bool &equal)
    : cmp_func_(cmp_func),
      ret_(ret),
      equal_(equal)
  {}
  ~ObDatumComparator() {}
  OB_INLINE bool operator() (const ObDatum &datum1, const ObDatum &datum2)
  {
    int &ret = ret_;
    int cmp_ret = 0;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(cmp_func_(datum1, datum2, cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare datum", K(ret), K(datum1), K(datum2), K_(cmp_func));
    } else if (0 == cmp_ret && !equal_) {
      equal_ = true;
    }
    return cmp_ret < 0;
  }
private:
  ObDatumCmpFuncType cmp_func_;
  int &ret_;
  bool &equal_;
};

enum class ObFilterInCmpType {
  MERGE_SEARCH,
  BINARY_SEARCH_DICT,
  BINARY_SEARCH,
  HASH_SEARCH,
};

inline ObFilterInCmpType get_filter_in_cmp_type(
  const int64_t row_count,
  const int64_t param_count,
  const bool is_sorted_dict)
{
  // BINARY_HASH_THRESHOLD: means the threshold to choose BINARY_SEARCH or HASH_SEARCH
  // When the dictionary is unordered, the only variable available for iteration is param_count.
  // Testing has shown that when the data size is small, the overhead of binary search is
  // lower than the overhead of computing hashes.
  // Therefore, this threshold is temporarily set to a small value(8).
  static constexpr int64_t BINARY_HASH_THRESHOLD = 8;

  // HASH_BUCKETS: means the number of buckets(slots) in hashset.
  // This value is related to the performance of the hashset.
  const int64_t HASH_BUCKETS = hash::cal_next_prime(param_count * 2);

  ObFilterInCmpType cmp_type = ObFilterInCmpType::HASH_SEARCH;
  if (is_sorted_dict) {
    if (row_count > 3 * param_count) {
      // row_count >> param_count
      if (row_count > HASH_BUCKETS * 4) {
        cmp_type = ObFilterInCmpType::BINARY_SEARCH_DICT;
      } else {
        cmp_type = ObFilterInCmpType::MERGE_SEARCH;
      }
    } else if (row_count * 3 >= param_count) {
      // row_count ~~ param_count
      if (row_count > HASH_BUCKETS) {
        cmp_type = ObFilterInCmpType::MERGE_SEARCH;
      } else {
        cmp_type = ObFilterInCmpType::HASH_SEARCH;
      }
    } else {
      // row_count << param_count
      cmp_type = ObFilterInCmpType::HASH_SEARCH;
    }
  } else {
    // Unordered dict
    if (param_count <= BINARY_HASH_THRESHOLD) {
      cmp_type = ObFilterInCmpType::BINARY_SEARCH;
    } else {
      cmp_type = ObFilterInCmpType::HASH_SEARCH;
    }
  }
  return cmp_type;
}

}
}

#endif // OCEANBASE_STORAGE_OB_STORAGE_UTIL_
