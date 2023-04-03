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

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_TABLE_OB_FAKE_TABLE_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_TABLE_OB_FAKE_TABLE_H_

#include "share/object/ob_obj_cast.h"
#include "common/row/ob_row_store.h"
#include "lib/time/ob_time_utility.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_allocator.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

#define null static_cast<const char*>(NULL)
#define timestamp(val) convert_to_timestamp(val)

#define COL(val) \
  ({\
    ObObj *_obj = new ObObj(); \
    int err = OB_SUCCESS; \
    if (OB_SUCCESS != (err = set_value(*_obj, val))) { \
      _OB_LOG(WARN, "fail to set value, err=%d", err); \
    } \
    _obj; \
  })

#define COL_T(type, val) \
  ({ \
    ObObj *_obj = new ObObj(); \
    _obj->set_##type(val); \
    _obj; \
  })

#define ADD_ROW(table_op, args...) \
  if (OB_SUCC(ret)) { \
    ObObj _cells[OB_MAX_COLUMN_NUMBER]; \
    ObNewRow _row; \
    _row.cells_ = _cells; \
    _row.count_ = table_op.get_column_count(); \
    if (OB_SUCCESS != (ret = fill_row(_row, ##args))) { \
      _OB_LOG(WARN, "fail to fill row, ret=%d", ret); \
    } else if (OB_SUCCESS != (ret = table_op.add_row(_row))) { \
      _OB_LOG(WARN, "fail to add row, ret=%d", ret); \
    } \
  }

#define EXCEPT_RESULT(_ctx, _result_table, op, collation) \
    EXCEPT_RESULT_WITH_IDX(_ctx, _result_table, op, 0, 0, collation)

#define EXCEPT_RESULT_SET(_ctx, _result_table, op, start_idx, end_idx, collation) \
    EXCEPT_RESULT_WITH_IDX(_ctx, _result_table, op, start_idx, end_idx, collation)

#define EXCEPT_RESULT_WITH_IDX(_ctx, _result_table, op, start_idx, end_idx, collation) \
  if (OB_SUCC(ret)) { \
    int _ret = OB_SUCCESS; \
    const ObNewRow *result_row = NULL; \
    while(OB_SUCCESS == (_ret = op.get_next_row(_ctx, result_row))) { \
      const ObNewRow *except_row = NULL; \
      printf("row=%s\n", to_cstring(*result_row)); \
      ASSERT_EQ(OB_SUCCESS, _result_table.get_next_row(_ctx, except_row)); \
      printf("except_row=%s\n", to_cstring(*except_row)); \
      ASSERT_TRUE(except_row->count_ == result_row->count_); \
      for (int64_t i = start_idx; i < end_idx; ++i) { \
        printf("index=%ld, cell=%s, respect_cell=%s\n", i, to_cstring(result_row->cells_[i]), to_cstring(except_row->cells_[i])); \
        ASSERT_TRUE(0 == except_row->cells_[i].compare(result_row->cells_[i], collation)); \
      } \
      result_row = NULL; \
    } \
    ASSERT_EQ(OB_ITER_END, _ret); \
  }

namespace oceanbase
{
namespace common
{
extern void init_global_memory_pool();
}  // namespace common
}  // namespace oceanbase

typedef struct TimeStampWrap
{
  int64_t usec_;
} TimeStampWrap;

TimeStampWrap convert_to_timestamp(int64_t usec)
{
  TimeStampWrap time_stamp;
  time_stamp.usec_ = usec;
  return time_stamp;
}

TimeStampWrap convert_to_timestamp(const char *str)
{
  TimeStampWrap time_stamp;
  ObString date = ObString::make_string(str);

  //ObTimeUtility::str_to_usec(date, time_stamp.usec_);
  ObTimeConvertCtx cvrt_ctx(NULL, false);
  (void)ObTimeConverter::str_to_datetime(date, cvrt_ctx, time_stamp.usec_, NULL);

  return time_stamp;
}

int set_value(ObObj &obj, int val)
{
  obj.set_int(static_cast<int64_t>(val));
  return OB_SUCCESS;
}

int set_value(ObObj &obj, int64_t val)
{
  obj.set_int(val);
  return OB_SUCCESS;
}

int set_value(ObObj &obj, float val)
{
  int ret = OB_SUCCESS;
  ObObj obj1;
  obj1.set_float(val);
  ObCastCtx cast_ctx(global_default_allocator,
                     NULL,
                     0,
                     CM_NONE,
                     CS_TYPE_INVALID,
                     NULL);
  if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, obj1, obj))) {
    _OB_LOG(WARN, "fail to cast obj, ret=%d", ret);
  }
  return ret;
}

int set_value(ObObj &obj, double val)
{
  ObObj obj1;
  int ret = OB_SUCCESS;
  obj1.set_double(val);
  ObCastCtx cast_ctx(global_default_allocator,
                     NULL,
                     0,
                     CM_NONE,
                     CS_TYPE_INVALID,
                     NULL);
  if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, obj1, obj))) {
    _OB_LOG(WARN, "fail to cast obj, ret=%d", ret);
  }
  return ret;
}

int set_value(ObObj &obj, const char *val)
{
  if (NULL == val) {
    obj.set_null();
  } else {
    obj.set_varchar(val, static_cast<ObString::obstr_size_t>(strlen(val)));
  }
  return OB_SUCCESS;
}

int set_value(ObObj &obj, const ObObj &val)
{
  obj = val;
  return OB_SUCCESS;
}

int set_value(ObObj &obj, TimeStampWrap val)
{
  obj.set_timestamp(val.usec_);
  return OB_SUCCESS;
}
int fill_row(ObNewRow &row, ...)
{
  int ret = OB_SUCCESS;
  ObObj *ptr = NULL;
  va_list ap;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wvarargs"

  va_start(ap, row);
  for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; ++i) {
    if (NULL == (ptr = va_arg(ap, ObObj*))) {
      ret = OB_INVALID_ARGUMENT;
      _OB_LOG(WARN, "argument is null");
    } else {
      row.cells_[i] = *ptr;
      row.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      delete ptr;
      ptr = NULL;
    }
  }
  va_end(ap);

#pragma GCC diagnostic pop

  return ret;
}
static ObArenaAllocator alloc_;
class ObFakeTable : public ObNoChildrenPhyOperator
{
private:
  class ObFakeTableCtx : public ObPhyOperatorCtx
  {
  public:
    ObFakeTableCtx(ObExecContext &ctx)
        : ObPhyOperatorCtx(ctx)
    {
    }
    int init(int64_t projector_size)
    {
      int ret = OB_SUCCESS;
      projector_size_ = projector_size;
      projector_ = static_cast<int32_t*>(exec_ctx_.get_allocator().alloc(sizeof(int32_t) * projector_size_));
      if (NULL == projector_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < projector_size_; i++) {
          projector_[i] = static_cast<int32_t>(i);
        }
      }
      return ret;
    }
    virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
    int64_t projector_size_;
    int32_t *projector_;

  private:
    ObRowStore::Iterator row_store_it_;
    friend class ObFakeTable;
  };

public:
  ObFakeTable(ObIAllocator &alloc) : ObNoChildrenPhyOperator(alloc), no_rescan_(false)
  {
  }

  ObFakeTable() : ObNoChildrenPhyOperator(alloc_), no_rescan_(false)
  {
  }
  ~ObFakeTable()
  {
  }

  void set_no_rescan() { no_rescan_ = true; }
  void reset()
  {
    row_store_.reset();
    ObNoChildrenPhyOperator::reset();
  }

  int add_row(const ObNewRow &row)
  {
    return row_store_.add_row(row);
  }

  ObPhyOperatorType get_type() const { return PHY_FAKE_TABLE; }

  int inner_open(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    ObFakeTableCtx *table_ctx = NULL;

    if (OB_SUCCESS != (ret = init_op_ctx(ctx))) {
      _OB_LOG(WARN, "fail to init operator context, ret=%d", ret);
    } else if (NULL == (table_ctx = GET_PHY_OPERATOR_CTX(ObFakeTableCtx, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "fail to get physical operator context");
    } else if (OB_FAIL(table_ctx->init(column_count_))) {
      _OB_LOG(WARN, "fail to init project size, ret=%d", ret);
    } else {
      table_ctx->row_store_it_ = row_store_.begin();
    }
    return ret;
  }

  int rescan(ObExecContext &ctx) const
  {
  	if (no_rescan_) {
  		return OB_SUCCESS;
  	} else {
			ObFakeTableCtx *fake_table_ctx = NULL;
			fake_table_ctx = GET_PHY_OPERATOR_CTX(ObFakeTableCtx, ctx, get_id());
			OB_ASSERT(fake_table_ctx);
			fake_table_ctx->row_store_it_ = row_store_.begin();
			return ObNoChildrenPhyOperator::rescan(ctx);
  	}
  }

  int inner_close(ObExecContext &ctx) const
  {
    UNUSED(ctx);
    return OB_SUCCESS;
  }
private:
  /**
   * @brief create operator context, only child operator can know it's specific operator type,
   * so must be overwrited by child operator,
   * @param ctx[in], execute context
   * @param op_ctx[out], the pointer of operator context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    ObPhyOperatorCtx *op_ctx = NULL;
    if (OB_SUCCESS != (ret = CREATE_PHY_OPERATOR_CTX(ObFakeTableCtx,
                                                     ctx,
                                                     get_id(),
                                                     get_type(),
                                                     op_ctx))) {
      SQL_EXE_LOG(WARN, "create physical operator context failed", K(ret));
    } else if (OB_SUCCESS != (ret = op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
      SQL_EXE_LOG(WARN, "create current row failed", K(ret));
    }
    return ret;
  }
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext &ctx, const ObNewRow *&row) const
  {
    int ret = OB_SUCCESS;
    ObFakeTableCtx *table_ctx = NULL;

    if (NULL == (table_ctx = GET_PHY_OPERATOR_CTX(ObFakeTableCtx, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "fail to get physical operator context");
    } else if (OB_SUCCESS != (ret = table_ctx->row_store_it_.get_next_row(
        table_ctx->get_cur_row()))) {
      if (OB_ITER_END != ret) {
        _OB_LOG(WARN, "fail to get next row, ret=%d", ret);
      }
    } else {
      row = &table_ctx->get_cur_row();
      const_cast<ObNewRow *>(row)->projector_ = table_ctx->projector_;
      const_cast<ObNewRow *>(row)->projector_size_ = table_ctx->projector_size_;
    }
    return ret;
  }

  virtual int64_t to_string_kv(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_KV(N_ROW_STORE, row_store_);
    return pos;
  }

  DISALLOW_COPY_AND_ASSIGN(ObFakeTable);
private:
  ObRowStore row_store_;
  bool no_rescan_;
};

#endif /* OCEANBASE_UNITTEST_SQL_ENGINE_TABLE_OB_FAKE_TABLE_H_ */
