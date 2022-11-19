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

#ifndef UNITTEST_SQL_OPTIMIZER_OB_STDIN_ITER_H_
#define UNITTEST_SQL_OPTIMIZER_OB_STDIN_ITER_H_

#include "common/object/ob_object.h"
#include "common/row/ob_row_store.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_string.h"
#include <iostream>
#include <fstream>
#include <string>
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "lib/container/ob_se_array.h"
#include "common/object/ob_obj_type.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{

using namespace sql;

namespace common
{

class ObMockIterWithLimit : public ObNewRowIterator {
public:
  ObMockIterWithLimit() : need_row_count_(0), got_row_count_(0) {};
  virtual ~ObMockIterWithLimit() {};

  void set_need_row_count(int64_t count) {
    need_row_count_ = count;
  }

  int64_t get_need_row_count() {
    return need_row_count_;
  }

  bool is_iter_end() {
    return need_row_count_ == got_row_count_;
  }

protected:

  void advance_iter() {
      ++got_row_count_;
    }

  void rescan() {
    got_row_count_ = 0;
  }

  int64_t need_row_count_;
  int64_t got_row_count_;
};



#define VARCHAR_ACT_TYPE                ObVarcharType
#define TINYINT_ACT_TYPE                ObTinyIntType
#define SMALLINT_ACT_TYPE               ObSmallIntType
#define MEDIUMINT_ACT_TYPE              ObMediumIntType
#define INT_ACT_TYPE                    ObInt32Type
#define BIGINT_ACT_TYPE                 ObIntType
#define TINYINT_UNSIGNED_ACT_TYPE       ObUTinyIntType
#define SMALLINT_UNSIGNED_ACT_TYPE      ObUSmallIntType
#define MEDIUMINT_UNSIGNED_ACT_TYPE     ObUMediumIntType
#define INT_UNSIGNED_ACT_TYPE           ObUInt32Type
#define BIGINT_UNSIGNED_ACT_TYPE        ObUint64Type
#define FLOAT_ACT_TYPE                  ObFloatType
#define DOUBLE_ACT_TYPE                 ObDoubleType
#define TS_ACT_TYPE                     ObTimestampType
#define NULLT_ACT_TYPE                  ObNullType
#define NUMBER_ACT_TYPE                 ObNumberType

#define AT_(type) type##_ACT_TYPE

#define RANDOM_CELL_GEN_CLASS_NAME(type) Random##type##CellGen

#define DECLARE_RANDOM_CELL_GEN(type) \
class RANDOM_CELL_GEN_CLASS_NAME(type): public RandomCellGen \
{ \
public: \
   RANDOM_CELL_GEN_CLASS_NAME(type)() { type_ = AT_(type); }; \
   virtual ObObj gen(ObIAllocator &buf, int64_t seed); \
}

#define RANDOM_CELL_GEN_CTR_NAME(type) get_##type##_gen
#define DECLARE_RANDOM_CELL_GEN_CTR(type) \
static RandomCellGen *RANDOM_CELL_GEN_CTR_NAME(type) (ObIAllocator &buf) \
{ \
  void *gen_buf = buf.alloc(sizeof(RANDOM_CELL_GEN_CLASS_NAME(type))); \
  if (NULL == gen_buf) return NULL; \
  else return new (gen_buf) RANDOM_CELL_GEN_CLASS_NAME(type)();  \
}


class ObStdinIter : public ObMockIterWithLimit
{
public:

  enum EOFBehavior {
    TERMINATE,
    REWIND,
    RANDOM
  };

private:

  EOFBehavior on_eof_;
  int64_t get_count_;
  bool pure_random_;

  class IRandomCellGen {
  public:
    virtual ObObj gen(ObIAllocator &buf, int64_t seed) = 0;
    void set_max (ObObj max) { max_ = max; }
    void set_min (ObObj min) { min_ = min; }
    ObObj max_;
    ObObj min_;
    TO_STRING_KV(K("IRandomCellGen"));
  };


  class RandomCellGen: public IRandomCellGen
  {
  public:
    RandomCellGen() : type_(ObNullType), acc_(NULL), need_random(false), length(0), common_prefix_len(10), my_buf(NULL) {};
    virtual ObObj gen(ObIAllocator &buf, int64_t seed) = 0;
    ObObjType type_;
    const ObAccuracy *acc_;
    bool need_random;
    int64_t length;
    int64_t common_prefix_len;
    char *my_buf;
  };

  typedef RandomCellGen * (*RandomCellGenCtr) (ObIAllocator &buf);

  DECLARE_RANDOM_CELL_GEN(BIGINT);
  DECLARE_RANDOM_CELL_GEN(VARCHAR);
  DECLARE_RANDOM_CELL_GEN(NULLT);
  DECLARE_RANDOM_CELL_GEN(TS);
  DECLARE_RANDOM_CELL_GEN(NUMBER);
  DECLARE_RANDOM_CELL_GEN(DOUBLE);
  DECLARE_RANDOM_CELL_GEN(FLOAT);

  DECLARE_RANDOM_CELL_GEN_CTR(BIGINT);
  DECLARE_RANDOM_CELL_GEN_CTR(VARCHAR);
  DECLARE_RANDOM_CELL_GEN_CTR(NULLT);
  DECLARE_RANDOM_CELL_GEN_CTR(TS);
  DECLARE_RANDOM_CELL_GEN_CTR(NUMBER);
  DECLARE_RANDOM_CELL_GEN_CTR(DOUBLE);
  DECLARE_RANDOM_CELL_GEN_CTR(FLOAT);


  bool map_inited;
  hash::ObHashMap<ObString, ObObjType> str_to_type_map_mysql_;
  RandomCellGenCtr random_cell_gen_ctrs[ObMaxType];
  oceanbase::common::ObSEArray<oceanbase::share::schema::ObColDesc, 10> col_descs;
  oceanbase::common::ObSEArray<IRandomCellGen *, 10> random_cell_generators;

  mutable int64_t time_consumed_;

  ObIAllocator &buf_;

  const static int64_t MAX_COLUMN_COUNT = 512;

  bool need_random[MAX_COLUMN_COUNT];



  const oceanbase::share::schema::ObTableSchema *schema_;
  static const int64_t STR_MAIN_BUF_SIZE = 655360;
  char str_main_buf_[STR_MAIN_BUF_SIZE];
  int64_t str_main_buf_ptr;

public:
  ObStdinIter(ObIAllocator &buf) :on_eof_(TERMINATE), time_consumed_(0), buf_(buf), str_main_buf_ptr(0), seed(0), seed_min(0),
  seed_max(INT64_MAX), seed_step(1), seed_step_length(1), seed_step_current(0)
  {
    memset(need_random,false,sizeof(need_random)/sizeof(need_random[0]));
    memset(random_cell_gen_ctrs,0,sizeof(random_cell_gen_ctrs)/sizeof(random_cell_gen_ctrs[0]));
    memset(str_main_buf_,0,sizeof(str_main_buf_));
    random_cell_gen_ctrs[ObIntType] = RANDOM_CELL_GEN_CTR_NAME(BIGINT);
    random_cell_gen_ctrs[ObVarcharType] = RANDOM_CELL_GEN_CTR_NAME(VARCHAR);
    random_cell_gen_ctrs[ObTimestampType] = RANDOM_CELL_GEN_CTR_NAME(TS);
    random_cell_gen_ctrs[ObNumberType] = RANDOM_CELL_GEN_CTR_NAME(NUMBER);
    random_cell_gen_ctrs[ObDoubleType] = RANDOM_CELL_GEN_CTR_NAME(DOUBLE);
    random_cell_gen_ctrs[ObFloatType] = RANDOM_CELL_GEN_CTR_NAME(FLOAT);
    init_type_map();
  }
  virtual ~ObStdinIter() {}
  int init_type_map();
  virtual void reset() {};
  virtual int get_next_row(ObNewRow *&row);

  //int init_schema(string &line, ObIAllocator &buf);
  int init_schema(const oceanbase::share::schema::ObTableSchema &schema);

  int extract_column_info(const std::string &line, ObIArray<oceanbase::common::ObObjType> &column_info);
  int lookup_type_from_str(const char *str,
                                                   int64_t len,
                                                   ObObjType &type);
  int init_random_cell_gen(const oceanbase::share::schema::ObTableSchema &schema, const ObIArray<oceanbase::share::schema::ObColDesc> &column_types, ObIAllocator &buf);
  void set_common_prefix_len(int64_t len);

  static char rand_char();

  void set_pure_random(bool random) {
    pure_random_ = random;
  }

  void set_random_column(int64_t i) {
    need_random[i] = true;
  }


  void set_eof_behavior(EOFBehavior behavior) {
    on_eof_ = behavior;
  }


  void init_projector(int64_t count) {
    if (NULL == new_row_.projector_) {
      new_row_.projector_ = static_cast<int32_t *>(buf_.alloc(count * sizeof(int32_t)));
    }
  }

  void add_projector(int64_t i) {
    new_row_.projector_[new_row_.projector_size_] = static_cast<int32_t>(i);
    ++new_row_.projector_size_;
  }

  int rescan() {
    seed = seed_min;
    seed_step_current = 0;
    ObMockIterWithLimit::rescan();
    return OB_SUCCESS;
  }

  int build_obj_from_str(const char *str,
                                                 int64_t len,
                                                 ObObjType type,
                                                 ObObj &obj,
                                                 ObIAllocator& buf);
  int fill_new_row_from_line(const std::string &line, ObNewRow &row, oceanbase::common::ObIArray<oceanbase::share::schema::ObColDesc> &col_desc, ObIAllocator &allocator);
  int fill_new_row_with_random(ObNewRow &row, ObIAllocator &allocator) const;
  int gen_random_cell_for_column(const int64_t column_id, ObObj& obj, ObIAllocator &buf, int64_t seed) const;
  static int cast_to_type(const ObObj &src, ObObj& res, ObObjType type, ObIAllocator &buf, const ObAccuracy *acc = NULL);

  const oceanbase::share::schema::ObTableSchema *get_schema() {return schema_;}

  mutable int64_t seed;

  mutable int64_t seed_min;
  mutable int64_t seed_max;
  mutable int64_t seed_step;
  mutable int64_t seed_step_length;
  mutable int64_t seed_step_current;



  ObNewRow new_row_;

};
static ObArenaAllocator alloc_;
class MyMockOperator : public ObPhyOperator
{
public:

  mutable int64_t time_;

  class MyMockCtx : public ObPhyOperatorCtx
  {

    friend class MyMockOperator;
  public:
    MyMockCtx(ObExecContext &ctx) : ObPhyOperatorCtx(ctx){
      UNUSED(ctx);
    }
    virtual ~MyMockCtx(){};
    virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
  };

  MyMockOperator() : ObPhyOperator(alloc_)
  {
  }
  ObStdinIter *iter;

  oceanbase::sql::ObPhyOperatorType get_type() const { return op_type_; }
  void set_type(ObPhyOperatorType op_type) { op_type_ = op_type; }

  virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator) {
    UNUSED(child_idx);
    UNUSED(child_operator);
    return OB_SUCCESS;
  };
  virtual ObPhyOperator *get_child(int32_t child_idx) const {
    UNUSED(child_idx);
    return NULL;
  }
  virtual int32_t get_child_num() const {
    return OB_SUCCESS;
  };
  virtual int inner_open(ObExecContext &ctx) const {
    int ret = OB_SUCCESS;

    int64_t open_start = ObTimeUtility::current_time();

    if (OB_FAIL(init_op_ctx(ctx))) {
      SQL_OPT_LOG(WARN, "failed to init op ctx");
    } else {

    }

    int64_t open_end = ObTimeUtility::current_time();

    time_ = open_end - open_start;

    return ret;
  };


  virtual int close(ObExecContext &ctx) const {
    UNUSED(ctx);
    return OB_SUCCESS;
  };
  virtual int init_op_ctx(ObExecContext &ctx) const {
    int ret = OB_SUCCESS;
    ObPhyOperatorCtx *op_ctx = NULL;
    if (OB_FAIL(inner_create_operator_ctx(ctx, op_ctx))) {
      SQL_OPT_LOG(WARN, "failed to init op ctx");
    } else if (OB_FAIL(op_ctx->create_cur_row(iter->get_schema()->get_column_count(), NULL, 0))) {
      SQL_OPT_LOG(WARN, "failed to create cur row");
    } else {
      op_ctx->get_cur_row().projector_ = projector_;
      op_ctx->get_cur_row().projector_size_ = projector_size_;
    }
    return ret;
  };

  int inner_create_operator_ctx(ObExecContext &ctx, ObPhyOperatorCtx *&op_ctx) const {
    int ret = OB_SUCCESS;
    MyMockCtx* mock_ctx = NULL;
    if(OB_FAIL(CREATE_PHY_OPERATOR_CTX(MyMockCtx, ctx, get_id(), get_type(), mock_ctx))) {
      SQL_OPT_LOG(WARN, "failed to init op ctx");
    } else {
      op_ctx = mock_ctx;
    }
    return ret;
  }

  virtual int inner_get_next_row(ObExecContext &ctx, const common::ObNewRow *&row) const {
    UNUSED(ctx);
    int ret = OB_SUCCESS;
    if (NULL == iter) {
      ret = OB_NOT_INIT;
    } else {
      MyMockCtx *mock_ctx = NULL;
      int64_t get_start = ObTimeUtility::current_time();
      if (NULL == (mock_ctx = GET_PHY_OPERATOR_CTX(MyMockCtx, ctx, get_id()))) {

      } else {


        common::ObNewRow *cur_row = &mock_ctx->get_cur_row();
        ret =  iter->get_next_row(cur_row);

        if (OB_SUCC(ret)) {
          row = cur_row;
        }
      }

      int64_t get_end = ObTimeUtility::current_time();
      time_ += get_end - get_start;
    }
    return ret;
  };
  virtual int init(ObExecContext &ctx, ObTaskInfo &task_info, ObPhyOperator &op) {
    UNUSED(ctx);
    UNUSED(task_info);
    UNUSED(op);
    return OB_SUCCESS;
  };

  virtual int rescan(ObExecContext &ctx) const {
    UNUSED(ctx);
    if (NULL == iter) {
      return OB_NOT_INIT;
    } else {
      return iter->rescan();
    }
  }



  ObPhyOperatorType op_type_;
};


}
}





#endif /* UNITTEST_SQL_OPTIMIZER_OB_STDIN_ITER_H_ */
