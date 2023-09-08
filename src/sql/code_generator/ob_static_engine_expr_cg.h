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

#ifndef OCEANBASE_SQL_CODE_GENERATOR_OB_STATIC_ENGINE_EXPR_CG_
#define OCEANBASE_SQL_CODE_GENERATOR_OB_STATIC_ENGINE_EXPR_CG_

#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}

namespace sql
{

class ObPhysicalPlan;
class ObDMLStmt;
class ObRawExprUniqueSet;
class ObSQLSessionInfo;

class ObExprCGCtx
{
public:
  ObExprCGCtx(common::ObIAllocator &allocator,
              ObSQLSessionInfo *session,
              share::schema::ObSchemaGetterGuard *schema_guard,
              const uint64_t cur_cluster_version)
    : allocator_(&allocator), session_(session),
      schema_guard_(schema_guard), cur_cluster_version_(cur_cluster_version)
  {}

private:
DISALLOW_COPY_AND_ASSIGN(ObExprCGCtx);

public:
  common::ObIAllocator *allocator_;
  ObSQLSessionInfo *session_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  uint64_t cur_cluster_version_;
};

class ObRawExpr;
class ObRawExprFactory;
class RowDesc;
struct ObHiddenColumnItem;
class ObStaticEngineExprCG
{
public:
  static const int64_t STACK_OVERFLOW_CHECK_DEPTH = 16;
  static const int64_t OLTP_WORKLOAD_CARDINALITY = 16;
  static const int64_t DATUM_EVAL_INFO_SIZE = sizeof(ObDatum) + sizeof(ObEvalInfo);
  friend class ObRawExpr;
  struct TmpFrameInfo {
    TmpFrameInfo() : expr_start_pos_(0), frame_info_() {}
    TmpFrameInfo(uint64_t start_pos,
                 uint64_t expr_cnt,
                 uint32_t frame_idx,
                 uint32_t frame_size,
                 uint32_t zero_init_pos,
                 uint32_t zero_init_size)
      : expr_start_pos_(start_pos),
        frame_info_(expr_cnt, frame_idx, frame_size, zero_init_pos, zero_init_size)
    {}
    TO_STRING_KV(K_(expr_start_pos), K_(frame_info));
  public:
    uint64_t expr_start_pos_; // 当前frame第一个expr在该类ObExpr数组中偏移值
    ObFrameInfo frame_info_;
  };

  ObStaticEngineExprCG(common::ObIAllocator &allocator,
                       ObSQLSessionInfo *session,
                       share::schema::ObSchemaGetterGuard *schema_guard,
                       const int64_t original_param_cnt,
                       int64_t param_cnt,
                       const uint64_t cur_cluster_version)
    : allocator_(allocator),
      original_param_cnt_(original_param_cnt),
      param_cnt_(param_cnt),
      op_cg_ctx_(allocator_, session, schema_guard, cur_cluster_version),
      flying_param_cnt_(0),
      batch_size_(0),
      rt_question_mark_eval_(false),
      need_flatten_gen_col_(true),
      cur_cluster_version_(cur_cluster_version)
  {
  }
  virtual ~ObStaticEngineExprCG() {}

  //将所有raw exprs展开后, 生成ObExpr
  // @param [in] all_raw_exprs 未展开前的raw expr集合
  // @param [out] expr_info frame及rt_exprs相关信息
  int generate(const ObRawExprUniqueSet &all_raw_exprs, ObExprFrameInfo &expr_info);

  int generate(ObRawExpr *expr,
               ObRawExprUniqueSet &flattened_raw_exprs,
               ObExprFrameInfo &expr_info);

  int generate_calculable_exprs(const common::ObIArray<ObHiddenColumnItem> &calculable_exprs,
                                ObPreCalcExprFrameInfo &pre_calc_frame);

  int generate_calculable_expr(ObRawExpr *raw_expr,
                               ObPreCalcExprFrameInfo &pre_calc_frame);

  static int generate_rt_expr(const ObRawExpr &src,
                              common::ObIArray<ObRawExpr *> &exprs,
                              ObExpr *&dst);


  // Attention : Please think over before you have to use this function.
  // This function is different from generate_rt_expr.
  // It won't put raw_expr into cur_op_exprs_ because it doesn't need to be calculated.
  static void *get_left_value_rt_expr(const ObRawExpr &raw_expr);

  int detect_batch_size(const ObRawExprUniqueSet &exprs, int64_t &batch_size,
                        int64_t config_maxrows, int64_t config_target_maxsize,
                        const double scan_cardinality);

  // TP workload VS AP workload:
  // TableScan cardinality(accessed rows) is used to determine TP load so far
  // More sophisticated rules would be added when optimizer support new
  // vectorized cost model
  bool is_oltp_workload(const double scan_cardinality) const {
    bool is_oltp_workload = false;
    if (scan_cardinality < OLTP_WORKLOAD_CARDINALITY) {
      is_oltp_workload = true;
    }
    return is_oltp_workload;
  }

  void set_batch_size(const int64_t v) { batch_size_ = v; }

  void set_rt_question_mark_eval(const bool v) { rt_question_mark_eval_ = v; }

  static int generate_partial_expr_frame(const ObPhysicalPlan &plan,
                                         ObExprFrameInfo &partial_expr_frame_info,
                                         ObIArray<ObRawExpr *> &raw_exprs);

  void set_need_flatten_gen_col(const bool v) { need_flatten_gen_col_ = v; }

  static int gen_expr_with_row_desc(const ObRawExpr *expr,
                                    const RowDesc &row_desc,
                                    ObIAllocator &alloctor,
                                    ObSQLSessionInfo *session,
                                    share::schema::ObSchemaGetterGuard *schema_guard,
                                    ObTempExpr *&temp_expr);

  static int init_temp_expr_mem_size(ObTempExpr &temp_expr);

private:
  static ObExpr *get_rt_expr(const ObRawExpr &raw_expr);
  // 构造ObExpr, 并将ObExpr对应设置到对应ObRawExpr中
  // @param [in]  raw_exprs
  // @param [out] rt_exprs, 构造后的物理表达式
  int construct_exprs(const common::ObIArray<ObRawExpr *> &raw_exprs,
                      common::ObIArray<ObExpr> &rt_exprs);

  // 初始化raw_expr中对应的rt_expr, 并返回frame信息
  // @param [in/out] raw_exprs 用于生成rt_exprs的表达式集合
  // @param [out] expr_info frame及rt_exprs相关信息
  int cg_exprs(const common::ObIArray<ObRawExpr *> &raw_exprs,
               ObExprFrameInfo &expr_info);

  // init type_, datum_meta_, obj_meta_, obj_datum_map_, args_, arg_cnt_
  // row_dimension_, op_
  int cg_expr_basic(const common::ObIArray<ObRawExpr *> &raw_exprs);

  // init parent_cnt_, parents_
  int cg_expr_parents(const common::ObIArray<ObRawExpr *> &raw_exprs);

  // init eval_func_, inner_eval_func_, expr_ctx_id_, extra_
  int cg_expr_by_operator(const common::ObIArray<ObRawExpr *> &raw_exprs,
                          int64_t &total_ctx_cnt);

  // init res_buf_len_, frame_idx_, datum_off_, res_buf_off_
  // @param [in/out] raw_exprs
  // @param [out] expr_info frame相关信息
  int cg_all_frame_layout(const common::ObIArray<ObRawExpr *> &raw_exprs,
                          ObExprFrameInfo &expr_info);

  int cg_expr_basic_funcs(const common::ObIArray<ObRawExpr *> &raw_exprs);

  // alloc stack overflow check exprs.
  int alloc_so_check_exprs(const common::ObIArray<ObRawExpr *> &raw_exprs,
                           ObExprFrameInfo &expr_info);

  // calculate res_buf_len_ for exprs' datums
  int calc_exprs_res_buf_len(const common::ObIArray<ObRawExpr *> &raw_exprs);

  // create tmp frameinfo based on expr info
  int create_tmp_frameinfo(const common::ObIArray<ObRawExpr *> &raw_exprs,
                           common::ObIArray<TmpFrameInfo> &tmp_frame_infos,
                           int64_t &frame_index_pos);

  // 将表达式按所属frame类型分成4类
  // @param [in] raw_exprs 待分类的所有表达式
  // @param [out] const_exprs 属于const frame的所有表达式
  // @param [out] param_exprs 属于param frame的所有表达式
  // @param [out] dynamic_param_exprs 属于dynamic frame的所有表达式
  // @param [out] no_const_param_exprs 属于datum frame的所有表达式
  int classify_exprs(const common::ObIArray<ObRawExpr *> &raw_exprs,
                     common::ObIArray<ObRawExpr *> &const_exprs,
                     common::ObIArray<ObRawExpr *> &param_exprs,
                     common::ObIArray<ObRawExpr *> &dynamic_param_exprs,
                     common::ObIArray<ObRawExpr *> &no_const_param_exprs) const;

  // 初始化const expr在frame中布局
  // @param [in/out] const_exprs const frame的所有表达式
  // @param [in/out] frame_index_pos 已生成frame的偏移, 用于计算当前生成的frame
  //                 在所有frame中的index
  // @param [out]    frame_info_arr 所有const frame的相关信息
  int cg_const_frame_layout(const common::ObIArray<ObRawExpr *> &const_exprs,
                            int64_t &frame_index_pos,
                            common::ObIArray<ObFrameInfo> &frame_info_arr);

  // 初始化param expr在frame中布局
  // @param [in/out] param_exprs const frame的所有表达式
  // @param [in/out] frame_index_pos 已生成frame的偏移, 用于计算当前生成的frame
  //                 在所有frame中的index
  // @param [out]    frame_info_arr 所有const frame的相关信息
  int cg_param_frame_layout(const common::ObIArray<ObRawExpr *> &param_exprs,
                            int64_t &frame_index_pos,
                            common::ObIArray<ObFrameInfo> &frame_info_arr);

  // 初始化dynamic param expr在frame中布局
  // @param [in/out] const_exprs const frame的所有表达式
  // @param [in/out] frame_index_pos 已生成frame的偏移, 用于计算当前生成的frame
  //                 在所有frame中的index
  // @param [out]    frame_info_arr 所有const frame的相关信息
  int cg_dynamic_frame_layout(const common::ObIArray<ObRawExpr *> &exprs,
                              int64_t &frame_index_pos,
                              common::ObIArray<ObFrameInfo> &frame_info_arr);

  // 初始化非const和param expr在frame中布局
  // @param [in/out] exprs 非const和param expr的所有表达式
  // @param [in/out] frame_index_pos 已生成frame的偏移, 用于计算当前生成的frame
  //                 在所有frame中的index
  // @param [out]    frame_info_arr 所有datum frame的相关信息
  int cg_datum_frame_layouts(const common::ObIArray<ObRawExpr *> &exprs,
                            int64_t &frame_index_pos,
                            common::ObIArray<ObFrameInfo> &frame_info_arr);

  // 初始化frame中布局
  // @param [in/out] exprs待计算frame布局的所有表达式
  // @param [in] reserve_empty_string 对于string type类型是否再frame中分配reserve内存
  // @param [in] continuous_datum ObDatum + ObEvalInfo should be continuous
  // @param [in/out] frame_index_pos 已生成frame的偏移, 用于计算当前生成的frame
  //                 在所有frame中的index
  // @param [out]    frame_info_arr 所有frame的相关信息
  int cg_frame_layout(const common::ObIArray<ObRawExpr *> &exprs,
                      const bool reserve_empty_string,
                      const bool continuous_datum,
                      int64_t &frame_index_pos,
                      common::ObIArray<ObFrameInfo> &frame_info_arr);

  // new frame layout: the vector version
  int cg_frame_layout_vector_version(const common::ObIArray<ObRawExpr *> &exprs,
                      const bool continuous_datum,
                      int64_t &frame_index_pos,
                      common::ObIArray<ObFrameInfo> &frame_info_arr);


  // 分配常量表达式frame内存, 并初始化
  // @param [in] exprs 所有常量表达式
  // @param [in] const_frames 所有const frame相关信息
  // @param [out] frame_ptrs 初始化后的frame指针列表
  int alloc_const_frame(const common::ObIArray<ObRawExpr *> &exprs,
                        const common::ObIArray<ObFrameInfo> &const_frames,
                        common::ObIArray<char *> &frame_ptrs);

  // called after res_buf_len_ assigned to get the buffer size with dynamic reserved buffer.
  int64_t reserve_data_consume(const ObExpr &expr)
  {
    const bool need_dyn_buf = ObDynReserveBuf::supported(expr.datum_meta_.type_);
    return expr.res_buf_len_
        + (need_dyn_buf && expr.res_buf_len_ > 0 ? sizeof(ObDynReserveBuf) : 0);
  }

  // called after res_buf_len_ assigned to get the buffer size with dynamic reserved buffer.
  int64_t reserve_data_consume(const common::ObObjType &type)
  {
    const bool need_dyn_buf = ObDynReserveBuf::supported(type);
    // ObObjDatumMapType datum_map_type = ObDatum::get_obj_datum_map_type(type);
    auto res_buf_len =
        ObDatum::get_reserved_size(ObDatum::get_obj_datum_map_type(type));
    return res_buf_len +
           +(need_dyn_buf && res_buf_len > 0 ? sizeof(ObDynReserveBuf) : 0);
  }

  inline int64_t get_expr_datums_count(const ObExpr &expr)
  {
    OB_ASSERT(!(expr.is_batch_result() && batch_size_ == 0));
    return expr.is_batch_result() ? batch_size_ : 1;
  }

  inline int64_t get_expr_skip_vector_size(const ObExpr &expr)
  {
    return expr.is_batch_result() ? ObBitVector::memory_size(batch_size_) : 0;
  }
  int64_t dynamic_buf_header_size(const ObExpr &expr)
  {
    const bool need_dyn_buf = ObDynReserveBuf::supported(expr.datum_meta_.type_);
    return (need_dyn_buf ? get_expr_datums_count(expr) * sizeof(ObDynReserveBuf)
                         : 0);
  }

  // vector version of reserve_data_consume
  int64_t reserve_datums_buf_len(const ObExpr &expr)
  {
    const bool need_dyn_buf = ObDynReserveBuf::supported(expr.datum_meta_.type_);
    return expr.res_buf_len_ *
               get_expr_datums_count(expr) /* reserve datums part */
           + (need_dyn_buf ? get_expr_datums_count(expr) * sizeof(ObDynReserveBuf)
                           : 0) /* dynamic datums part */;
  }

  inline int64_t get_datums_header_size(const ObExpr &expr)
  {
    return sizeof(ObDatum) * get_expr_datums_count(expr);
  }

  int arrange_datum_data(common::ObIArray<ObRawExpr *> &exprs,
                         const ObFrameInfo &frame,
                         const bool continuous_datum);

  // vector version of arrange_datum_data
  int arrange_datums_data(common::ObIArray<ObRawExpr *> &exprs,
                         const ObFrameInfo &frame,
                         const bool continuous_datum);

  int inner_generate_calculable_exprs(
                                const common::ObIArray<ObHiddenColumnItem> &calculable_exprs,
                                ObPreCalcExprFrameInfo &expr_info);

  // total datums size: header + reserved data
  int64_t get_expr_datums_size(const ObExpr &expr) {
    return get_expr_datums_header_size(expr) + reserve_datums_buf_len(expr);
  }

  // datums meta/header size vector version.
  // four parts:
  // - datum instance vector
  // - EvalInfo instance
  // - EvalFlag(BitVector) instance + BitVector data
  // - SkipBitmap(BitVector) + BitVector data
  int64_t get_expr_datums_header_size(const ObExpr &expr) {
    return get_datums_header_size(expr) + sizeof(ObEvalInfo) +
           2 * get_expr_skip_vector_size(expr);
  }
  // datum meta/header size non-vector version.
  // two parts:
  // - datum instance
  // - EvalInfo instance
  int get_expr_datum_fixed_header_size() {
    return sizeof(ObDatum) + sizeof(ObEvalInfo);
  }
  // Fetch non const exprs
  // @param [in] raw_exprs all raw exprs
  // @param [out] no_const_param_exprs exprs using datum frame
  int get_vectorized_exprs(const common::ObIArray<ObRawExpr *> &raw_exprs,
                     common::ObIArray<ObRawExpr *> &no_const_param_exprs) const;
  bool is_vectorized_expr(const ObRawExpr *raw_expr) const;
  int compute_max_batch_size(const ObRawExpr *raw_expr);

  enum class ObExprBatchSize {
    one = 1,
    small = 8,
    full = 65535
  };
  // Certain exprs can NOT be executed vectorizely. Check the exps within this
  // routine
  ObExprBatchSize
  get_expr_execute_size(const common::ObIArray<ObRawExpr *> &raw_exprs);
  inline bool is_large_data(ObObjType type) {
    bool b_large_data = false;
    if (type == ObLongTextType || type == ObMediumTextType ||
        type == ObLobType) {
      b_large_data = true;
    }
    return b_large_data;
  }

  // get the frame idx of param frames and datum index (in frame) by index of param store.
  void get_param_frame_idx(const int64_t idx, int64_t &frame_idx, int64_t &datum_idx);

  int divide_probably_local_exprs(common::ObIArray<ObRawExpr *> &exprs);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObStaticEngineExprCG);

private:
  //用于分配cg出来的expr对象的内存
  common::ObIAllocator &allocator_;
  //所有参数化后的常量对象
  DatumParamStore *param_store_;

  // original param cnt, see comment of ObPhysicalPlanCtx
  int64_t original_param_cnt_;

  //count of param store
  int64_t param_cnt_;
  // operator cg的上下文
  ObExprCGCtx op_cg_ctx_;
  // Count of param store in generating, for calculable expressions CG.
  int64_t flying_param_cnt_;
  // batch size detected in generate()
  int64_t batch_size_;

  // evaluate question mark expression at run time, used in PL which has no param frames.
  bool rt_question_mark_eval_;
  //is code generate temp expr witch used in table location
  bool need_flatten_gen_col_;
  uint64_t cur_cluster_version_;
};

} // end namespace sql
} // end namespace oceanbase
#endif /*OCEANBASE_SQL_CODE_GENERATOR_OB_STATIC_ENGINE_EXPR_CG_*/
