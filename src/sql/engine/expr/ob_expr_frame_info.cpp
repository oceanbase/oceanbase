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
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "sql/engine/expr/ob_expr_extra_info_factory.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER(ObFrameInfo, expr_cnt_, frame_idx_, frame_size_);

int ObExprFrameInfo::pre_alloc_exec_memory(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  uint64_t frame_cnt = 0;
  char** frames = NULL;
  char** expr_ctx_arr = NULL;
  ObPhysicalPlanCtx* phy_ctx = NULL;
  if (NULL == (phy_ctx = exec_ctx.get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(phy_ctx), K(ret));
  } else if (OB_FAIL(alloc_frame(exec_ctx.get_allocator(), *phy_ctx, frame_cnt, frames))) {
    LOG_WARN("fail to alloc frame", K(ret));
  } else {
    exec_ctx.set_frame_cnt(frame_cnt);
    exec_ctx.set_frames(frames);
  }

  return ret;
}

#define ALLOC_FRAME_MEM(frame_info_arr)                                                           \
  for (int64_t i = 0; OB_SUCC(ret) && i < frame_info_arr.count(); i++) {                          \
    char* frame_mem = static_cast<char*>(exec_allocator.alloc(frame_info_arr.at(i).frame_size_)); \
    if (OB_ISNULL(frame_mem)) {                                                                   \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                            \
      LOG_WARN("alloc memory failed", K(ret), K(frame_info_arr.at(i).frame_size_));               \
    } else {                                                                                      \
      memset(frame_mem, 0, frame_info_arr.at(i).frame_size_);                                     \
      frames[frame_idx++] = frame_mem;                                                            \
    }                                                                                             \
  }

int ObExprFrameInfo::alloc_frame(
    common::ObIAllocator& exec_allocator, ObPhysicalPlanCtx& phy_ctx, uint64_t& frame_cnt, char**& frames) const
{
  int ret = common::OB_SUCCESS;
  frame_cnt = const_frame_ptrs_.count() + param_frame_.count() + dynamic_frame_.count() + datum_frame_.count();
  if (frame_cnt == 0) {
    // do nothing
  } else if (NULL == (frames = static_cast<char**>(exec_allocator.alloc(frame_cnt * sizeof(char*))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(frame_cnt));
  } else {
    int64_t frame_idx = 0;  // frame idx
    OB_ASSERT(const_frame_ptrs_.count() == const_frame_.count());
    OB_ASSERT(phy_ctx.get_param_frame_ptrs().count() == param_frame_.count());
    for (int64_t i = 0; i < const_frame_ptrs_.count(); i++) {
      frames[frame_idx++] = const_frame_ptrs_.at(i);
    }
    for (int64_t i = 0; i < phy_ctx.get_param_frame_ptrs().count(); i++) {
      frames[frame_idx++] = phy_ctx.get_param_frame_ptrs().at(i);
    }
    int64_t begin_idx = frame_idx;
    const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    ALLOC_FRAME_MEM(dynamic_frame_);
    for (int64_t i = 0; OB_SUCC(ret) && i < dynamic_frame_.count(); ++i) {
      char* cur_frame = frames[begin_idx + i];
      for (int64_t j = 0; j < dynamic_frame_.at(i).expr_cnt_; ++j) {
        ObDatum* datum = reinterpret_cast<ObDatum*>(cur_frame + j * datum_eval_info_size);
        datum->set_null();
      }
    }
    ALLOC_FRAME_MEM(datum_frame_);
  }

  return ret;
}
#undef ALLOC_FRAME_MEM

int ObPreCalcExprFrameInfo::assign(const ObPreCalcExprFrameInfo& other, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  need_ctx_cnt_ = other.need_ctx_cnt_;

  if (OB_FAIL(rt_exprs_.assign(other.rt_exprs_))) {
    LOG_WARN("failed to copy rt exprs", K(ret));
  } else if (OB_FAIL(const_frame_.assign(other.const_frame_))) {
    LOG_WARN("failed to copy const frame ptrs", K(ret));
  } else if (OB_FAIL(param_frame_.assign(other.param_frame_))) {
    LOG_WARN("failed to copy param frames", K(ret));
  } else if (OB_FAIL(dynamic_frame_.assign(other.dynamic_frame_))) {
    LOG_WARN("failed to copy dynamic frames", K(ret));
  } else if (OB_FAIL(datum_frame_.assign(other.datum_frame_))) {
    LOG_WARN("failed to copy datum frame", K(ret));
  } else if (const_frame_ptrs_.prepare_allocate(other.const_frame_ptrs_.count())) {
    LOG_WARN("failed to prepare allocate array", K(ret));
  } else {
    char* frame_mem = NULL;
    for (int i = 0; OB_SUCC(ret) && i < other.const_frame_.count(); i++) {
      if (OB_ISNULL(frame_mem = (char*)allocator.alloc(other.const_frame_.at(i).frame_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        MEMCPY(frame_mem, other.const_frame_ptrs_.at(i), other.const_frame_.at(i).frame_size_);
        const_frame_ptrs_.at(i) = frame_mem;
      }
      // set datum ptr
      for (int j = 0; OB_SUCC(ret) && j < other.const_frame_.at(i).expr_cnt_; j++) {
        char* other_frame_mem = other.const_frame_ptrs_.at(i);
        int64_t other_frame_size = other.const_frame_.at(i).frame_size_;
        const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
        ObDatum* other_expr_datum = reinterpret_cast<ObDatum*>(other_frame_mem + j * datum_eval_info_size);
        ObDatum* expr_datum = reinterpret_cast<ObDatum*>(frame_mem + j * datum_eval_info_size);
        if (NULL == other_expr_datum->ptr_) {
          // do nothing
        } else if ((other_expr_datum->ptr_ < other_frame_mem ||
                       other_expr_datum->ptr_ >= other_frame_mem + other_frame_size)) {
          // if datum's ptr_ does not locate in frame_mem, it must be deep copied separately
          void* extra_buf = NULL;
          if (0 == other_expr_datum->len_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("length is null and ptr not null", K(ret), K(*other_expr_datum));
          } else if (OB_ISNULL(extra_buf = allocator.alloc(other_expr_datum->len_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else {
            MEMCPY(extra_buf, other_expr_datum->ptr_, other_expr_datum->len_);
            expr_datum->ptr_ = static_cast<char*>(extra_buf);
          }
        } else {
          expr_datum->ptr_ = frame_mem + (other_expr_datum->ptr_ - other.const_frame_ptrs_.at(i));
        }
      }  // end for
    }    // for end
    // init pre calc expr ptrs
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pre_calc_rt_exprs_.prepare_allocate(other.pre_calc_rt_exprs_.count()))) {
      LOG_WARN("failed to prepare allocate rt exprs", K(ret));
    } else {
      // allocate args memory for all rt exprs
      for (int i = 0; OB_SUCC(ret) && i < rt_exprs_.count(); i++) {
        if (rt_exprs_.at(i).arg_cnt_ > 0) {
          ObExpr** arg_buf = NULL;
          int64_t buf_size = rt_exprs_.at(i).arg_cnt_ * sizeof(ObExpr*);
          if (OB_ISNULL(arg_buf = (ObExpr**)allocator.alloc(buf_size))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else {
            MEMSET(arg_buf, 0, buf_size);
            rt_exprs_.at(i).args_ = arg_buf;
          }
        }

        // allocate parents memory for all rt exprs
        if (rt_exprs_.at(i).parent_cnt_ > 0) {
          ObExpr** parent_buf = NULL;
          int64_t buf_size = rt_exprs_.at(i).parent_cnt_ * sizeof(ObExpr*);
          if (OB_ISNULL(parent_buf = (ObExpr**)allocator.alloc(buf_size))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else {
            MEMSET(parent_buf, 0, buf_size);
            rt_exprs_.at(i).parents_ = parent_buf;
          }
        }
      }  // end for
    }
    hash::ObHashMap<int64_t, int64_t> expr_to_pos;
    bool rt_exprs_empty = (0 == other.rt_exprs_.count());
    if (OB_SUCC(ret) && !rt_exprs_empty &&
        OB_FAIL(expr_to_pos.create(other.rt_exprs_.count(), ObModIds::OB_SQL_EXPR_CALC))) {
      LOG_WARN("failed to init hashmap", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !rt_exprs_empty && i < other.rt_exprs_.count(); i++) {
      if (OB_FAIL(expr_to_pos.set_refactored(reinterpret_cast<int64_t>(&other.rt_exprs_.at(i)), i))) {
        LOG_WARN("failed to insert into map", K(ret));
      }
    }

    for (int i = 0; OB_SUCC(ret) && !rt_exprs_empty && i < other.pre_calc_rt_exprs_.count(); i++) {
      int64_t pos = -1;
      if (OB_FAIL(expr_to_pos.get_refactored(reinterpret_cast<int64_t>(other.pre_calc_rt_exprs_.at(i)), pos))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        pre_calc_rt_exprs_.at(i) = &rt_exprs_.at(pos);
      }
    }  // for end

    // replace arg ptrs and parent ptrs for all rt exprs
    for (int i = 0; OB_SUCC(ret) && i < other.rt_exprs_.count(); i++) {
      if (other.rt_exprs_.at(i).arg_cnt_ > 0) {
        for (int j = 0; j < other.rt_exprs_.at(i).arg_cnt_; j++) {
          int64_t pos = -1;
          if (OB_FAIL(expr_to_pos.get_refactored(reinterpret_cast<int64_t>(other.rt_exprs_.at(i).args_[j]), pos))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            }
          } else {
            rt_exprs_.at(i).args_[j] = &rt_exprs_.at(pos);
          }
        }  // end for
      }

      if (other.rt_exprs_.at(i).parent_cnt_ > 0) {
        for (int j = 0; j < other.rt_exprs_.at(i).parent_cnt_; j++) {
          int64_t pos = -1;
          if (OB_FAIL(expr_to_pos.get_refactored(reinterpret_cast<int64_t>(other.rt_exprs_.at(i).parents_[j]), pos))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            }
          } else {
            rt_exprs_.at(i).parents_[j] = &rt_exprs_.at(pos);
          }
        }  // end for
      }
    }  // end for
    if (OB_SUCC(ret)) {
      // deep copy extra info & inner function ptrs
      for (int i = 0; OB_SUCC(ret) && i < other.rt_exprs_.count(); i++) {
        if (ObExprExtraInfoFactory::is_registered(other.rt_exprs_.at(i).type_)) {
          if (OB_ISNULL(other.rt_exprs_.at(i).extra_info_)) {
            // do nothing
          } else if (OB_FAIL(other.rt_exprs_.at(i).extra_info_->deep_copy(
                         allocator, other.rt_exprs_.at(i).type_, rt_exprs_.at(i).extra_info_))) {
            LOG_WARN("failed to deep copy extra info", K(ret));
          }
        }

        // copy inner function ptrs
        if (other.rt_exprs_.at(i).inner_func_cnt_ > 0) {
          int64_t func_cnt = other.rt_exprs_.at(i).inner_func_cnt_;
          void* funcs_buf = NULL;
          if (OB_ISNULL(funcs_buf = allocator.alloc(sizeof(void*) * func_cnt))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else {
            MEMCPY(funcs_buf, other.rt_exprs_.at(i).inner_functions_, func_cnt * sizeof(void*));
            rt_exprs_.at(i).inner_functions_ = (void**)funcs_buf;
          }
        }
      }  // end for
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
