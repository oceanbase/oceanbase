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
 * This file is for implement of func multimode expr helper
 */

#ifndef OCEANBASE_SQL_OB_EXPR_MULTI_MODE_FUNC_HELPER_H_
#define OCEANBASE_SQL_OB_EXPR_MULTI_MODE_FUNC_HELPER_H_

#include "sql/session/ob_sql_session_info.h"
#include "lib/allocator/page_arena.h"
#include "objit/common/ob_item_type.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

#define ObMultiModeDefaultMagnification 3
#define ObMultiModeSingleMagnification 1
#define ObMultiModeDoubleMagnification 2
#define ObMultiModeTripleMagnification 3
#define ObMultiModeFourMagnification 4
#define ObMultiModeFiveMagnification 5
#define ObMultiModeSixMagnification 6

static const int32_t FIRST_MEM_THRESHOLD = 0x1;
static const int32_t SECOND_MEM_THRESHOLD = 0x2;
static const int32_t THIRD_MEM_THRESHOLD = 0x4;
static const int32_t MEM_EXCEED_THRESHOLD = 0x8;

enum ObMultiModeType
{
  T_PL_FUNC_XML_TRANSFORM = 0,
  T_PL_FUNC_XML_CREATE_XML = 1,
  T_PL_FUNC_XML_CONSTRUCTOR = 2,
  T_PL_FUNC_XML_GETVAL = 3,
  T_PL_FUNC_XML_MAX = 100, // reserve [0, 99] for xml func
  T_PL_FUNC_GIS_GET_WKB = 101,
  T_PL_FUNC_GIS_GET_WKT = 102,
  T_PL_FUNC_GIS_ST_ISVALID = 103,
  T_PL_FUNC_GIS_GET_GEOJSON = 104,
  T_PL_FUNC_GIS_CONTRUCTOR = 105,
  T_PL_FUNC_GIS_CAST = 106,
  T_PL_FUNC_GIS_GET_VERTICES = 107,
  T_PL_FUNC_GIS_MAX = 200,
};

class ObMultiModeExprHelper final
{
public:
  static uint64_t get_tenant_id(ObSQLSessionInfo *session); // json and xml get tenant_id public function
};



class MultimodeAlloctor : public ObIAllocator
{
public:
  MultimodeAlloctor(ObArenaAllocator &arena, uint64_t type, int &ret);
  MultimodeAlloctor(ObArenaAllocator &arena, uint64_t type, int64_t tenant_id, int &ret, const char *func_name = "");
  MultimodeAlloctor(ObArenaAllocator &arena, uint64_t type, int64_t tenant_id, int &ret, 
                    int32_t cached_trace_level, const char *func_name = "");
  ~MultimodeAlloctor();

public:
  virtual void *alloc(const int64_t sz);
  void *alloc(const int64_t size, const ObMemAttr &attr);
  virtual void *realloc(void *ptr, const int64_t oldsz, const int64_t newsz) { return arena_.realloc(ptr, oldsz, newsz); }
  virtual void free(void *ptr) { arena_.free(ptr); }
  int64_t used() const { return arena_.used(); }
  int64_t total() const { return arena_.total(); }
  void reset() { arena_.reset(); }
  void reuse() override { arena_.reuse(); }
  void set_attr(const ObMemAttr &attr) override
  {
    arena_.set_attr(attr);
  }

  void mem_check(float multiple);
  void set_baseline_size(uint64_t baseline_size);
  void add_baseline_size(uint64_t add_size);
  void set_baseline_size_and_flag(uint64_t baseline_size);
  void set_children_used(uint64_t children_used) { children_used_ = children_used; }
  int add_baseline_size(const ObExpr *expr, ObEvalCtx &ctx, uint32_t multiple = 1);
  int add_baseline_size(ObDatum *datum, bool has_lob_header, uint32_t multiple = 1);
  bool is_open_trace() { return check_level_ > 0; }
  int64_t cur_func_used() const { return used() - children_used_ + ext_used_; }
  int eval_arg(const ObExpr *arg, ObEvalCtx &ctx, common::ObDatum *&datum);
  void add_ext_used(uint64_t add);
  void memory_usage_check_if_need();
  TO_STRING_KV(K_(ret), K_(baseline_size), K_(type), K_(func_name), 
      K_(mem_threshold_flag), K_(check_level), K(cur_func_used()), K(ext_used_));
private:
  static uint64_t get_expected_multiple(uint64_t type);
  bool has_reached_threshold() { return static_cast<bool>(MEM_EXCEED_THRESHOLD & mem_threshold_flag_); }
  bool has_first_mem_threshold() { return static_cast<bool>(FIRST_MEM_THRESHOLD & mem_threshold_flag_); }
  bool has_second_mem_threshold() { return static_cast<bool>(SECOND_MEM_THRESHOLD & mem_threshold_flag_); }
  bool has_third_mem_threshold() { return static_cast<bool>(THIRD_MEM_THRESHOLD & mem_threshold_flag_); }
  bool has_level_mem_threshold(uint8_t level) { return static_cast<bool>((1 << level) & mem_threshold_flag_); }
  bool has_reached_specified_threshold(double threshold);
  void set_reached_threshold() { mem_threshold_flag_ |= MEM_EXCEED_THRESHOLD; }
  void set_first_mem_threshold() { mem_threshold_flag_ |= FIRST_MEM_THRESHOLD; }
  void set_second_mem_threshold() { mem_threshold_flag_ |= SECOND_MEM_THRESHOLD; }
  void set_third_mem_threshold() { mem_threshold_flag_ |= THIRD_MEM_THRESHOLD; }
  void set_level_mem_threshold(uint8_t level) { mem_threshold_flag_ |= (1 << level); }
  void memory_usage_check();
  static constexpr int OB_MEM_TRACK_MIN_FOOT = 10 << 10; //ObMemoryTrackingMinimumFootprint
  static const int32_t LEVEL_NUMBERS = 99;
  static const int32_t LEVEL_CLASS = 3;
  static const double MEM_THRESHOLD_LEVEL[LEVEL_NUMBERS][LEVEL_CLASS];
private:
  ObIAllocator &arena_;
  uint64_t baseline_size_;
  uint64_t type_;
  int32_t mem_threshold_flag_;
  int32_t check_level_;
  const char* func_name_;
  uint64_t children_used_;
  uint64_t expect_threshold_;
  int &ret_;
  uint64_t ext_used_;
};


};
};

#endif // OCEANBASE_SQL_OB_EXPR_MULTI_MODE_FUNC_HELPER_H_