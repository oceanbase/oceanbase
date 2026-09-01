/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <type_traits>
#include <unistd.h>
#include <random>
#define private public
#define protected public

#include "share/vector/expr_cmp_func.ipp"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;


class TestExprSimdCmp : public ::testing::Test
{
public:
  TestExprSimdCmp() {}
  virtual void SetUp() {
    // Suppress grep output from CpuFlagSet::init_from_os() when detecting CPU flags
    fflush(stdout);
    int saved_stdout = dup(STDOUT_FILENO);
    if (saved_stdout >= 0) {
      freopen("/dev/null", "w", stdout);
      init_arches();
      fflush(stdout);
      dup2(saved_stdout, STDOUT_FILENO);
      close(saved_stdout);
    } else {
      init_arches();
    }
  }
  virtual void TearDown() {}
};

// 扩展 simd_supported 中支持的类型: VEC_TC_INTEGER, VEC_TC_UINTEGER, VEC_TC_FLOAT,
// VEC_TC_DOUBLE, VEC_TC_DATE, VEC_TC_DATETIME, VEC_TC_TIME, VEC_TC_BIT, VEC_TC_ENUM_SET,
// VEC_TC_DEC_INT32, VEC_TC_YEAR, VEC_TC_INTERVAL_YM, VEC_TC_DEC_INT64, VEC_TC_MYSQL_DATE,
// VEC_TC_MYSQL_DATETIME

// Frame layout for a single fixed-length batch expr (e.g. int64_t).
// Mirrors the logic in ObStaticEngineExprCG::arrange_datums_data (vector version).
// Layout (per expr, each in its own frame):
//   [datum_off_,     datum_off_+datums_size)          : ObDatum array
//   [pvt_skip_off_,  pvt_skip_off_+skip_size)         : pvt skip bitvector
//   [vector_header_off_, ...+sizeof(VectorHeader))    : VectorHeader (format + vector struct)
//   [null_bitmap_off_,  ...+bitmap_size)              : null bitmap bitvector
//   [eval_info_off_,    ...+sizeof(ObEvalInfo))        : ObEvalInfo
//   [eval_flags_off_,   ...+bitmap_size)              : eval flags bitvector
//   [res_buf_off_,      ...+val_size*batch_size)      : raw value data
struct FixedExprFrameLayout
{
  uint32_t datum_off;
  uint32_t pvt_skip_off;
  uint32_t vector_header_off;
  uint32_t null_bitmap_off;
  uint32_t eval_info_off;
  uint32_t eval_flags_off;
  uint32_t res_buf_off;
  int64_t  frame_size;

  FixedExprFrameLayout(int64_t batch_size, int64_t val_size)
  {
    int64_t pos   = 0;
    datum_off        = static_cast<uint32_t>(pos);
    pos             += sizeof(ObDatum) * batch_size;
    pvt_skip_off     = static_cast<uint32_t>(pos);
    pos             += ObBitVector::memory_size(batch_size);
    // fixed-length data: no lens/ptrs/cont_buf sections
    vector_header_off = static_cast<uint32_t>(pos);
    pos             += sizeof(VectorHeader);
    null_bitmap_off  = static_cast<uint32_t>(pos);
    pos             += ObBitVector::memory_size(batch_size);
    eval_info_off    = static_cast<uint32_t>(pos);
    pos             += sizeof(ObEvalInfo);
    eval_flags_off   = static_cast<uint32_t>(pos);
    pos             += ObBitVector::memory_size(batch_size);
    // no dyn_buf for integer type
    res_buf_off      = static_cast<uint32_t>(pos);
    pos             += val_size * batch_size;
    frame_size       = pos;
  }
};

// Type traits for simd_supported types (CType from RTCType<vec_tc>)
template <VecValueTypeClass vec_tc>
struct SimdCmpTypeTraits;

#define DEF_SIMD_CMP_TYPE_TRAITS(vec_tc, ob_type_setter) \
  template <> struct SimdCmpTypeTraits<vec_tc> { \
    using CType = RTCType<vec_tc>; \
    static constexpr size_t size = sizeof(RTCType<vec_tc>); \
    static void set_obj_meta(ObObjMeta &m) { m.set_##ob_type_setter(); } \
  }

DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_INTEGER, int);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_UINTEGER, uint64);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_FLOAT, float);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_DOUBLE, double);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_DATE, date);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_DATETIME, datetime);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_TIME, time);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_BIT, bit);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_ENUM_SET, enum);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_DEC_INT32, int32);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_YEAR, utinyint);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_INTERVAL_YM, interval_ym);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_DEC_INT64, int);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_MYSQL_DATE, mysql_date);
DEF_SIMD_CMP_TYPE_TRAITS(VEC_TC_MYSQL_DATETIME, mysql_datetime);

#undef DEF_SIMD_CMP_TYPE_TRAITS

// static void init_int64_expr(ObExpr &e, uint32_t frame_idx, const FixedExprFrameLayout &layout)
// {
//   new (&e) ObExpr();
//   e.frame_idx_          = frame_idx;
//   e.datum_off_          = layout.datum_off;
//   e.pvt_skip_off_       = layout.pvt_skip_off;
//   e.eval_info_off_      = layout.eval_info_off;
//   e.eval_flags_off_     = layout.eval_flags_off;
//   e.res_buf_off_        = layout.res_buf_off;
//   e.res_buf_len_        = static_cast<uint32_t>(sizeof(int64_t));
//   e.null_bitmap_off_    = layout.null_bitmap_off;
//   e.vector_header_off_  = layout.vector_header_off;
//   e.batch_result_       = 1;
//   e.is_fixed_length_data_ = 1;
//   e.is_called_in_sql_   = 1;
//   e.vec_value_tc_       = VEC_TC_INTEGER;
//   e.batch_idx_mask_     = UINT64_MAX;
//   e.len_                = static_cast<uint32_t>(sizeof(int64_t));
//   e.datum_meta_.type_   = ObIntType;
//   e.datum_meta_.cs_type_ = CS_TYPE_BINARY;
//   e.obj_meta_.set_int();
// }

template <VecValueTypeClass vec_tc>
static void init_expr_for_type(ObExpr &e, uint32_t frame_idx, const FixedExprFrameLayout &layout)
{
  using Traits = SimdCmpTypeTraits<vec_tc>;
  new (&e) ObExpr();
  e.frame_idx_          = frame_idx;
  e.datum_off_          = layout.datum_off;
  e.pvt_skip_off_       = layout.pvt_skip_off;
  e.eval_info_off_      = layout.eval_info_off;
  e.eval_flags_off_     = layout.eval_flags_off;
  e.res_buf_off_        = layout.res_buf_off;
  e.res_buf_len_        = static_cast<uint32_t>(Traits::size);
  e.null_bitmap_off_    = layout.null_bitmap_off;
  e.vector_header_off_  = layout.vector_header_off;
  e.batch_result_       = 1;
  e.is_fixed_length_data_ = 1;
  e.is_called_in_sql_   = 1;
  e.vec_value_tc_       = vec_tc;
  e.batch_idx_mask_     = UINT64_MAX;
  e.len_                = static_cast<uint32_t>(Traits::size);
  e.datum_meta_.type_   = ObIntType;  // result is always int64
  e.datum_meta_.cs_type_ = CS_TYPE_BINARY;
  Traits::set_obj_meta(e.obj_meta_);
}

// Placement-new an ObFixedLengthFormat<int64_t> into the VectorHeader's buffer,
// pointing it at the frame's data region and null-bitmap.
// static void init_fixed_int64_vector(char *frame, const FixedExprFrameLayout &layout,
//                                     int64_t batch_size)
// {
//   ObBitVector *nulls = reinterpret_cast<ObBitVector *>(frame + layout.null_bitmap_off);
//   nulls->reset(batch_size);   // clear all null flags (0 = not null)
//   ObBitVector *eval_flags = reinterpret_cast<ObBitVector *>(frame + layout.eval_flags_off);
//   eval_flags->reset(batch_size);
//   VectorHeader *vh  = reinterpret_cast<VectorHeader *>(frame + layout.vector_header_off);
//   vh->format_       = VEC_FIXED;
//   char *data        = frame + layout.res_buf_off;
//   new (vh->vector_buf_) ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>>(data, nulls);
// }

template <VecValueTypeClass vec_tc>
static void init_fixed_vector_for_type(char *frame, const FixedExprFrameLayout &layout,
                                      int64_t batch_size)
{
  using Traits = SimdCmpTypeTraits<vec_tc>;
  using CType = typename Traits::CType;
  ObBitVector *nulls = reinterpret_cast<ObBitVector *>(frame + layout.null_bitmap_off);
  nulls->reset(batch_size);
  ObBitVector *eval_flags = reinterpret_cast<ObBitVector *>(frame + layout.eval_flags_off);
  eval_flags->reset(batch_size);
  VectorHeader *vh  = reinterpret_cast<VectorHeader *>(frame + layout.vector_header_off);
  vh->format_       = VEC_FIXED;
  char *data        = frame + layout.res_buf_off;
  new (vh->vector_buf_) ObFixedLengthVector<CType, VectorBasicOp<vec_tc>>(data, nulls);
}

// template <ObCmpOp cmp_op>
// static int64_t scalar_int64_cmp(int64_t l, int64_t r)
// {
//   if (cmp_op == CO_LT) { return l <  r ? 1 : 0; }
//   if (cmp_op == CO_LE) { return l <= r ? 1 : 0; }
//   if (cmp_op == CO_GT) { return l >  r ? 1 : 0; }
//   if (cmp_op == CO_GE) { return l >= r ? 1 : 0; }
//   if (cmp_op == CO_EQ) { return l == r ? 1 : 0; }
//   if (cmp_op == CO_NE) { return l != r ? 1 : 0; }
//   return 0;
// }

template <typename T, ObCmpOp cmp_op>
static int64_t scalar_cmp(T l, T r)
{
  if (cmp_op == CO_LT) { return l <  r ? 1 : 0; }
  if (cmp_op == CO_LE) { return l <= r ? 1 : 0; }
  if (cmp_op == CO_GT) { return l >  r ? 1 : 0; }
  if (cmp_op == CO_GE) { return l >= r ? 1 : 0; }
  if (cmp_op == CO_EQ) { return l == r ? 1 : 0; }
  if (cmp_op == CO_NE) { return l != r ? 1 : 0; }
  return 0;
}

// Fill random int64_t values into the left and right input frames, inject one
// equal pair to exercise EQ/NE paths, then mark both frames as fully evaluated:
//   - ObEvalInfo::evaluated_ = 1 (scalar / single-row eval flag)
//   - eval_flags bitvector: all bits [0, batch_size) set to 1 (vector eval flag)
// static void fill_int64_input_data(char *left_frame, char *right_frame,
//                                   const FixedExprFrameLayout &layout,
//                                   int64_t batch_size)
// {
//   int64_t *left_data  = reinterpret_cast<int64_t *>(left_frame  + layout.res_buf_off);
//   int64_t *right_data = reinterpret_cast<int64_t *>(right_frame + layout.res_buf_off);

//   srand(42);
//   for (int64_t i = 0; i < batch_size; i++) {
//     left_data[i]  = static_cast<int64_t>(rand()) - RAND_MAX / 2;
//     right_data[i] = static_cast<int64_t>(rand()) - RAND_MAX / 2;
//   }
//   // Inject an equal pair to exercise EQ / NE paths
//   if (batch_size >= 4) {
//     left_data[batch_size / 4] = right_data[batch_size / 4];
//   }

//   // Mark input exprs as evaluated so the framework does not re-evaluate them
//   char *frames[2] = {left_frame, right_frame};
//   for (int f = 0; f < 2; f++) {
//     ObEvalInfo *eval_info =
//         reinterpret_cast<ObEvalInfo *>(frames[f] + layout.eval_info_off);
//     eval_info->set_evaluated(true);
//     ObBitVector *eval_flags =
//         reinterpret_cast<ObBitVector *>(frames[f] + layout.eval_flags_off);
//     eval_flags->set_all(static_cast<int64_t>(0), batch_size);
//   }
// }

template <VecValueTypeClass vec_tc>
static void fill_input_data_for_type(char *left_frame, char *right_frame,
                                     const FixedExprFrameLayout &layout,
                                     int64_t batch_size)
{
  using Traits = SimdCmpTypeTraits<vec_tc>;
  using CType = typename Traits::CType;
  CType *left_data  = reinterpret_cast<CType *>(left_frame  + layout.res_buf_off);
  CType *right_data = reinterpret_cast<CType *>(right_frame + layout.res_buf_off);

  srand(42);
  for (int64_t i = 0; i < batch_size; i++) {
    if constexpr (std::is_same_v<CType, int64_t>) {
      left_data[i]  = static_cast<int64_t>(rand()) - RAND_MAX / 2;
      right_data[i] = static_cast<int64_t>(rand()) - RAND_MAX / 2;
    } else if constexpr (std::is_same_v<CType, uint64_t>) {
      left_data[i]  = static_cast<uint64_t>(rand()) * (static_cast<uint64_t>(rand()) + 1);
      right_data[i] = static_cast<uint64_t>(rand()) * (static_cast<uint64_t>(rand()) + 1);
    } else if constexpr (std::is_same_v<CType, int32_t>) {
      left_data[i]  = static_cast<int32_t>(rand()) - RAND_MAX / 2;
      right_data[i] = static_cast<int32_t>(rand()) - RAND_MAX / 2;
    } else if constexpr (std::is_same_v<CType, uint8_t>) {
      left_data[i]  = static_cast<uint8_t>(rand() % 256);
      right_data[i] = static_cast<uint8_t>(rand() % 256);
    } else if constexpr (std::is_same_v<CType, float>) {
      left_data[i]  = (static_cast<float>(rand()) / RAND_MAX - 0.5f) * 1e6f;
      right_data[i] = (static_cast<float>(rand()) / RAND_MAX - 0.5f) * 1e6f;
    } else if constexpr (std::is_same_v<CType, double>) {
      left_data[i]  = (static_cast<double>(rand()) / RAND_MAX - 0.5) * 1e12;
      right_data[i] = (static_cast<double>(rand()) / RAND_MAX - 0.5) * 1e12;
    }
  }
  // if (batch_size >= 4) {
  //   left_data[batch_size / 4] = right_data[batch_size / 4];
  // }
  for (int i = 1; i < batch_size; i*= 4) {
    left_data[i] = right_data[i];
  }

  char *frames[2] = {left_frame, right_frame};
  for (int f = 0; f < 2; f++) {
    ObEvalInfo *eval_info =
        reinterpret_cast<ObEvalInfo *>(frames[f] + layout.eval_info_off);
    eval_info->set_evaluated(true);
    ObBitVector *eval_flags =
        reinterpret_cast<ObBitVector *>(frames[f] + layout.eval_flags_off);
    eval_flags->set_all(static_cast<int64_t>(0), batch_size);
  }
}

// Core test driver: allocates frames, wires up expressions, fills random
// int64_t values, calls NEON simd_eval_vector, then verifies every result
// against a scalar row-by-row comparison.
// template <ObCmpOp cmp_op>
// static void do_simd_int64_cmp_test(int64_t batch_size)
// {
//   const FixedExprFrameLayout layout(batch_size, static_cast<int64_t>(sizeof(int64_t)));

//   // Allocate zero-initialised frame memory for result (0), left (1), right (2)
//   char *fmem[3];
//   for (int i = 0; i < 3; i++) {
//     fmem[i] = new char[layout.frame_size]();
//   }

//   // Build ObExpr objects in stack storage (no constructor called for ObEvalCtx later)
//   alignas(ObExpr) char expr_storage[3][sizeof(ObExpr)];
//   ObExpr &res_expr   = *reinterpret_cast<ObExpr *>(expr_storage[0]);
//   ObExpr &left_expr  = *reinterpret_cast<ObExpr *>(expr_storage[1]);
//   ObExpr &right_expr = *reinterpret_cast<ObExpr *>(expr_storage[2]);

//   init_int64_expr(res_expr,   0, layout);
//   init_int64_expr(left_expr,  1, layout);
//   init_int64_expr(right_expr, 2, layout);

//   ObExpr *args[2] = {&left_expr, &right_expr};
//   res_expr.args_    = args;
//   res_expr.arg_cnt_ = 2;

//   // Initialise VEC_FIXED vectors inside each frame
//   for (int i = 0; i < 3; i++) {
//     init_fixed_int64_vector(fmem[i], layout, batch_size);
//   }

//   // Fill random values and mark input exprs as evaluated
//   fill_int64_input_data(fmem[1], fmem[2], layout, batch_size);

//   // Capture data pointers for result verification (must be done after fill)
//   const int64_t *left_data  = reinterpret_cast<const int64_t *>(fmem[1] + layout.res_buf_off);
//   const int64_t *right_data = reinterpret_cast<const int64_t *>(fmem[2] + layout.res_buf_off);

//   // Build a minimal ObEvalCtx by zero-initialising raw storage and only
//   // setting the two fields accessed by simd_eval_vector (frames_ and max_batch_size_).
//   alignas(ObEvalCtx) char ctx_storage[sizeof(ObEvalCtx)];
//   memset(ctx_storage, 0, sizeof(ObEvalCtx));
//   ObEvalCtx *ctx_ptr       = reinterpret_cast<ObEvalCtx *>(ctx_storage);
//   ctx_ptr->frames_         = fmem;
//   ctx_ptr->max_batch_size_ = batch_size;

//   // Skip bitvector: no rows skipped
//   int64_t skip_bytes = ObBitVector::memory_size(batch_size);
//   char *skip_buf = new char[skip_bytes]();
//   ObBitVector *skip = reinterpret_cast<ObBitVector *>(skip_buf);

//   EvalBound bound(static_cast<uint16_t>(batch_size), true /*all_rows_active*/);
//   int ret = OB_SUCCESS;
// // if x86 call specific::avx512::simd_eval_vector, if arm call specific::neon::simd_eval_vector
// // else if neon call specific::normal::simd_eval_vector
// #if OB_USE_MULTITARGET_CODE && defined(__x86_64__)
//   if (common::is_arch_supported(ObTargetArch::AVX512)) {
//     ret =
//       specific::avx512::simd_eval_vector<VEC_TC_INTEGER, static_cast<int>(sizeof(int64_t)), cmp_op,
//                                          false, false>(res_expr, *ctx_ptr, *skip, bound);
//   }
// #elif OB_ARM_USE_MULTITARGET_CODE
//   ret = specific::neon::simd_eval_vector<VEC_TC_INTEGER, static_cast<int>(sizeof(int64_t)), cmp_op,
//                                          false, false>(res_expr, *ctx_ptr, *skip, bound);
// #else
//   // do nothing
// #endif
//   ASSERT_EQ(OB_SUCCESS, ret);

//   // Verify each result against scalar comparison
//   const int64_t *result = reinterpret_cast<const int64_t *>(fmem[0] + layout.res_buf_off);
//   for (int64_t i = 0; i < batch_size; i++) {
//     int64_t expected = scalar_int64_cmp<cmp_op>(left_data[i], right_data[i]);
//     EXPECT_EQ(expected, result[i])
//         << "cmp_op=" << static_cast<int>(cmp_op)
//         << " row="   << i
//         << " left="  << left_data[i]
//         << " right=" << right_data[i];
//   }

//   delete[] skip_buf;
//   for (int i = 0; i < 3; i++) { delete[] fmem[i]; }
// }

// Generic SIMD cmp test for any simd_supported type
template <VecValueTypeClass vec_tc, ObCmpOp cmp_op>
static void do_simd_cmp_test(int64_t batch_size)
{
  using Traits = SimdCmpTypeTraits<vec_tc>;
  using CType = typename Traits::CType;
  constexpr int val_size = static_cast<int>(Traits::size);
  constexpr int64_t frame_val_size = (Traits::size > sizeof(int64_t)) ? Traits::size : sizeof(int64_t);
  const FixedExprFrameLayout layout(batch_size, frame_val_size);

  char *fmem[3];
  for (int i = 0; i < 3; i++) {
    fmem[i] = new char[layout.frame_size]();
  }

  alignas(ObExpr) char expr_storage[3][sizeof(ObExpr)];
  ObExpr &res_expr   = *reinterpret_cast<ObExpr *>(expr_storage[0]);
  ObExpr &left_expr  = *reinterpret_cast<ObExpr *>(expr_storage[1]);
  ObExpr &right_expr = *reinterpret_cast<ObExpr *>(expr_storage[2]);

  init_expr_for_type<VEC_TC_INTEGER>(res_expr, 0, layout);  // result is always int64
  init_expr_for_type<vec_tc>(left_expr,  1, layout);
  init_expr_for_type<vec_tc>(right_expr, 2, layout);

  ObExpr *args[2] = {&left_expr, &right_expr};
  res_expr.args_    = args;
  res_expr.arg_cnt_ = 2;

  for (int i = 0; i < 3; i++) {
    init_fixed_vector_for_type<vec_tc>(fmem[i], layout, batch_size);
  }

  fill_input_data_for_type<vec_tc>(fmem[1], fmem[2], layout, batch_size);

  const CType *left_data  = reinterpret_cast<const CType *>(fmem[1] + layout.res_buf_off);
  const CType *right_data = reinterpret_cast<const CType *>(fmem[2] + layout.res_buf_off);

  alignas(ObEvalCtx) char ctx_storage[sizeof(ObEvalCtx)];
  memset(ctx_storage, 0, sizeof(ObEvalCtx));
  ObEvalCtx *ctx_ptr       = reinterpret_cast<ObEvalCtx *>(ctx_storage);
  ctx_ptr->frames_         = fmem;
  ctx_ptr->max_batch_size_ = batch_size;

  int64_t skip_bytes = ObBitVector::memory_size(batch_size);
  char *skip_buf = new char[skip_bytes]();
  ObBitVector *skip = reinterpret_cast<ObBitVector *>(skip_buf);

  EvalBound bound(static_cast<uint16_t>(batch_size), true);
  int ret = OB_SUCCESS;
  bool calculated = false;
#if OB_USE_MULTITARGET_CODE && defined(__x86_64__)
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    ret = specific::avx512::simd_eval_vector<vec_tc, val_size, cmp_op, false, false>(
        res_expr, *ctx_ptr, *skip, bound);
    calculated = true;
  }
#elif OB_ARM_USE_MULTITARGET_CODE
  std::cout << "NEON calculated" << std::endl;
  ret = specific::neon::simd_eval_vector<vec_tc, val_size, cmp_op, false, false>(
      res_expr, *ctx_ptr, *skip, bound);
  calculated = true;
#endif
  ASSERT_EQ(OB_SUCCESS, ret);

  std::cout << "calculated: " << calculated << std::endl;

  const int64_t *result = reinterpret_cast<const int64_t *>(fmem[0] + layout.res_buf_off);
  for (int64_t i = 0; i < batch_size && calculated; i++) {
    int64_t expected = scalar_cmp<CType, cmp_op>(left_data[i], right_data[i]);
    EXPECT_EQ(expected, result[i])
        << "vec_tc=" << static_cast<int>(vec_tc)
        << " cmp_op=" << static_cast<int>(cmp_op)
        << " row=" << i
        << " left=" << +left_data[i]
        << " right=" << +right_data[i];
  }

  delete[] skip_buf;
  for (int i = 0; i < 3; i++) { delete[] fmem[i]; }
}

static int random_batch(int min, int max)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(min, max);
  return dis(gen);
}

// All 6 comparison operators, batch_size = 32 (exact multiple of vec_batch=8 → pure SIMD path)
// TEST_F(TestExprSimdCmp, simd_eval_vector_int64_full_simd_batch)
// {
//   do_simd_int64_cmp_test<CO_LT>(32);
//   do_simd_int64_cmp_test<CO_LE>(32);
//   do_simd_int64_cmp_test<CO_GT>(32);
//   do_simd_int64_cmp_test<CO_GE>(32);
//   do_simd_int64_cmp_test<CO_EQ>(32);
//   do_simd_int64_cmp_test<CO_NE>(32);

//   do_simd_int64_cmp_test<CO_LT>(256);
//   do_simd_int64_cmp_test<CO_LE>(256);
//   do_simd_int64_cmp_test<CO_GT>(256);
//   do_simd_int64_cmp_test<CO_GE>(256);
//   do_simd_int64_cmp_test<CO_EQ>(256);
//   do_simd_int64_cmp_test<CO_NE>(256);

//   do_simd_int64_cmp_test<CO_LT>(1024);
//   do_simd_int64_cmp_test<CO_LE>(1024);
//   do_simd_int64_cmp_test<CO_GT>(1024);
//   do_simd_int64_cmp_test<CO_GE>(1024);
//   do_simd_int64_cmp_test<CO_EQ>(1024);
//   do_simd_int64_cmp_test<CO_NE>(1024);
// }

// batch_size not a multiple of 8: exercises the scalar fallback for the tail rows
// TEST_F(TestExprSimdCmp, simd_eval_vector_int64_with_scalar_tail)
// {
//   do_simd_int64_cmp_test<CO_LT>(33);   // 32 SIMD + 1 scalar
//   do_simd_int64_cmp_test<CO_GT>(35);   // 32 SIMD + 3 scalar
//   do_simd_int64_cmp_test<CO_EQ>(17);   // 16 SIMD + 1 scalar
//   do_simd_int64_cmp_test<CO_NE>(25);   // 24 SIMD + 1 scalar

//   do_simd_int64_cmp_test<CO_LT>(33);
//   do_simd_int64_cmp_test<CO_LE>(150);
//   do_simd_int64_cmp_test<CO_GT>(255);
//   do_simd_int64_cmp_test<CO_GE>(254);
//   do_simd_int64_cmp_test<CO_EQ>(2);
//   do_simd_int64_cmp_test<CO_NE>(99);
// }

// int64_t (VEC_TC_INTEGER)
TEST_F(TestExprSimdCmp, simd_eval_vector_int64)
{
  do_simd_cmp_test<VEC_TC_INTEGER, CO_LT>(32);
  do_simd_cmp_test<VEC_TC_INTEGER, CO_LE>(32);
  do_simd_cmp_test<VEC_TC_INTEGER, CO_GT>(32);
  do_simd_cmp_test<VEC_TC_INTEGER, CO_GE>(32);
  do_simd_cmp_test<VEC_TC_INTEGER, CO_EQ>(32);
  do_simd_cmp_test<VEC_TC_INTEGER, CO_NE>(32);

  do_simd_cmp_test<VEC_TC_INTEGER, CO_LT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_INTEGER, CO_LE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_INTEGER, CO_GT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_INTEGER, CO_GE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_INTEGER, CO_EQ>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_INTEGER, CO_NE>(random_batch(128, 256));
}
// uint64_t (VEC_TC_UINTEGER)
TEST_F(TestExprSimdCmp, simd_eval_vector_uint64)
{
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_LT>(32);
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_LE>(32);
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_GT>(32);
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_GE>(32);
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_EQ>(32);
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_NE>(32);
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_EQ>(33);

  do_simd_cmp_test<VEC_TC_UINTEGER, CO_LT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_LE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_GT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_GE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_EQ>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_UINTEGER, CO_NE>(random_batch(128, 256));
}

// float (VEC_TC_FLOAT)
TEST_F(TestExprSimdCmp, simd_eval_vector_float)
{
  do_simd_cmp_test<VEC_TC_FLOAT, CO_LT>(32);
  do_simd_cmp_test<VEC_TC_FLOAT, CO_LE>(32);
  do_simd_cmp_test<VEC_TC_FLOAT, CO_GT>(32);
  do_simd_cmp_test<VEC_TC_FLOAT, CO_GE>(32);
  do_simd_cmp_test<VEC_TC_FLOAT, CO_EQ>(32);
  do_simd_cmp_test<VEC_TC_FLOAT, CO_NE>(32);

  do_simd_cmp_test<VEC_TC_FLOAT, CO_LT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_FLOAT, CO_LE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_FLOAT, CO_GT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_FLOAT, CO_GE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_FLOAT, CO_EQ>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_FLOAT, CO_NE>(random_batch(128, 256));
}

// double (VEC_TC_DOUBLE)
TEST_F(TestExprSimdCmp, simd_eval_vector_double)
{
  do_simd_cmp_test<VEC_TC_DOUBLE, CO_LT>(32);
  do_simd_cmp_test<VEC_TC_DOUBLE, CO_LE>(32);
  do_simd_cmp_test<VEC_TC_DOUBLE, CO_GT>(32);
  do_simd_cmp_test<VEC_TC_DOUBLE, CO_GE>(32);
  do_simd_cmp_test<VEC_TC_DOUBLE, CO_EQ>(32);
  do_simd_cmp_test<VEC_TC_DOUBLE, CO_NE>(32);
  do_simd_cmp_test<VEC_TC_DOUBLE, CO_EQ>(25);
}

// int32_t (VEC_TC_DATE)
TEST_F(TestExprSimdCmp, simd_eval_vector_date)
{
  do_simd_cmp_test<VEC_TC_DATE, CO_LT>(32);
  do_simd_cmp_test<VEC_TC_DATE, CO_LE>(32);
  do_simd_cmp_test<VEC_TC_DATE, CO_GT>(32);
  do_simd_cmp_test<VEC_TC_DATE, CO_GE>(32);
  do_simd_cmp_test<VEC_TC_DATE, CO_EQ>(32);
  do_simd_cmp_test<VEC_TC_DATE, CO_NE>(32);

  do_simd_cmp_test<VEC_TC_DATE, CO_LT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DATE, CO_LE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DATE, CO_GT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DATE, CO_GE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DATE, CO_EQ>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DATE, CO_NE>(random_batch(128, 256));
}

// uint8_t (VEC_TC_YEAR)
TEST_F(TestExprSimdCmp, simd_eval_vector_year)
{
  do_simd_cmp_test<VEC_TC_YEAR, CO_LT>(128);
  do_simd_cmp_test<VEC_TC_YEAR, CO_LE>(128);
  do_simd_cmp_test<VEC_TC_YEAR, CO_GT>(128);
  do_simd_cmp_test<VEC_TC_YEAR, CO_GE>(128);
  do_simd_cmp_test<VEC_TC_YEAR, CO_EQ>(128);
  do_simd_cmp_test<VEC_TC_YEAR, CO_NE>(128);

  do_simd_cmp_test<VEC_TC_YEAR, CO_LT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_YEAR, CO_LE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_YEAR, CO_GT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_YEAR, CO_GE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_YEAR, CO_EQ>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_YEAR, CO_NE>(random_batch(128, 256));
}


TEST_F(TestExprSimdCmp, simd_eval_vector_dec_int32)
{
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_LT>(32);
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_LE>(32);
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_GT>(32);
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_GE>(32);
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_EQ>(32);
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_NE>(32);

  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_LT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_LE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_GT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_GE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_EQ>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_DEC_INT32, CO_NE>(random_batch(128, 256));
}

// uint64_t (VEC_TC_BIT)
TEST_F(TestExprSimdCmp, simd_eval_vector_bit)
{
  do_simd_cmp_test<VEC_TC_BIT, CO_LT>(32);
  do_simd_cmp_test<VEC_TC_BIT, CO_LE>(32);
  do_simd_cmp_test<VEC_TC_BIT, CO_GT>(32);
  do_simd_cmp_test<VEC_TC_BIT, CO_GE>(32);
  do_simd_cmp_test<VEC_TC_BIT, CO_EQ>(32);
  do_simd_cmp_test<VEC_TC_BIT, CO_NE>(32);

  do_simd_cmp_test<VEC_TC_BIT, CO_LT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_BIT, CO_LE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_BIT, CO_GT>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_BIT, CO_GE>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_BIT, CO_EQ>(random_batch(128, 256));
  do_simd_cmp_test<VEC_TC_BIT, CO_NE>(random_batch(128, 256));
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_expr_simd_cmp.log", true);
  return RUN_ALL_TESTS();
}