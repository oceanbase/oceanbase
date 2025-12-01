/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIB_OB_VECTOR_IP_DISTANCE_H_
#define OCEANBASE_LIB_OB_VECTOR_IP_DISTANCE_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/object/ob_obj_compare.h"
#include "ob_vector_op_common.h"

namespace oceanbase
{
namespace common
{
template <typename T>
struct ObVectorIpDistance
{
  static int ip_distance_func(const T *a, const T *b, const int64_t len, double &distance);
  static void fvec_inner_products_ny(T* ip, const T* x, const T* y, size_t d, size_t ny);
};

template<>
int ObVectorIpDistance<float>::ip_distance_func(const float *a, const float *b, const int64_t len, double &similarity);
template <>
int ObVectorIpDistance<uint8_t>::ip_distance_func(const uint8_t *a, const uint8_t *b, const int64_t len, double &similarity);
template <>
void ObVectorIpDistance<float>::fvec_inner_products_ny(float* ip, const float* x, const float* y, size_t d, size_t ny);

OB_INLINE int ip_distance_normal(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    distance += a[i] * b[i];
  }
  return ret;
}

#if defined(__SSE2__) || defined(__AVX__) || defined(__AVX2__) || defined(__AVX512F__) || defined(__AVX512BW__)
// SSE
OB_INLINE static int ip_distance_simd4_avx128(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 128 / (sizeof(float) * 8); // 4
  int64_t dim = len >> 2 << 2;
  double tmp_distance = 0;

  __m128 vsum = _mm_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm_add_ps(vsum, _mm_mul_ps(_mm_loadu_ps(a + i), _mm_loadu_ps(b + i)));
  }

  float sum[4] CACHE_ALIGNED;
  _mm_store_ps(sum, vsum);

  tmp_distance = sum[0] + sum[1] + sum[2] + sum[3];
  distance += tmp_distance;

  return ret;
}

OB_INLINE static int ip_distance_simd4_avx128_extra(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 2 << 2;
  double extra_distance = 0;
  if (OB_FAIL(ip_distance_simd4_avx128(a, b, dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  } else if (0 < len - dim && OB_FAIL(ip_distance_normal(a + dim, b + dim, len - dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  }
  return ret;
}

OB_INLINE static int ip_distance_avx128(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (4 < len) {
    if (OB_FAIL(ip_distance_simd4_avx128_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(distance));
    }
  } else if (OB_FAIL(ip_distance_normal(a, b, len, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret));
  }
  return ret;
}
#endif

#if defined(__AVX__) || defined(__AVX2__) || defined(__AVX512F__) || defined(__AVX512BW__)
// AVX2
OB_INLINE static int ip_distance_simd8_avx256(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 256 / (sizeof(float) * 8); // 8
  int64_t dim = len >> 3 << 3;
  double tmp_distance = 0;

  __m256 vsum = _mm256_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm256_add_ps(vsum, _mm256_mul_ps(_mm256_loadu_ps(a + i), _mm256_loadu_ps(b + i)));
  }
  float sum[8] CACHE_ALIGNED;
  _mm256_store_ps(sum, vsum);

  tmp_distance = sum[0] + sum[1] + sum[2] + sum[3]
      + sum[4] + sum[5] + sum[6] + sum[7];
  distance += tmp_distance;

  return ret;
}

OB_INLINE static int ip_distance_simd8_avx256_extra(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 3 << 3;
  if (OB_FAIL(ip_distance_simd8_avx256(a, b, dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  } else if (0 < len - dim && OB_FAIL(ip_distance_simd4_avx128_extra(a + dim, b + dim, len - dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  }
  return ret;
}

OB_INLINE static int ip_distance_avx256(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (8 < len) {
    if (OB_FAIL(ip_distance_simd8_avx256_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret));
    }
  } else if (4 < len) {
    if (OB_FAIL(ip_distance_simd4_avx128_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(distance));
    }
  } else if (OB_FAIL(ip_distance_normal(a, b, len, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret));
  }
  return ret;
}
#endif

#if defined(__AVX512F__) || defined(__AVX512BW__)
// AVX512
OB_INLINE static int ip_distance_simd16_avx512(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  const int64_t batch = 512 / (sizeof(float) * 8); // 16
  int64_t dim = len >> 4 << 4;
  double tmp_distance = 0;

  __m512 vsum = _mm512_set1_ps(0);
  for (int64_t i = 0; i < dim; i += batch) {
    vsum = _mm512_add_ps(vsum, _mm512_mul_ps(_mm512_loadu_ps(a + i), _mm512_loadu_ps(b + i)));
  }
  float sum[16] CACHE_ALIGNED;
  _mm512_store_ps(sum, vsum);

  tmp_distance = sum[0] + sum[1] + sum[2] + sum[3] + sum[4] + sum[5] + sum[6] + sum[7]
      + sum[8] + sum[9] + sum[10] + sum[11] + sum[12] + sum[13] + sum[14] + sum[15];
  distance += tmp_distance;

  return ret;
}

OB_INLINE static int ip_distance_simd16_avx512_extra(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  int64_t dim = len >> 4 << 4;
  if (OB_FAIL(ip_distance_simd16_avx512(a, b, dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  } else if (0 < len - dim && OB_FAIL(ip_distance_simd8_avx256_extra(a + dim, b + dim, len - dim, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(len), K(dim), K(distance));
  }
  return ret;
}

OB_INLINE static int ip_distance_avx512(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (16 < len) {
    if (OB_FAIL(ip_distance_simd16_avx512_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret));
    }
  } else if (8 < len) {
    if (OB_FAIL(ip_distance_simd8_avx256_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret));
    }
  } else if (4 < len) {
    if (OB_FAIL(ip_distance_simd4_avx128_extra(a, b, len, distance))) {
      LIB_LOG(WARN, "failed to cal ip distance", K(ret), K(distance));
    }
  } else if (OB_FAIL(ip_distance_normal(a, b, len, distance))) {
    LIB_LOG(WARN, "failed to cal ip distance", K(ret));
  }
  return ret;
}
#endif

OB_DECLARE_DEFAULT_CODE (
inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  #if defined(__SSE2__)
  ret = ip_distance_avx128(a, b, len, distance);
  #else
  ret = ip_distance_normal(a, b, len, distance);
  #endif
  return ret;
}
)

OB_DECLARE_AVX2_SPECIFIC_CODE (
inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  #if defined(__AVX__) || defined(__AVX2__)
  ret = ip_distance_avx256(a, b, len, distance);
  #elif defined(__SSE2__)
  ret = ip_distance_avx128(a, b, len, distance);
  #else
  ret = ip_distance_normal(a, b, len, distance);
  #endif
  return ret;
}
)

OB_DECLARE_AVX512_SPECIFIC_CODE (
inline static int ip_distance(const float *a, const float *b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  #if defined(__AVX512F__) || defined(__AVX512BW__)
  ret = ip_distance_avx512(a, b, len, distance);
  #elif defined(__AVX__) || defined(__AVX2__)
  ret = ip_distance_avx256(a, b, len, distance);
  #elif defined(__SSE2__)
  ret = ip_distance_avx128(a, b, len, distance);
  #else
  ret = ip_distance_normal(a, b, len, distance);
  #endif
  return ret;
}
)

// fvec_inner_products_ny
OB_DECLARE_DEFAULT_CODE(
  inline static float fvec_inner_product_ref(const float* x, const float* y, size_t d) {
      size_t i;
      float res = 0;
      for (i = 0; i < d; i++) {
          res += x[i] * y[i];
      }
      return res;
  }
  inline static void fvec_inner_products_ny(float* ip, const float* x, const float* y, size_t d, size_t ny) {
      for (size_t i = 0; i < ny; i++) {
          ip[i] = fvec_inner_product_ref(x, y, d);
          y += d;
      }
  }
)

OB_DECLARE_SSE42_SPECIFIC_CODE(
  struct ElementOpIP {
    static float
    op(float x, float y) {
        return x * y;
    }

    static __m128
    op(__m128 x, __m128 y) {
        return _mm_mul_ps(x, y);
    }
  };

  template <class ElementOp>
  inline static void fvec_op_ny_D1(float* dis, const float* x, const float* y, size_t ny) {
      float x0s = x[0];
      __m128 x0 = _mm_set_ps(x0s, x0s, x0s, x0s);

      size_t i;
      for (i = 0; i + 3 < ny; i += 4) {
          __m128 accu = ElementOp::op(x0, _mm_loadu_ps(y));
          y += 4;
          dis[i] = _mm_cvtss_f32(accu);
          __m128 tmp = _mm_shuffle_ps(accu, accu, 1);
          dis[i + 1] = _mm_cvtss_f32(tmp);
          tmp = _mm_shuffle_ps(accu, accu, 2);
          dis[i + 2] = _mm_cvtss_f32(tmp);
          tmp = _mm_shuffle_ps(accu, accu, 3);
          dis[i + 3] = _mm_cvtss_f32(tmp);
      }
      while (i < ny) {  // handle non-multiple-of-4 case
          dis[i++] = ElementOp::op(x0s, *y++);
      }
  }

  template <class ElementOp>
  inline static void fvec_op_ny_D2(float* dis, const float* x, const float* y, size_t ny) {
      __m128 x0 = _mm_set_ps(x[1], x[0], x[1], x[0]);

      size_t i;
      for (i = 0; i + 1 < ny; i += 2) {
          __m128 accu = ElementOp::op(x0, _mm_loadu_ps(y));
          y += 4;
          accu = _mm_hadd_ps(accu, accu);
          dis[i] = _mm_cvtss_f32(accu);
          accu = _mm_shuffle_ps(accu, accu, 3);
          dis[i + 1] = _mm_cvtss_f32(accu);
      }
      if (i < ny) {  // handle odd case
          dis[i] = ElementOp::op(x[0], y[0]) + ElementOp::op(x[1], y[1]);
      }
  }

  template <class ElementOp>
  inline static void fvec_op_ny_D4(float* dis, const float* x, const float* y, size_t ny) {
      __m128 x0 = _mm_loadu_ps(x);

      for (size_t i = 0; i < ny; i++) {
          __m128 accu = ElementOp::op(x0, _mm_loadu_ps(y));
          y += 4;
          accu = _mm_hadd_ps(accu, accu);
          accu = _mm_hadd_ps(accu, accu);
          dis[i] = _mm_cvtss_f32(accu);
      }
  }

  template <class ElementOp>
  inline static void fvec_op_ny_D8(float* dis, const float* x, const float* y, size_t ny) {
      __m128 x0 = _mm_loadu_ps(x);
      __m128 x1 = _mm_loadu_ps(x + 4);

      for (size_t i = 0; i < ny; i++) {
          __m128 accu = ElementOp::op(x0, _mm_loadu_ps(y));
          y += 4;
          accu = _mm_add_ps(accu, ElementOp::op(x1, _mm_loadu_ps(y)));
          y += 4;
          accu = _mm_hadd_ps(accu, accu);
          accu = _mm_hadd_ps(accu, accu);
          dis[i] = _mm_cvtss_f32(accu);
      }
  }

  template <class ElementOp>
  inline static void fvec_op_ny_D12(float* dis, const float* x, const float* y, size_t ny) {
      __m128 x0 = _mm_loadu_ps(x);
      __m128 x1 = _mm_loadu_ps(x + 4);
      __m128 x2 = _mm_loadu_ps(x + 8);

      for (size_t i = 0; i < ny; i++) {
          __m128 accu = ElementOp::op(x0, _mm_loadu_ps(y));
          y += 4;
          accu = _mm_add_ps(accu, ElementOp::op(x1, _mm_loadu_ps(y)));
          y += 4;
          accu = _mm_add_ps(accu, ElementOp::op(x2, _mm_loadu_ps(y)));
          y += 4;
          accu = _mm_hadd_ps(accu, accu);
          accu = _mm_hadd_ps(accu, accu);
          dis[i] = _mm_cvtss_f32(accu);
      }
  }

  inline static float fvec_inner_product_sse(const float* x, const float* y, size_t d) {
      __m128 mx, my;
      __m128 msum1 = _mm_setzero_ps();

      while (d >= 4) {
          mx = _mm_loadu_ps(x);
          x += 4;
          my = _mm_loadu_ps(y);
          y += 4;
          msum1 = _mm_add_ps(msum1, _mm_mul_ps(mx, my));
          d -= 4;
      }

      // add the last 1, 2, or 3 values
      mx = masked_read(d, x);
      my = masked_read(d, y);
      __m128 prod = _mm_mul_ps(mx, my);

      msum1 = _mm_add_ps(msum1, prod);

      msum1 = _mm_hadd_ps(msum1, msum1);
      msum1 = _mm_hadd_ps(msum1, msum1);
      return _mm_cvtss_f32(msum1);
  }

  inline static void fvec_inner_products_ny(float* dis, const float* x, const float* y, size_t d, size_t ny) {
    switch (d) {
      case 1:
        fvec_op_ny_D1<ElementOpIP>(dis, x, y, ny);
        return;
      case 2:
        fvec_op_ny_D2<ElementOpIP>(dis, x, y, ny);
        return;
      case 4:
        fvec_op_ny_D4<ElementOpIP>(dis, x, y, ny);
        return;
      case 8:
        fvec_op_ny_D8<ElementOpIP>(dis, x, y, ny);
        return;
      case 12:
        fvec_op_ny_D12<ElementOpIP>(dis, x, y, ny);
        return;
      default:
        for (size_t i = 0; i < ny; i++) {
            dis[i] = fvec_inner_product_sse(x, y, d);
            y += d;
        }
        return;
    }
  }
)

#if defined(__aarch64__)
inline static int ip_distance_neon(const float *x, const float *y, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  float32x4_t sum = vdupq_n_f32(0.0f);
  auto dim = len;
  auto d = len;
  while (d >= 16) {
    float32x4x4_t a = vld1q_f32_x4(x + dim - d);
    float32x4x4_t b = vld1q_f32_x4(y + dim - d);
    float32x4x4_t c;
    c.val[0] = vmulq_f32(a.val[0], b.val[0]);
    c.val[1] = vmulq_f32(a.val[1], b.val[1]);
    c.val[2] = vmulq_f32(a.val[2], b.val[2]);
    c.val[3] = vmulq_f32(a.val[3], b.val[3]);

    c.val[0] = vaddq_f32(c.val[0], c.val[1]);
    c.val[2] = vaddq_f32(c.val[2], c.val[3]);
    c.val[0] = vaddq_f32(c.val[0], c.val[2]);

    sum = vaddq_f32(sum, c.val[0]);
    d -= 16;
  }
  // calculate leftover

  if (d >= 8) {
    float32x4x2_t a = vld1q_f32_x2(x + dim - d);
    float32x4x2_t b = vld1q_f32_x2(y + dim - d);
    float32x4x2_t c;
    c.val[0] = vmulq_f32(a.val[0], b.val[0]);
    c.val[1] = vmulq_f32(a.val[1], b.val[1]);
    c.val[0] = vaddq_f32(c.val[0], c.val[1]);
    sum = vaddq_f32(sum, c.val[0]);
    d -= 8;
  }

  if (d >= 4) {
    float32x4_t a = vld1q_f32(x + dim - d);
    float32x4_t b = vld1q_f32(y + dim - d);
    float32x4_t c;
    c = vmulq_f32(a, b);
    sum = vaddq_f32(sum, c);
    d -= 4;
  }

  float32x4_t res_x = vdupq_n_f32(0.0f);
  float32x4_t res_y = vdupq_n_f32(0.0f);
  if (d >= 3) {
      res_x = vld1q_lane_f32(x + dim - d, res_x, 2);
      res_y = vld1q_lane_f32(y + dim - d, res_y, 2);
      d -= 1;
  }

  if (d >= 2) {
      res_x = vld1q_lane_f32(x + dim - d, res_x, 1);
      res_y = vld1q_lane_f32(y + dim - d, res_y, 1);
      d -= 1;
  }

  if (d >= 1) {
      res_x = vld1q_lane_f32(x + dim - d, res_x, 0);
      res_y = vld1q_lane_f32(y + dim - d, res_y, 0);
      d -= 1;
  }

  sum = vaddq_f32(sum, vmulq_f32(res_x, res_y));
  distance = vaddvq_f32(sum);
  return ret;
}
#endif

} // common
} // oceanbase
#endif
