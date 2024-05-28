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

#ifndef OCEANBASE_LIB_OB_VECTOR_H_
#define OCEANBASE_LIB_OB_VECTOR_H_

#include <stddef.h>
#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

class ObTypeVector
{
public:
    typedef int (*VectorIndexDistanceFunc) (const ObTypeVector&, const ObTypeVector&, double &distance);
    ObTypeVector() : vals_(NULL), dims_(0) {}
    ObTypeVector(float* vals, int64_t size) : vals_(vals), dims_(size) {}
    virtual ~ObTypeVector() {}
    void reset()
    {
      vals_ = nullptr;
      dims_ = 0;
    }
    void destroy(ObIAllocator &allocator);
    int deep_copy(const ObTypeVector& other, ObIAllocator &allocator);
    int deep_copy(const ObTypeVector& other);
    int shallow_copy(ObTypeVector& other);
    int clear_vals();
    int add(const ObTypeVector& other);
    int divide(const int64_t divisor);
    const float& at(int64_t idx) const {
        OB_ASSERT(idx < dims_);
        return vals_[idx];
    }
    float& at(int64_t idx) {
        OB_ASSERT(idx < dims_);
        return vals_[idx];
    }
    bool is_valid() const { return vals_ != NULL; }
    int64_t dims() const { return dims_; }
    float* ptr() const { return vals_; }
    void assign(const ObTypeVector& vector) {
        OB_ASSERT(dims_ == vector.dims());
        for (int64_t i = 0; i < dims_; ++i) {
            vals_[i] = vector.vals_[i];
        }
    }
    void assign(float* vals, const int64_t size) {
      vals_ = vals;
      dims_ = size;
    }
    int vector_cmp(const ObTypeVector& other);
    bool vector_lt(const ObTypeVector& other);
    bool vector_le(const ObTypeVector& other);
    bool vector_eq(const ObTypeVector& other);
    bool vector_ne(const ObTypeVector& other);
    bool vector_ge(const ObTypeVector& other);
    bool vector_gt(const ObTypeVector& other);
    // cal distance
    static const char* get_distance_expr_str(const ObVectorDistanceType distance_type);
    int cal_l2_distance(const ObTypeVector &other, double &distance) const;
    int cal_l2_square(const ObTypeVector &other, double &square) const;
    int cal_cosine_distance(const ObTypeVector &other, double &distance) const;
    int cal_inner_product_distance(const ObTypeVector &other, double &distance) const;
    int cal_angular_distance(const ObTypeVector &other, double &distance) const;
    int cal_distance(const common::ObVectorDistanceType vd_type, const ObTypeVector &other, double &distance) const;
    int cal_kmeans_distance(const common::ObVectorDistanceType vd_type, const ObTypeVector &other, double &distance) const;
    static int l2_distance(const ObTypeVector& a, const ObTypeVector& b, double &distance);
    static int cosine_distance(const ObTypeVector& a, const ObTypeVector& b, double &distance);
    static int ip_distance(const ObTypeVector& a, const ObTypeVector& b, double &distance);
    static int get_vector_dfunc(common::ObVectorDistanceType vd_type, VectorIndexDistanceFunc& func);
    NEED_SERIALIZE_AND_DESERIALIZE;
    DECLARE_TO_STRING;
public:
  static const int precision = 8;
private:
  float* vals_;
  int64_t dims_;
};

}
}

#endif