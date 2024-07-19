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

#include "lib/vector/ob_vector.h"
#include "lib/vector/ob_vector_l2_distance.h"
#include "lib/vector/ob_vector_ip_distance.h"
#include "lib/vector/ob_vector_cosine_distance.h"
#include "common/object/ob_obj_compare.h"

namespace oceanbase
{
namespace common
{

int ObVectorArray::get_part_cnt(int64_t idx, int64_t& part_cnt)
{
  int ret = OB_SUCCESS;
  part_cnt = -1;
  if (OB_UNLIKELY(idx < 0 || idx >= part_cnt_length_)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "idx out of range", K(ret), K(idx));
  } else {
    part_cnt = part_cnts_[idx] - (idx == 0 ? 0 : part_cnts_[idx - 1]);
  }
  return ret;
}

int ObVectorArray::get_part_offset(int64_t idx, int64_t dims, int64_t& part_offset)
{
  int ret = OB_SUCCESS;
  part_offset = -1;
  if (OB_UNLIKELY(idx < 0 || idx >= part_cnt_length_)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "idx out of range", K(ret), K(idx));
  } else {
    part_offset = (idx == 0 ? 0 : part_cnts_[idx - 1] * dims);
  }
  return ret;
}

DEFINE_SERIALIZE(ObVectorArray)
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();
  //Null ObString is allowed
  if (OB_ISNULL(buf) || OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "size overflow", K(ret),
        KP(buf), K(serialize_size), "remain", buf_len - pos);
  } else if (OB_FAIL(serialization::encode_vstr(buf, buf_len, pos, part_cnts_, sizeof(int64_t) * part_cnt_length_))) {
    LIB_LOG(WARN, "string serialize failed", K(ret), K(part_cnt_length_));
  } else if (OB_FAIL(serialization::encode_vstr(buf, buf_len, pos, ptr_, length_ * sizeof(float)))) {
    LIB_LOG(WARN, "string serialize failed", K(ret), K(length_));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObVectorArray)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  const int64_t MINIMAL_NEEDED_SIZE = 2; //at least need two bytes
  if (OB_ISNULL(buf) || OB_UNLIKELY((data_len - pos) < MINIMAL_NEEDED_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), KP(buf), "remain", data_len - pos);
  } else {
    int64_t offset_cnt_len = 0;
    part_cnts_ = reinterpret_cast<int64_t*>(const_cast<char*>(serialization::decode_vstr(buf, data_len, pos, &offset_cnt_len)));
    ptr_ = reinterpret_cast<float *>((const_cast<char *>(serialization::decode_vstr(buf, data_len, pos, &len))));
    if (OB_ISNULL(ptr_) || OB_ISNULL(part_cnts_)) {
      ret = OB_ERROR;
      LIB_LOG(WARN, "decode NULL string", K(ret));
    } else {
      length_ = static_cast<int64_t>(len) / sizeof(float);
      part_cnt_length_ = static_cast<int64_t>(offset_cnt_len) / sizeof(int64_t);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObVectorArray)
{
  return serialization::encoded_length_vstr(length_ * sizeof(float)) + 
         serialization::encoded_length_vstr(part_cnt_length_ * sizeof(int64_t));
}

void ObTypeVector::destroy(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(vals_)) {
    allocator.free(vals_);
    vals_ = nullptr;
    dims_ = 0;
  }
}

int ObTypeVector::deep_copy(const ObTypeVector& other, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other.ptr()) || 0 == other.dims()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), K(other));
  } else {
    reset();
    dims_ = other.dims_;
    if (nullptr == (vals_ = static_cast<float*>(allocator.alloc(sizeof(float) * dims_)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to alloc double array", K(ret), K_(dims));
    } else {
      MEMCPY(vals_, other.ptr(), sizeof(float) * dims_);
    }
  }
  return ret;
}

int ObTypeVector::deep_copy(const ObTypeVector& other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vals_)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "vals_ is nullptr", K(ret));
  } else if (OB_ISNULL(other.ptr()) || dims_ != other.dims()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), K(other), K(this));
  } else {
    MEMCPY(vals_, other.ptr(), sizeof(float) * dims_);
  }
  return ret;
}

int ObTypeVector::shallow_copy(ObTypeVector& other)
{
  int ret = OB_SUCCESS;
  assign(other.ptr(), other.dims());
  return ret;
}

int ObTypeVector::clear_vals()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vals_)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "vals_ is nullptr", K(ret));
  } else {
    MEMSET(vals_, 0, sizeof(float) * dims_);
  }
  return ret;
}

int ObTypeVector::add(const ObTypeVector& other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vals_)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "vals_ is nullptr", K(ret));
  } else if (OB_ISNULL(other.ptr()) || dims_ != other.dims()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), K(other), K(this));
  } else if (OB_FAIL(ObVectorL2Distance::vector_add_func(vals_, other.ptr(), dims_))) {
    LIB_LOG(WARN, "failed to add vector", K(ret), K(other), K(this));
  }
  // } else {
  //   for (int64_t i = 0; i < dims_; ++i) {
  //     vals_[i] += other.at(i); // TODO(@jingshui): maybe overflow
  //   }
  // }
  return ret;
}

int ObTypeVector::divide(const int64_t divisor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vals_)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "vals_ is nullptr", K(ret));
  } else if (0 == divisor) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), K(divisor));
  } else {
    for (int64_t i = 0; i < dims_; ++i) {
      vals_[i] /= divisor;
    }
  }
  return ret;
}

int ObTypeVector::vector_cmp(const ObTypeVector& other)
{
  int cmp_res = ObObjCmpFuncs::CR_EQ;
  const double P[] =
  {
    5/1e000, 5/1e001, 5/1e002, 5/1e003, 5/1e004, 5/1e005, 5/1e006, 5/1e007,
    5/1e008, 5/1e009, 5/1e010, 5/1e011, 5/1e012, 5/1e013, 5/1e014, 5/1e015,
    5/1e016, 5/1e017, 5/1e018, 5/1e019, 5/1e020, 5/1e021, 5/1e022, 5/1e023,
    5/1e024, 5/1e025, 5/1e026, 5/1e027, 5/1e028, 5/1e029, 5/1e030, 5/1e031
  };
  if (OB_UNLIKELY(dims_ != other.dims())) {
    cmp_res = ObObjCmpFuncs::CR_OB_ERROR;
  } else {
    double p = P[precision+1];
    for (int64_t i = 0; i < dims_; ++i) {
      if (vals_[i] == other.at(i) || fabs(vals_[i] - other.at(i)) < p) {
        continue;
      } else if (vals_[i] < other.at(i)) {
        cmp_res = ObObjCmpFuncs::CR_LT;
        break;
      } else if (vals_[i] > other.at(i)) {
        cmp_res = ObObjCmpFuncs::CR_GT;
        break;
      }
    }
  }
  return cmp_res;
}

bool ObTypeVector::vector_lt(const ObTypeVector& other)
{
  return vector_cmp(other) == ObObjCmpFuncs::CR_LT;
}

bool ObTypeVector::vector_le(const ObTypeVector& other)
{
  int cmp_res = vector_cmp(other);
  return cmp_res == ObObjCmpFuncs::CR_LT ||
          cmp_res == ObObjCmpFuncs::CR_EQ;
}

bool ObTypeVector::vector_eq(const ObTypeVector& other)
{
  return vector_cmp(other) == ObObjCmpFuncs::CR_EQ;
}

bool ObTypeVector::vector_ne(const ObTypeVector& other)
{
  int cmp_res = vector_cmp(other);
  return cmp_res == ObObjCmpFuncs::CR_LT ||
          cmp_res == ObObjCmpFuncs::CR_GT;
}

bool ObTypeVector::vector_ge(const ObTypeVector& other)
{
  int cmp_res = vector_cmp(other);
  return cmp_res == ObObjCmpFuncs::CR_GT ||
          cmp_res == ObObjCmpFuncs::CR_EQ;
}

bool ObTypeVector::vector_gt(const ObTypeVector& other)
{
  return vector_cmp(other) == ObObjCmpFuncs::CR_GT;
}

const char* ObTypeVector::get_distance_expr_str(const ObVectorDistanceType distance_type)
{
    const char *str = "";
    switch(distance_type) {
    case L2: {
      str = N_VECTOR_L2_DISTANCE;
      break;
    }
    case INNER_PRODUCT: {
      str = N_VECTOR_INNER_PRODUCT;
      break;
    }
    case COSINE: {
      str = N_VECTOR_COS_DISTANCE;
      break;
    }
    default:
      break;
  }
    return str;
}

int ObTypeVector::cal_l2_distance(const ObTypeVector &other, double &distance) const
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (OB_UNLIKELY(other.dims() != dims_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "vector length mismatch", K(ret), K(other.dims()), K_(dims));
  } else if (OB_FAIL(ObVectorL2Distance::l2_distance_func(vals_, other.ptr(), dims_, distance))) {
    LIB_LOG(WARN, "faild to cal l2 distance", K(ret));
  }
  return ret;
}

int ObTypeVector::cal_l2_square(const ObTypeVector &other, double &square) const
{
  int ret = OB_SUCCESS;
  square = 0;
  if (OB_UNLIKELY(other.dims() != dims_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "vector length mismatch", K(ret), K(other.dims()), K_(dims));
  } else if (OB_FAIL(ObVectorL2Distance::l2_square_func(vals_, other.ptr(), dims_, square))) {
    LIB_LOG(WARN, "faild to cal l2 square", K(ret));
  }
  return ret;
}

int ObTypeVector::cal_cosine_distance(const ObTypeVector &other, double &distance) const
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (OB_UNLIKELY(other.dims() != dims_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "vector length mismatch", K(ret), K(other.dims()), K_(dims));
  } else if (OB_FAIL(ObTypeVector::cosine_distance(*this, other, distance))) {
    LIB_LOG(WARN, "faild to cal cosine distance", K(ret));
  }
  return ret;
}

int ObTypeVector::cal_inner_product_distance(const ObTypeVector &other, double &distance) const
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (OB_UNLIKELY(other.dims() != dims_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "vector length mismatch", K(ret), K(other.dims()), K_(dims));
  } else if (OB_FAIL(ObTypeVector::ip_distance(*this, other, distance))) {
    LIB_LOG(WARN, "faild to cal inner product distance", K(ret));
  }
  return ret;
}

int ObTypeVector::cal_angular_distance(const ObTypeVector &other, double &distance) const
{
  int ret = OB_SUCCESS;
  distance = 0;
  if (OB_UNLIKELY(other.dims() != dims_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "vector length mismatch", K(ret), K(other.dims()), K_(dims));
  } else if (OB_FAIL(ObVectorCosineDistance::angular_distance_func(vals_, other.ptr(), dims_, distance))) {
    LIB_LOG(WARN, "faild to cal angular distance", K(ret));
  }
  return ret;
}

int ObTypeVector::cal_distance(const common::ObVectorDistanceType vd_type, const ObTypeVector &other, double &distance) const
{
  int ret = OB_SUCCESS;
  distance = 0;
  switch(vd_type) {
    case L2: {
      ret = cal_l2_distance(other, distance);
      break;
    }
    case INNER_PRODUCT: {
      ret = cal_inner_product_distance(other, distance);
      break;
    }
    case COSINE: {
      ret = cal_cosine_distance(other, distance);
      break;
    }
    default:{
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObTypeVector::cal_kmeans_distance(const common::ObVectorDistanceType vd_type, const ObTypeVector &other, double &distance) const
{
  int ret = OB_SUCCESS;
  distance = 0;
  switch(vd_type) {
    case L2: {
      ret = cal_l2_distance(other, distance);
      break;
    }
    case INNER_PRODUCT:
    case COSINE: {
      ret = cal_angular_distance(other, distance);
      break;
    }
    default:{
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObTypeVector::l2_distance(const ObTypeVector& a, const ObTypeVector& b, double &distance)
{
  OB_ASSERT(a.dims() == b.dims());
  return ObVectorL2Distance::l2_distance_func(a.ptr(), b.ptr(), a.dims(), distance);
}

int ObTypeVector::ip_distance(const ObTypeVector& a, const ObTypeVector& b, double &distance)
{
  OB_ASSERT(a.dims() == b.dims());
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObVectorIpDistance::ip_distance_func(a.ptr(), b.ptr(), a.dims(), distance))) {
    distance = - distance;
  }
  return ret;
}

int ObTypeVector::cosine_distance(const ObTypeVector& a, const ObTypeVector& b, double &distance)
{
  OB_ASSERT(a.dims() == b.dims());
  return ObVectorCosineDistance::cosine_distance_func(a.ptr(), b.ptr(), a.dims(), distance);
}

int ObTypeVector::get_vector_dfunc(common::ObVectorDistanceType vd_type, VectorIndexDistanceFunc& func)
{
  int ret = OB_SUCCESS;
  func = nullptr;
  switch(vd_type) {
  case L2: {
    func = l2_distance;
    break;
  }
  case INNER_PRODUCT: {
    func = ip_distance;
    break;
  }
  case COSINE: {
    func = cosine_distance;
    break;
  }
  default:{
    ret = OB_ERR_UNEXPECTED;
  }
  }
  return ret;
}

DEFINE_SERIALIZE(ObTypeVector)
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();
  //Null ObString is allowed
  if (OB_ISNULL(buf) || OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "size overflow", K(ret),
        KP(buf), K(serialize_size), "remain", buf_len - pos);
  } else if (OB_FAIL(serialization::encode_vstr(buf, buf_len, pos, vals_, dims_ * sizeof(float)))) {
    LIB_LOG(WARN, "string serialize failed", K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTypeVector)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  const int64_t MINIMAL_NEEDED_SIZE = 2; //at least need two bytes
  if (OB_ISNULL(buf) || OB_UNLIKELY((data_len - pos) < MINIMAL_NEEDED_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), KP(buf), "remain", data_len - pos);
  } else {
    vals_ = reinterpret_cast<float *>((const_cast<char *>(serialization::decode_vstr(buf, data_len, pos, &len))));
    if (OB_ISNULL(vals_)) {
        ret = OB_ERROR;
        LIB_LOG(WARN, "decode NULL string", K(ret));
    } else {
        dims_ = static_cast<int64_t>(len) / sizeof(float);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTypeVector)
{
  return serialization::encoded_length_vstr(dims_ * sizeof(float));
}

int64_t ObTypeVector::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_ARRAY_START();
    if (OB_NOT_NULL(vals_)) {
      for (int64_t i = 0; i < dims_; ++i) {
        if (0 == i) {
          BUF_PRINTF("%.6f", at(i));
        } else {
          J_COMMA();
          BUF_PRINTF("%.6f", at(i));
        }
      }
    }
    J_ARRAY_END();
  }
  return pos;
}

}
}