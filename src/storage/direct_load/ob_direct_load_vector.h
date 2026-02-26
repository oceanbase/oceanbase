/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "share/datum/ob_datum.h"
#include "share/vector/ob_i_vector.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObColDesc;
} // namespace schema
} // namespace share
namespace storage
{
class ObDirectLoadVector
{
public:
  ObDirectLoadVector() = default;
  virtual ~ObDirectLoadVector() = default;

  virtual ObIVector *get_vector() const = 0;
  virtual int64_t memory_usage() const = 0;
  virtual int64_t bytes_usage(const int64_t batch_size) const = 0;
  // for check rowkey length
  virtual void sum_bytes_usage(int64_t *sum_bytes, const int64_t batch_size) const = 0;
  // for check rowkey length with LOB columns
  virtual int sum_lob_length(int64_t *sum_bytes, const int64_t batch_size) const = 0;

  virtual void reuse(const int64_t batch_size) = 0;

  // --------- append interface --------- //
  virtual int append_default(const int64_t batch_idx) = 0;
  virtual int append_default(const int64_t batch_idx, const int64_t size) = 0;
  virtual int append_datum(const int64_t batch_idx, const ObDatum &datum) = 0;
  virtual int append_batch(const int64_t batch_idx, const ObDirectLoadVector &src,
                           const int64_t offset, const int64_t size) = 0;
  virtual int append_batch(const int64_t batch_idx, ObIVector *src, const int64_t offset,
                           const int64_t size) = 0;
  virtual int append_batch(const int64_t batch_idx, const ObDatumVector &datum_vec,
                           const int64_t offset, const int64_t size) = 0;
  virtual int append_selective(const int64_t batch_idx, const ObDirectLoadVector &src,
                               const uint16_t *selector, const int64_t size) = 0;
  virtual int append_selective(const int64_t batch_idx, ObIVector *src, const uint16_t *selector,
                               const int64_t size) = 0;
  virtual int append_selective(const int64_t batch_idx, const ObDatumVector &datum_vec,
                               const uint16_t *selector, const int64_t size) = 0;

  // --------- set interface --------- //
  virtual int set_all_null(const int64_t batch_size) { return OB_ERR_UNEXPECTED; }
  virtual int set_default(const int64_t batch_idx) = 0;
  virtual int set_datum(const int64_t batch_idx, const ObDatum &datum) = 0;

  // --------- shallow copy interface --------- //
  virtual int shallow_copy(ObIVector *src, const int64_t batch_size) = 0;
  virtual int shallow_copy(const ObDatumVector &datum_vec, const int64_t batch_size) = 0;

  // --------- get interface --------- //
  virtual int get_datum(const int64_t batch_idx, ObDatum &datum) = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;

public:
  static int create_vector(VectorFormat format, VecValueTypeClass value_tc, bool is_nullable,
                           const int64_t max_batch_size, ObIAllocator &allocator,
                           ObDirectLoadVector *&vector);
  // 定长类型:VEC_FIXED, 变长类型:VEC_DISCRETE
  static int create_vector(const share::schema::ObColDesc &col_desc, bool is_nullable,
                           const int64_t max_batch_size, ObIAllocator &allocator,
                           ObDirectLoadVector *&vector);
  static int create_vector(const common::ObObjMeta &col_type, bool is_nullable,
                           const int64_t max_batch_size, ObIAllocator &allocator,
                           ObDirectLoadVector *&vector);
};

} // namespace storage
} // namespace oceanbase
