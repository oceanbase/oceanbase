/**
 * Copyright (c) 2024 OceanBase
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

#include "common/object/ob_obj_type.h"
#include "share/vector/ob_i_vector.h"

namespace oceanbase
{
namespace common
{
class ObFixedLengthBase;
class ObContinuousBase;
class ObUniformBase;
class ObDatum;
} // namespace common
namespace share
{
class ObTabletCacheInterval;
} // namespace share
namespace blocksstable
{
class ObStorageDatum;
} // namespace blocksstable
namespace sql
{
class ObBatchRows;
} // namespace sql
namespace storage
{
class ObDirectLoadVectorUtils
{
public:
  static int new_vector(VectorFormat format, VecValueTypeClass value_tc, ObIAllocator &allocator,
                        common::ObIVector *&vector);
  static int prepare_vector(ObIVector *vector, const int64_t max_batch_size,
                            ObIAllocator &allocator);

  // 按需实现, 目前只支持以下场景:
  // * VEC_CONTINUOUS, VEC_DISCRETE, VEC_UNIFORM, VEC_UNIFORM_CONST -> VEC_DISCRETE
  // * VEC_UNIFORM -> VEC_UNIFORM
  // * VEC_UNIFORM_CONST -> VEC_UNIFORM_CONST
  static int shallow_copy_vector(ObIVector *src_vec, ObIVector *dest_vec, const int64_t batch_size);

  // 按需实现, 目前只支持以下场景:
  // * VEC_UNIFORM_CONST -> VEC_DISCRETE
  // * VEC_UNIFORM_CONST -> VEC_UNIFORM
  static int expand_const_vector(ObIVector *const_vec, ObIVector *dest_vec, const int64_t batch_size);

  // 按需实现, 目标只支持以下场景
  // * const_datum -> VEC_DISCRETE
  // * const_datum -> VEC_UNIFORM
  static int expand_const_datum(const common::ObDatum &const_datum, ObIVector *dest_vec, const int64_t batch_size);

  static int get_payload(ObIVector *vector, const int64_t idx, bool &is_null, const char *&payload,
                         ObLength &len);

  static int to_datum(common::ObIVector *vector, const int64_t idx, common::ObDatum &datum);
  static int to_datums(const common::ObIArray<common::ObIVector *> &vectors, int64_t idx,
                       common::ObDatum *datums, const int64_t count);
  static int to_datums(const common::ObIArray<common::ObIVector *> &vectors, int64_t idx,
                       blocksstable::ObStorageDatum *datums, const int64_t count);
  static int set_datum(common::ObIVector *vector, const int64_t idx, const common::ObDatum &datum);

  // tablet id vector, ginore null value
  static const VecValueTypeClass tablet_id_value_tc = VEC_TC_INTEGER;
  static int make_const_tablet_id_vector(const ObTabletID &tablet_id, ObIAllocator &allocator,
                                         common::ObIVector *&vector);
  static int set_tablet_id(common::ObIVector *vector, const int64_t batch_idx,
                           const ObTabletID &tablet_id);
  static ObTabletID get_tablet_id(common::ObFixedLengthBase *vector, const int64_t batch_idx);
  template <bool IS_CONST>
  static ObTabletID get_tablet_id(common::ObUniformBase *vector, const int64_t batch_idx);
  static ObTabletID get_tablet_id(common::ObIVector *vector, const int64_t batch_idx);
  static bool check_all_tablet_id_is_same(common::ObIVector *vector, const int64_t size);

  // hidden pk vector
  static int batch_fill_hidden_pk(common::ObIVector *vector, const int64_t start,
                                  const int64_t size, share::ObTabletCacheInterval &pk_interval);

  // multi version vector
  static const VecValueTypeClass multi_version_value_tc = VEC_TC_INTEGER;
  static int make_const_multi_version_vector(const int64_t value, ObIAllocator &allocator,
                                             common::ObIVector *&vector);
};

} // namespace storage
} // namespace oceanbase
