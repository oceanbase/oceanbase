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
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace common
{
class ObDatum;
} // namespace common
namespace share
{
namespace schema
{
class ObColDesc;
} // namespace schema
class ObTabletCacheInterval;
} // namespace share
namespace storage
{
class ObDirectLoadBatchRows;

class ObDirectLoadVectorUtils
{
public:
  static int new_vector(VectorFormat format, VecValueTypeClass value_tc, ObIAllocator &allocator,
                        common::ObIVector *&vector);
  static int prepare_vector(ObIVector *vector, const int64_t max_batch_size,
                            ObIAllocator &allocator);

  static int to_datum(common::ObIVector *vector, const int64_t idx, common::ObDatum &datum);

  static int check_rowkey_length(const ObDirectLoadBatchRows &batch_rows,
                                 const int64_t rowkey_column_count);
  static int check_rowkey_length(const ObDirectLoadBatchRows &batch_rows,
                                 const int64_t rowkey_column_count,
                                 const common::ObIArray<share::schema::ObColDesc> &col_descs);

  // tablet id vector, ginore null value
  static const VecValueTypeClass tablet_id_value_tc = VEC_TC_INTEGER;
  static int make_const_tablet_id_vector(const ObTabletID &tablet_id, ObIAllocator &allocator,
                                         common::ObIVector *&vector);
  static ObTabletID get_tablet_id(common::ObIVector *vector, const int64_t batch_idx);

  static bool check_all_tablet_id_is_same(const uint64_t *tablet_ids, const int64_t size);
  static bool check_is_same_tablet_id(const ObTabletID &tablet_id, common::ObIVector *vector,
                                      const int64_t size);
  static bool check_is_same_tablet_id(const ObTabletID &tablet_id, common::ObIVector *vector,
                                      const uint16_t *selector, const int64_t size);
  static bool check_is_same_tablet_id(const ObTabletID &tablet_id,
                                      const common::ObDatumVector &datum_vec, const int64_t size);
  static bool check_is_same_tablet_id(const ObTabletID &tablet_id,
                                      const common::ObDatumVector &datum_vec,
                                      const uint16_t *selector, const int64_t size);

  // hidden pk vector
  static int batch_fill_hidden_pk(common::ObIVector *vector, const int64_t start,
                                  const int64_t size, share::ObTabletCacheInterval &pk_interval);
  static int batch_fill_value(common::ObIVector *vector, const int64_t start,
                              const int64_t size, const int64_t value);

  // multi version vector
  static const VecValueTypeClass multi_version_value_tc = VEC_TC_INTEGER;
  static int make_const_multi_version_vector(const int64_t value, ObIAllocator &allocator,
                                             common::ObIVector *&vector);
};

} // namespace storage
} // namespace oceanbase
