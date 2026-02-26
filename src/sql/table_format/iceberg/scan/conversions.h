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

#ifndef CONVERSIONS_H
#define CONVERSIONS_H

#include "common/object/ob_object.h"

#include <optional>

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObColumnSchemaV2;
}
} // namespace share

namespace sql
{
class ObColumnMeta;

namespace iceberg
{

class Conversions
{
public:
  static int convert_statistics_binary_to_ob_obj(ObIAllocator &allocator,
                                                 const ObString &binary,
                                                 const ObObjType ob_obj_type,
                                                 const std::optional<ObCollationType> collation_type,
                                                 const std::optional<int16_t> ob_type_precision,
                                                 const std::optional<int16_t> ob_type_scale,
                                                 ObObj &ob_obj);

  static int convert_statistics_binary_to_ob_obj(
      ObIAllocator &allocator,
      const ObString &binary,
      const share::schema::ObColumnSchemaV2 &column_schema,
      ObObj &ob_obj);
  static int convert_statistics_binary_to_ob_obj(
      ObIAllocator &allocator,
      const ObString &binary,
      const ObColumnMeta &column_meta,
      ObObj &ob_obj);

  static int decimal_required_bytes(int32_t precision, int32_t &required_bytes);
};

} // namespace iceberg

} // namespace sql

} // namespace oceanbase

#endif // CONVERSIONS_H
