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

#ifndef OCEANBASE_SCHEMA_FIELD_H
#define OCEANBASE_SCHEMA_FIELD_H

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

class SchemaField
{
public:
  /// \brief Construct a field.
  /// \param[in] field_id The field ID.
  /// \param[in] name The field name.
  /// \param[in] type The field type.
  /// \param[in] optional Whether values of this field are required or nullable.
  /// \param[in] doc Optional documentation string for the field.
  SchemaField(int32_t field_id,
              const ObString name,
              const Type *type,
              bool optional = false,
              const ObString doc = "")
      : field_id_(field_id), name_(name), type_(type), optional_(optional), doc_(doc)
  {
  }

  static SchemaField make_optional(int32_t field_id,
                                   const ObString &name,
                                   const Type *type,
                                   const ObString &doc = "")
  {
    return SchemaField(field_id, name, type, true, doc);
  }

  static SchemaField make_required(int32_t field_id,
                                   const ObString &name,
                                   const Type *type,
                                   const ObString &doc = "")
  {
    return SchemaField(field_id, name, type, false, doc);
  }

  int32_t field_id() const
  {
    return field_id_;
  }
  const ObString &name() const
  {
    return name_;
  }
  const Type *type() const
  {
    return type_;
  }
  bool optional() const
  {
    return optional_;
  }
  const ObString &doc() const
  {
    return doc_;
  }
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const;

  TO_STRING_KV(K_(field_id), K_(name), K_(optional), K_(doc));

private:
  int32_t field_id_;
  const ObString name_;
  const Type *type_;
  bool optional_;
  const ObString doc_;
};

} // namespace iceberg

} // namespace sql

} // namespace oceanbase

#endif // OCEANBASE_SCHEMA_FIELD_H
