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

#ifndef OCEANBASE_SHARE_CATALOG_JDBC_OB_JDBC_CATALOG_TYPES_H_
#define OCEANBASE_SHARE_CATALOG_JDBC_OB_JDBC_CATALOG_TYPES_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
class ObIJsonBase;
}

namespace share
{

constexpr int64_t MAX_METADATA_JSON_SIZE = 16L * 1024L * 1024L;

struct ObJdbcTargetType
{
  common::ObString name_;
  int64_t length_ = -1;
  common::ObString length_semantics_;
  int64_t precision_ = -1;
  int64_t scale_ = -1;

  TO_STRING_KV(K_(name),
               K_(length),
               K_(length_semantics),
               K_(precision),
               K_(scale));
};

struct ObJdbcColumnMetadata
{
  common::ObString name_;
  int32_t ordinal_position_ = 0;
  bool source_nullable_ = true;
  bool target_nullable_ = true;
  common::ObString source_charset_;
  common::ObString source_collation_;
  ObJdbcTargetType target_type_;

  TO_STRING_KV(K_(name),
               K_(ordinal_position),
               K_(source_nullable),
               K_(target_nullable),
               K_(source_charset),
               K_(source_collation),
               K_(target_type));
};

struct ObJdbcTableMetadataDesc
{
  static constexpr int64_t CONTRACT_VERSION = 1;

  int64_t contract_version_ = 0;
  common::ObCompatibilityMode target_mode_ = common::OCEANBASE_MODE;
  common::ObString qualified_table_name_;
  common::ObSEArray<ObJdbcColumnMetadata, 8> columns_;

  int build_scan_parameters(common::ObIAllocator &allocator,
                            common::ObString &parameters,
                            uint64_t catalog_id,
                            int64_t catalog_schema_version) const;

  TO_STRING_KV(K_(contract_version),
               K_(target_mode),
               K_(qualified_table_name),
               K_(columns));
};

class ObJdbcTableMetadataParser final
{
public:
  static int parse_scan_parameters(const common::ObString &json_text,
                                   uint64_t &catalog_id,
                                   int64_t &catalog_schema_version);
  static int parse(const common::ObString &json_text,
                   common::ObIAllocator &allocator,
                   ObJdbcTableMetadataDesc &metadata);

private:
  static int get_member(const common::ObIJsonBase &object,
                       const char *name,
                       const common::ObIJsonBase *&value,
                       const bool required);
  static int get_json_string(const common::ObIJsonBase &value,
                             common::ObString &string_value);
  static int copy_string(const common::ObIJsonBase &object,
                        const char *name,
                        common::ObIAllocator &allocator,
                        common::ObString &destination,
                        const bool required);
  static int get_number(const common::ObIJsonBase &object,
                       const char *name,
                       int64_t &destination,
                       const bool required,
                       const int64_t null_value = -1);
  static int get_bool(const common::ObIJsonBase &object,
                     const char *name,
                     int8_t &destination,
                     const bool required);
  static int get_required_object(const common::ObIJsonBase &object,
                                const char *name,
                                const common::ObIJsonBase *&value);
  static bool is_one_of(const common::ObString &value,
                       const char *const candidates[],
                       const int64_t candidate_count);
  static bool is_known_target_name(const common::ObString &name);
  static int parse_target_type(const common::ObIJsonBase &value,
                               common::ObIAllocator &allocator,
                               ObJdbcTargetType &target_type);
  static int parse_column(const common::ObIJsonBase &value,
                          common::ObIAllocator &allocator,
                          ObJdbcColumnMetadata &column);
  static int parse_columns(const common::ObIJsonBase &root,
                           common::ObIAllocator &allocator,
                           ObJdbcTableMetadataDesc &metadata);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_JDBC_OB_JDBC_CATALOG_TYPES_H_
