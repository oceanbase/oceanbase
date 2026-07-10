/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SHARE_OB_CATALOG_EXT_PARTITION_INFO_H
#define OB_SHARE_OB_CATALOG_EXT_PARTITION_INFO_H

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

// External / catalog table partition spec (not ObTablePartitionInfo / ddl PartitionInfo).
// part_stattype_: use 0 as unset; same value as StatTypeLocked::NULL_TYPE in ob_stat_define.h.
struct ObCatalogExtPartitionInfo
{
  ObCatalogExtPartitionInfo()
      : partition_(), path_(), partition_values_(), iceberg_part_names_(), iceberg_part_sql_literals_(),
        modify_ts_(0), data_size_(0), file_num_(0), schema_version_(0), part_stattype_(0),
        iceberg_spec_id_(-1)
  {
  }

  int assign(const ObCatalogExtPartitionInfo &other);

  ObString partition_;
  ObString path_;
  ObSEArray<ObString, 4> partition_values_;
  ObSEArray<ObString, 4> iceberg_part_names_;
  ObSEArray<ObString, 4> iceberg_part_sql_literals_;
  int64_t modify_ts_;
  int64_t data_size_;
  int64_t file_num_;
  int64_t schema_version_;
  int64_t part_stattype_;
  int64_t iceberg_spec_id_;

  int64_t get_size() const
  {
    int64_t size = sizeof(ObCatalogExtPartitionInfo)
                   + sizeof(ObString) + partition_.length() + 1
                   + sizeof(ObString) + path_.length() + 1;
    for (int64_t i = 0; i < partition_values_.count(); ++i) {
      size += sizeof(ObString) + partition_values_.at(i).length() + 1;
    }
    for (int64_t i = 0; i < iceberg_part_names_.count(); ++i) {
      size += sizeof(ObString) + iceberg_part_names_.at(i).length() + 1;
    }
    for (int64_t i = 0; i < iceberg_part_sql_literals_.count(); ++i) {
      size += sizeof(ObString) + iceberg_part_sql_literals_.at(i).length() + 1;
    }
    return size;
  }

  TO_STRING_KV(K_(partition),
               K_(path),
               K_(iceberg_part_names),
               K_(iceberg_part_sql_literals),
               K_(iceberg_spec_id),
               K_(modify_ts),
               K_(file_num),
               K_(data_size),
               K_(schema_version),
               K_(part_stattype));
};

}  // namespace common
}  // namespace oceanbase

#endif  // OB_SHARE_OB_CATALOG_EXT_PARTITION_INFO_H
