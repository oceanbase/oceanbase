/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_OPT_HIVE_DEFINE_FWD_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_OPT_HIVE_DEFINE_FWD_H

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_catalog_ext_partition_info.h"

namespace oceanbase
{
namespace sql
{

struct ObHiveFileDesc
{
  ObHiveFileDesc() : part_id_(OB_INVALID_PARTITION_ID), file_size_(0), modify_ts_(0)
  {
  }
  TO_STRING_KV(K(part_id_), K(file_size_), K(modify_ts_), K(file_path_));
  int64_t part_id_;
  int64_t file_size_;
  int64_t modify_ts_;
  common::ObString file_path_;
};

// Same as common::ObCatalogExtPartitionInfo; keep sql-layer name for optimizer / external table.
typedef common::ObCatalogExtPartitionInfo HivePartitionInfo;

}  // namespace sql
}  // namespace oceanbase

#endif  // _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_OPT_HIVE_DEFINE_FWD_H
