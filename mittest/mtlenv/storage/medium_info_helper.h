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

#ifndef OCEANBASE_UNITTEST_MEDIUM_INFO_HELPER
#define OCEANBASE_UNITTEST_MEDIUM_INFO_HELPER

#include <stdint.h>
#include "lib/allocator/ob_allocator.h"
#include "unittest/storage/init_basic_struct.h"
#include "unittest/storage/schema_utils.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"

namespace oceanbase
{
namespace storage
{
class MediumInfoHelper
{
public:
  static int build_medium_compaction_info(
      common::ObIAllocator &allocator,
      compaction::ObMediumCompactionInfo &info,
      const int64_t medium_snapshot);
};

int MediumInfoHelper::build_medium_compaction_info(
    common::ObIAllocator &allocator,
    compaction::ObMediumCompactionInfo &info,
    const int64_t medium_snapshot)
{
  int ret = common::OB_SUCCESS;
  info.compaction_type_ = compaction::ObMediumCompactionInfo::ObCompactionType::MEDIUM_COMPACTION;
  info.medium_snapshot_ = medium_snapshot;
  info.last_medium_snapshot_ = medium_snapshot;
  info.data_version_ = 100;
  info.cluster_id_ = 1;

  // storage schema
  const uint64_t table_id = 1234567;
  share::schema::ObTableSchema table_schema;
  build_test_schema(table_schema, table_id);
  ret = info.storage_schema_.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL);

  return ret;
}
}
}

#endif // OCEANBASE_UNITTEST_MEDIUM_INFO_HELPER
