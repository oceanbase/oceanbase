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

#include "ob_archive_util.h"
#include "ob_archive_mgr.h"
#include "clog/ob_partition_log_service.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_group.h"

namespace oceanbase {
namespace archive {
using namespace oceanbase::clog;
using namespace oceanbase::common;
using namespace oceanbase::storage;
void* ob_archive_malloc(const int64_t nbyte)
{
  ObMemAttr memattr;
  memattr.label_ = ObModIds::OB_ARCHIVE_TEMP_MEMORY;

  return ob_malloc(nbyte, memattr);
}

void ob_archive_free(void* ptr)
{
  ob_free(ptr);
}

int check_is_leader(const ObPGKey& pg_key, const int64_t epoch, bool& is_leader)
{
  int ret = OB_SUCCESS;
  int64_t current_epoch = 0;
  is_leader = false;
  ObRole role;
  ObIPartitionLogService* pls = NULL;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;

  if (OB_UNLIKELY(!pg_key.is_valid()) || OB_UNLIKELY(epoch <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", K(pg_key), K(epoch));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, guard))) {
    ARCHIVE_LOG(WARN, "get_partition fail", KR(ret), K(pg_key), K(epoch));
    is_leader = false;
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "get_partition_group fail", KR(ret), K(pg_key));
  } else if (OB_ISNULL(pls = partition->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ObIPartitionLogService is NULL", KR(ret), K(pg_key));
  } else if (OB_FAIL(pls->get_role_and_leader_epoch(role, current_epoch)) && OB_NOT_MASTER != ret) {
    ARCHIVE_LOG(WARN, "get_role_and_leader_epoch fail", KR(ret), K(pg_key));
    is_leader = false;
    ret = OB_SUCCESS;
  } else if (OB_NOT_MASTER == ret) {
    is_leader = false;
    ret = OB_SUCCESS;
  } else if (!is_strong_leader(role)) {
    is_leader = false;
    ARCHIVE_LOG(DEBUG, "pg role is not leader, skip", K(pg_key), K(role));
  } else if (current_epoch != epoch) {
    is_leader = false;
    ARCHIVE_LOG(DEBUG, "pg role is not leader, skip", K(pg_key), K(role), K(epoch), K(current_epoch), K(is_leader));
  } else {
    is_leader = true;
  }

  return ret;
}

bool is_valid_archive_compressor_type(const common::ObCompressorType compressor_type)
{
  return ((LZ4_COMPRESSOR == compressor_type) || (ZSTD_1_3_8_COMPRESSOR == compressor_type));
}
}  // namespace archive
}  // namespace oceanbase
