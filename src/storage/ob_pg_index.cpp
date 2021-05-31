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

#define USING_LOG_PREFIX STORAGE
#include "storage/ob_pg_index.h"
#include "share/ob_force_print_log.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace storage {

int ObPGKeyWrap::init(const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;

  if (!pg_key.is_valid()) {
    STORAGE_LOG(WARN, "invalid argument", K(pg_key));
  } else {
    pg_key_ = pg_key;
  }
  return ret;
}

int ObPartitionGroupIndex::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPartitionGroupIndex init twice", K(ret));
  } else if (OB_FAIL(pg_index_map_.init(ObModIds::OB_PG_INDEX_MAP))) {
    STORAGE_LOG(WARN, "partition group index map init error", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}

void ObPartitionGroupIndex::destroy()
{
  pg_index_map_.destroy();
  is_inited_ = false;
}

int ObPartitionGroupIndex::add_partition(const ObPartitionKey& pkey, const ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObPGKeyWrap* pg_key_wrap = NULL;
  lib::ObMutexGuard change_guard(change_mutex_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroupIndex not init", K(pkey), K(pg_key));
  } else if (OB_ENTRY_NOT_EXIST != (ret = pg_index_map_.get(pkey, pg_key_wrap))) {
    if (OB_SUCCESS == ret) {
      ret = OB_ENTRY_EXIST;
      STORAGE_LOG(WARN, "cannot add duplicated partition", K(ret), K(pg_key), K(pkey));
      // need revert, otherwise ref leak
      pg_index_map_.revert(pg_key_wrap);
    } else {
      STORAGE_LOG(WARN, "partition index map insert and get error", K(ret), K(pkey), K(pg_key), K(lbt()));
    }
  } else {
    if (NULL == (pg_key_wrap = op_alloc(ObPGKeyWrap))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc pg key wrap error", K(ret), KP(pg_key_wrap), K(pkey), K(pg_key));
    } else if (OB_FAIL(pg_key_wrap->init(pg_key))) {
      STORAGE_LOG(WARN, "Partition group wrap init error", K(ret), K(pkey), K(pg_key), K(*pg_key_wrap));
    } else if (OB_FAIL(pg_index_map_.insert_and_get(pkey, pg_key_wrap))) {
      STORAGE_LOG(WARN, "partition index map insert and get error", K(ret), K(pkey), K(pg_key));
    } else {
      // need revert, otherwise ref leak
      pg_index_map_.revert(pg_key_wrap);
      FLOG_INFO("ObPartitionGroupIndex add partition success", K(pkey), K(pg_key), K(lbt()));
    }
    if (OB_FAIL(ret)) {
      if (NULL != pg_key_wrap) {
        pg_index_map_.del(pkey);
        op_free(pg_key_wrap);
        pg_key_wrap = NULL;
      }
    }
  }

  return ret;
}

/*
remove partition scenario:
(1) standalone partition:
 a) gc
 b) replay slog

(2) partition in pg:
 a) gc
 b) replay slog
 c) replay remove_partition_from_pg clog

(3) rebuild, migration etc
*/
int ObPartitionGroupIndex::remove_partition(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObPGKeyWrap* pg_key_wrap = NULL;
  lib::ObMutexGuard change_guard(change_mutex_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroupIndex not init", K(pkey));
  } else if (OB_FAIL(pg_index_map_.get(pkey, pg_key_wrap))) {
    STORAGE_LOG(WARN, "partition index map get error", K(ret), K(pkey));
  } else {
    pg_index_map_.del(pkey);
    // whatever, need revert!!!
    pg_index_map_.revert(pg_key_wrap);
    FLOG_INFO("ObPartitionGroupIndex remove partition success", K(pkey), "pg_key", pg_key_wrap->get_pg_key(), K(lbt()));
  }

  return ret;
}

int ObPartitionGroupIndex::get_pg_key(const common::ObPartitionKey& pkey, common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObPGKeyWrap* pg_key_wrap = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroupIndex not init", K(pkey), K(pg_key));
  } else if (OB_FAIL(pg_index_map_.get(pkey, pg_key_wrap))) {
    STORAGE_LOG(DEBUG, "partition index map insert and get error", K(ret), K(pkey), K(pg_key), K(lbt()));
  } else {
    if (!pg_key_wrap->get_pg_key().is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "invalid partition group wrap key, unexpected error", K(ret), K(pkey), K(*pg_key_wrap));
    } else {
      pg_key = pg_key_wrap->get_pg_key();
    }
    // whatever, need revert!!!
    pg_index_map_.revert(pg_key_wrap);
  }

  return ret;
}

ObPartitionGroupIndex::ObPartitionGroupIndex()
    : is_inited_(false), pg_index_map_(), change_mutex_(ObLatchIds::OB_PG_INDEX_LOCK)
{}

}  // namespace storage
}  // namespace oceanbase
