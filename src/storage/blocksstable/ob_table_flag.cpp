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
#include "storage/blocksstable/ob_table_flag.h"

namespace oceanbase
{
namespace storage
{

ObTableFlag::ObTableFlag()
  : has_backup_flag_(ObTableHasBackupFlag::NO_BACKUP),
    has_local_flag_(ObTableHasLocalFlag::HAS_LOCAL),
    is_shared_flag_(ObTableIsSharedFlag::IS_NOT_SHARED),
    reserved_(0)
{
}

ObTableFlag::ObTableFlag(int64_t flag)
  : flag_(flag)
{
}

ObTableFlag::~ObTableFlag()
{
}

void ObTableFlag::reset()
{
  has_backup_flag_ = ObTableHasBackupFlag::NO_BACKUP;
  has_local_flag_ = ObTableHasLocalFlag::HAS_LOCAL;
  is_shared_flag_ = ObTableIsSharedFlag::IS_NOT_SHARED;
  reserved_ = 0;
}

bool ObTableFlag::is_valid() const
{
  return has_backup_flag_ >= ObTableHasBackupFlag::NO_BACKUP && has_backup_flag_ < ObTableHasBackupFlag::MAX
      && has_local_flag_ >= ObTableHasLocalFlag::HAS_LOCAL && has_local_flag_ < ObTableHasLocalFlag::MAX
      && is_shared_flag_ >= ObTableIsSharedFlag::IS_NOT_SHARED && is_shared_flag_ < ObTableIsSharedFlag::MAX;
}

void ObTableFlag::set_default()
{
  has_backup_flag_ = ObTableHasBackupFlag::NO_BACKUP;
  has_local_flag_ = ObTableHasLocalFlag::HAS_LOCAL;
  is_shared_flag_ = ObTableIsSharedFlag::IS_NOT_SHARED;
  reserved_ = 0;
}

void ObTableFlag::clear()
{
  has_backup_flag_ = ObTableHasBackupFlag::NO_BACKUP;
  has_local_flag_ = ObTableHasLocalFlag::NO_LOCAL;
  is_shared_flag_ = ObTableIsSharedFlag::IS_NOT_SHARED;
  reserved_ = 0;
}

OB_SERIALIZE_MEMBER(ObTableFlag, flag_);

}
}
