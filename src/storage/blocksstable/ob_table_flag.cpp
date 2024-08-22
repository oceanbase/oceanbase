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

ObTableBackupFlag::ObTableBackupFlag()
  : has_backup_flag_(ObTableHasBackupFlag::NO_BACKUP),
    has_local_flag_(ObTableHasLocalFlag::HAS_LOCAL),
    reserved_(0)
{
}

ObTableBackupFlag::ObTableBackupFlag(int64_t flag)
  : flag_(flag)
{
}

ObTableBackupFlag::~ObTableBackupFlag()
{
}

void ObTableBackupFlag::reset()
{
  has_backup_flag_ = ObTableHasBackupFlag::NO_BACKUP;
  has_local_flag_ = ObTableHasLocalFlag::HAS_LOCAL;
  reserved_ = 0;
}

bool ObTableBackupFlag::is_valid() const
{
  return has_backup_flag_ >= ObTableHasBackupFlag::NO_BACKUP && has_backup_flag_ < ObTableHasBackupFlag::MAX
      && has_local_flag_ >= ObTableHasLocalFlag::HAS_LOCAL && has_local_flag_ < ObTableHasLocalFlag::MAX;
}

void ObTableBackupFlag::set_default()
{
  has_backup_flag_ = ObTableHasBackupFlag::NO_BACKUP;
  has_local_flag_ = ObTableHasLocalFlag::HAS_LOCAL;
  reserved_ = 0;
}

void ObTableBackupFlag::clear()
{
  has_backup_flag_ = ObTableHasBackupFlag::NO_BACKUP;
  has_local_flag_ = ObTableHasLocalFlag::NO_LOCAL;
  reserved_ = 0;
}

OB_SERIALIZE_MEMBER(ObTableBackupFlag, flag_);



ObTableSharedFlag::ObTableSharedFlag()
  : shared_flag_(PRIVATE),
    reserved_(0)
{
}

ObTableSharedFlag::~ObTableSharedFlag()
{
}

void ObTableSharedFlag::reset()
{
  shared_flag_ = PRIVATE;
  reserved_ = 0;
}

bool ObTableSharedFlag::is_valid() const
{
  return shared_flag_ >= PRIVATE && shared_flag_ < MAX;
}

void ObTableSharedFlag::set_default()
{
  shared_flag_ = PRIVATE;
  reserved_ = 0;
}

OB_SERIALIZE_MEMBER(ObTableSharedFlag, flag_);


}
}
