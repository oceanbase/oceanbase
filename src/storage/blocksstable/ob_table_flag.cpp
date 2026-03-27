/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
    is_split_sstable_(0),
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
