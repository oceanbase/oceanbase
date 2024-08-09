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

#ifndef SRC_STORAGE_BLOCKSSTABLE_TABLE_FLAG_H_
#define SRC_STORAGE_BLOCKSSTABLE_TABLE_FLAG_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{

class ObTableHasBackupFlag final
{
public:
  enum FLAG
  {
    NO_BACKUP = 0,
    HAS_BACKUP = 1,
    MAX,
  };
};

class ObTableHasLocalFlag final
{
public:
  // TODO: yangyi.yyy
  enum FLAG
  {
    HAS_LOCAL = 0,
    NO_LOCAL = 1,
    MAX
  };
};

class ObTableIsSharedFlag final
{
public:
  enum FLAG
  {
    IS_NOT_SHARED = 0,
    IS_SHARED = 1,
    MAX
  };
};

struct ObTableFlag final
{
  OB_UNIS_VERSION(1);
public:
  ObTableFlag();
  // TODO: yangyi.yyy to Refactor
  ObTableFlag(int64_t flag);
  ~ObTableFlag();
  void reset();
  bool is_valid() const;
  OB_INLINE bool operator==(const ObTableFlag &other) const;
  OB_INLINE bool operator!=(const ObTableFlag &other) const;
  void set_default();
  void clear();
  TO_STRING_KV(K_(has_backup_flag), K_(has_local_flag), K_(is_shared_flag));

public:
  bool has_backup() const { return ObTableHasBackupFlag::HAS_BACKUP == has_backup_flag_; }
  bool has_no_backup() const { return ObTableHasBackupFlag::NO_BACKUP == has_backup_flag_; }
  void set_has_backup() { has_backup_flag_ = ObTableHasBackupFlag::HAS_BACKUP; }
  void set_no_backup() { has_backup_flag_ = ObTableHasBackupFlag::NO_BACKUP; }
  bool has_local() const { return ObTableHasLocalFlag::HAS_LOCAL == has_local_flag_; }
  void set_has_local() { has_local_flag_ = ObTableHasLocalFlag::HAS_LOCAL; }
  void set_no_local() { has_local_flag_ = ObTableHasLocalFlag::NO_LOCAL; }

private:
  static const uint64_t SF_BIT_HAS_BACKUP = 1;
  static const uint64_t SF_BIT_HAS_LOCAL = 1;
  static const uint64_t SF_BIT_IS_SHARED = 1;
  static const uint64_t SF_BIT_RESERVED = 61;

public:
  union {
    int64_t flag_;
    struct {
      ObTableHasBackupFlag::FLAG has_backup_flag_ : SF_BIT_HAS_BACKUP;
      ObTableHasLocalFlag::FLAG has_local_flag_   : SF_BIT_HAS_LOCAL;
      ObTableIsSharedFlag::FLAG is_shared_flag_   : SF_BIT_IS_SHARED;
      int64_t reserved_: SF_BIT_RESERVED;
    };
  };
};

bool ObTableFlag::operator==(const ObTableFlag &other) const
{
  return has_backup_flag_ == other.has_backup_flag_
      && has_local_flag_ == other.has_local_flag_
      && is_shared_flag_ == other.is_shared_flag_;
}

bool ObTableFlag::operator!=(const ObTableFlag &other) const
{
  return !(this->operator==(other));
}

}
}

#endif