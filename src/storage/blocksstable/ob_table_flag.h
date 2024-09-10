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
  enum FLAG
  {
    HAS_LOCAL = 0,
    NO_LOCAL = 1,
    MAX
  };
};

struct ObTableBackupFlag final
{
  OB_UNIS_VERSION(1);
public:
  ObTableBackupFlag();
  // TODO: yangyi.yyy to Refactor
  ObTableBackupFlag(int64_t flag);
  ~ObTableBackupFlag();
  void reset();
  bool is_valid() const;
  OB_INLINE bool operator==(const ObTableBackupFlag &other) const;
  OB_INLINE bool operator!=(const ObTableBackupFlag &other) const;
  void set_default();
  void clear();
  TO_STRING_KV(K_(has_backup_flag), K_(has_local_flag));

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
  static const uint64_t SF_BIT_RESERVED = 30;

public:
  union {
    int32_t flag_;
    struct {
      ObTableHasBackupFlag::FLAG has_backup_flag_ : SF_BIT_HAS_BACKUP;
      ObTableHasLocalFlag::FLAG has_local_flag_   : SF_BIT_HAS_LOCAL;
      int64_t reserved_: SF_BIT_RESERVED;
    };
  };
};

bool ObTableBackupFlag::operator==(const ObTableBackupFlag &other) const
{
  return flag_ == other.flag_;
}

bool ObTableBackupFlag::operator!=(const ObTableBackupFlag &other) const
{
  return !(this->operator==(other));
}

struct ObTableSharedFlag final
{
  OB_UNIS_VERSION(1);
public:
  enum FLAG : uint8_t
  {
    PRIVATE = 0,                     //share nothing
    SHARED_SSTABLE = 1,              //sstable is public data, including meta tree and data
    SHARED_MACRO_BLOCKS = 2,         //only macro block is public data, meta tree is private
    MAX
  };
public:
  ObTableSharedFlag();
  ~ObTableSharedFlag();
  void reset();
  bool is_valid() const;
  OB_INLINE bool operator==(const ObTableSharedFlag &other) const;
  OB_INLINE bool operator!=(const ObTableSharedFlag &other) const;
  void set_default();
  void clear();

  void set_private() { shared_flag_ = PRIVATE; }
  void set_shared_sstable() { shared_flag_ = SHARED_SSTABLE; }
  void set_share_macro_blocks() { shared_flag_ = SHARED_MACRO_BLOCKS; }
  bool is_shared_macro_blocks() const {
    return SHARED_SSTABLE == shared_flag_
        || SHARED_MACRO_BLOCKS == shared_flag_; }
  bool is_shared_sstable() const { return SHARED_SSTABLE == shared_flag_; }
  bool is_only_shared_macro_blocks() const {
    return SHARED_SSTABLE != shared_flag_
        && SHARED_MACRO_BLOCKS == shared_flag_; }
  TO_STRING_KV(K_(shared_flag), K_(reserved));

private:
  static const uint64_t SF_BIT_IS_SHARED = 8;
  static const uint64_t SF_BIT_RESERVED = 24;
  union {
    int32_t flag_;
    struct {;
      FLAG shared_flag_ : SF_BIT_IS_SHARED;
      int32_t reserved_: SF_BIT_RESERVED;
    };
  };
};

bool ObTableSharedFlag::operator==(const ObTableSharedFlag &other) const
{
  return flag_ == other.flag_;
}

bool ObTableSharedFlag::operator!=(const ObTableSharedFlag &other) const
{
  return !(this->operator==(other));
}


}
}

#endif
