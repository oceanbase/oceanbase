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

#define private public
#define protected public

#include "share/ob_ls_id.h"
#include "lib/random/ob_random.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/backup/ob_backup_index_cache.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
namespace backup
{

static int64_t random(const int64_t a, const int64_t b)
{
  static common::ObRandom RND;
  return RND.get(a, b);
}

static void make_random_tablet_id(common::ObTabletID &tablet_id)
{
  tablet_id = ObTabletID(random(1, 10000000));
}

static void make_random_meta_type(backup::ObBackupMetaType &meta_type)
{
  meta_type = BACKUP_SSTABLE_META;
}

static void make_random_logic_id(blocksstable::ObLogicMacroBlockId &logic_id)
{
  logic_id.tablet_id_ = random(1, 10000000);
  blocksstable::ObMacroDataSeq seq;
  seq.macro_data_seq_ = random(1, 10000000);
  logic_id.data_seq_ = seq;
  logic_id.logic_version_ = random(1, 10000000);
}

static void make_random_backup_physical_id(ObBackupPhysicalID &physical_id)
{
  physical_id.ls_id_ = random(1, ObBackupPhysicalID::MAX_BACKUP_LS_ID);
  physical_id.type_ = 0;
  physical_id.turn_id_ = random(1, ObBackupPhysicalID::MAX_BACKUP_TURN_ID);
  physical_id.retry_id_ = random(0, ObBackupPhysicalID::MAX_BACKUP_RETRY_ID);
  physical_id.file_id_ = random(0, ObBackupPhysicalID::MAX_BACKUP_FILE_ID);
  physical_id.reserved_ = 0;
  physical_id.backup_set_id_ = random(1, ObBackupPhysicalID::MAX_BACKUP_SET_ID);
  physical_id.offset_ = random(1, ObBackupPhysicalID::MAX_BACKUP_FILE_SIZE);
  physical_id.length_ = random(1, ObBackupPhysicalID::MAX_BACKUP_BLOCK_SIZE);
}

static void make_random_backup_set_id(int64_t &backup_set_id)
{
  backup_set_id = random(1, 4096);
}
static void make_random_ls_id(share::ObLSID &ls_id)
{
  ls_id = share::ObLSID(random(1, 1024));
}

static void make_random_file_id(int64_t &file_id)
{
  file_id = random(1, 4096);
}

static void make_random_turn_id(int64_t &turn_id)
{
  turn_id = random(1, 1024);
}

static void make_random_retry_id(int64_t &retry_id)
{
  retry_id = random(0, 1023);
}

static void make_random_offset(int64_t &offset)
{
  const int64_t align = DIO_READ_ALIGN_SIZE;
  offset = random(1, 1024) * align;
}
static void make_random_length(int64_t &length)
{
  const int64_t align = DIO_READ_ALIGN_SIZE;
  length = random(1, 1024) * align;
}

static void make_random_table_key(storage::ObITable::TableKey &table_key)
{
  table_key.table_type_ = storage::ObITable::MINOR_SSTABLE;
  make_random_tablet_id(table_key.tablet_id_);
  table_key.scn_range_.start_scn_.convert_for_gts(1);
  table_key.scn_range_.end_scn_.convert_for_gts(100);
}

static void make_random_pair(ObBackupMacroBlockIDPair &pair)
{
  make_random_logic_id(pair.logic_id_);
  make_random_backup_physical_id(pair.physical_id_);
}

static void make_random_cache_key(ObBackupIndexCacheKey &cache_key)
{
  ObBackupBlockDesc block_desc;
  make_random_turn_id(block_desc.turn_id_);
  make_random_retry_id(block_desc.retry_id_);
  block_desc.file_type_ = BACKUP_DATA_FILE;
  make_random_file_id(block_desc.file_id_);
  make_random_offset(block_desc.offset_);
  make_random_length(block_desc.length_);
  cache_key.mode_ = BACKUP_MODE;
  cache_key.tenant_id_ = 1;
  make_random_backup_set_id(cache_key.backup_set_id_);
  make_random_ls_id(cache_key.ls_id_);
  cache_key.backup_data_type_.type_ = share::ObBackupDataType::BACKUP_SYS;
  cache_key.block_desc_ = block_desc;
}

static int make_random_buffer(common::ObIAllocator &allocator,
    blocksstable::ObBufferReader &buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t size = random(1024, 2 * 1024 * 1024);
  if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(size));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      buf[i] = static_cast<char>(i % 256);
    }
    if (OB_SUCC(ret)) {
      buffer.assign(buf, size, size);
    }
  }
  return ret;
}

template <class T>
static bool cmp(const common::ObIArray<T> &lhs_list, const common::ObIArray<T> &rhs_list)
{
  bool bret = true;
  LOG_INFO("compare summary", "lhs_count", lhs_list.count(), "rhs_count", rhs_list.count());
  if (lhs_list.count() != rhs_list.count()) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "count not match", K(lhs_list.count()), K(rhs_list.count()));
  } else {
    for (int64_t i = 0; i < lhs_list.count(); ++i) {
      const T &lhs = lhs_list.at(i);
      const T &rhs = rhs_list.at(i);
      if (lhs == rhs) {
        bret = true;
      } else {
        bret = false;
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "value not match", K(lhs), K(rhs));
        break;
      }
    }
  }
  return bret;
}

}
}
