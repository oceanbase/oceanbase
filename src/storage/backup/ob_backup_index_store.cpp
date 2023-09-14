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

#include "storage/backup/ob_backup_index_store.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/oblog/ob_log_module.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "storage/backup/ob_backup_iterator.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

#include <algorithm>

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

/* ObBackupIndexStoreParam */

ObBackupIndexStoreParam::ObBackupIndexStoreParam()
    : index_level_(),
      tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      ls_id_(),
      is_tenant_level_(false),
      backup_data_type_(),
      turn_id_(),
      retry_id_()
{}

bool ObBackupIndexStoreParam::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && backup_set_id_ > 0 && backup_data_type_.is_valid() && turn_id_ > 0 &&
         retry_id_ >= 0;
}

/* ObIBackupIndexStore */

ObIBackupIndexStore::ObIBackupIndexStore()
    : is_inited_(false),
      is_tenant_level_(false),
      mode_(MAX_MODE),
      index_level_(),
      backup_dest_(),
      tenant_id_(OB_INVALID_ID),
      ls_id_(0),
      turn_id_(-1),
      retry_id_(-1),
      backup_set_desc_(),
      backup_data_type_(),
      index_kv_cache_(NULL),
      trailer_()
{}

ObIBackupIndexStore::~ObIBackupIndexStore()
{}

int ObIBackupIndexStore::pread_file_(const common::ObString &path, const share::ObBackupStorageInfo *storage_info,
    const int64_t offset, const int64_t read_size, char *buf)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  int64_t real_read_size = 0;
  if (OB_UNLIKELY(0 == path.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is invalid", K(ret), K(path));
  } else if (OB_UNLIKELY(read_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read data len is zero", K(path));
  } else if (OB_FAIL(util.read_part_file(path, storage_info, buf, read_size, offset, real_read_size))) {
    LOG_WARN("failed to pread file", K(ret), K(path), K(offset), K(read_size));
  } else if (OB_UNLIKELY(real_read_size != read_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not read enough file", K(ret), K(read_size), K(real_read_size), K(path));
  }
  return ret;
}

int ObIBackupIndexStore::decode_headers_(blocksstable::ObBufferReader &buffer_reader,
    ObBackupCommonHeader &common_header, ObBackupMultiLevelIndexHeader &index_header, int64_t &data_size)
{
  int ret = OB_SUCCESS;
  if (!buffer_reader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(buffer_reader));
  } else if (OB_FAIL(decode_common_header_(buffer_reader, common_header))) {
    LOG_WARN("failed to decode common header", K(ret), K(buffer_reader));
  } else {
    const int64_t pos = buffer_reader.pos();
    if (OB_FAIL(decode_multi_level_index_header_(buffer_reader, index_header))) {
      LOG_WARN("failed to decode multi level index header", K(ret), K(buffer_reader));
    } else {
      data_size = common_header.data_length_ - (buffer_reader.pos() - pos);
    }
  }
  return ret;
}

int ObIBackupIndexStore::decode_common_header_(
    blocksstable::ObBufferReader &buffer_reader, ObBackupCommonHeader &header)
{
  int ret = OB_SUCCESS;
  const ObBackupCommonHeader *common_header = NULL;
  if (OB_UNLIKELY(!buffer_reader.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer reader is not valid", K(ret), K(buffer_reader));
  } else if (OB_FAIL(buffer_reader.get(common_header))) {
    LOG_WARN("failed to get common header from buffer reader", K(ret));
  } else if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common header is null", K(ret));
  } else if (OB_FAIL(common_header->check_valid())) {
    LOG_WARN("common header is not valid", K(ret), K(*common_header));
  } else if (common_header->data_length_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer read not enough", K(ret), K(*common_header), K(buffer_reader));
  } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
    LOG_WARN("failed to check data checksum", K(ret), K(common_header));
  } else {
    header = *common_header;
  }
  return ret;
}

int ObIBackupIndexStore::decode_multi_level_index_header_(
    blocksstable::ObBufferReader &buffer_reader, ObBackupMultiLevelIndexHeader &header)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!buffer_reader.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer reader is not valid", K(ret), K(buffer_reader));
  } else if (OB_FAIL(buffer_reader.read_serialize(header))) {
    LOG_WARN("failed to read serialize", K(ret));
  }
  return ret;
}

template <typename IndexType>
int ObIBackupIndexStore::decode_index_from_block_(
    const int64_t end_pos, blocksstable::ObBufferReader &buffer_reader, common::ObIArray<IndexType> &index_list)
{
  int ret = OB_SUCCESS;
  index_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
    IndexType index;
    if (OB_FAIL(buffer_reader.read_serialize(index))) {
      LOG_WARN("failed to read serialize meta index", K(ret), K(i), K(buffer_reader), K(end_pos));
    } else if (OB_FAIL(index_list.push_back(index))) {
      LOG_WARN("failed to push back", K(ret), K(index));
    }
  }
  return ret;
}

int ObIBackupIndexStore::fetch_block_(const ObBackupFileType &backup_file_type, const int64_t offset,
    const int64_t length, common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  ObBackupIndexCacheKey key;
  const ObBackupIndexCacheValue *pvalue = NULL;
  ObKVCacheHandle handle;
  char *buf = NULL;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(offset), K(length));
  } else if (OB_ISNULL(index_kv_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index cache should not be null", K(ret));
  } else if (OB_FAIL(get_backup_index_cache_key(backup_file_type, offset, length, key))) {
    LOG_WARN("failed to get backup index cache key", K(ret), K(backup_file_type), K(offset), K(length));
  } else if (OB_FAIL(index_kv_cache_->get(key, pvalue, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(do_on_cache_miss_(backup_file_type, offset, length, allocator, buffer_reader))) {
        LOG_WARN("failed to do on cache miss", K(ret), K(backup_file_type), K(offset), K(length));
      } else {
        const int64_t hit_cnt = index_kv_cache_->get_hit_cnt();
        const int64_t miss_cnt = index_kv_cache_->get_miss_cnt();
        LOG_DEBUG("do on cache miss", K(offset), K(length), K(hit_cnt), K(miss_cnt));
      }
    } else {
      LOG_WARN("failed to get value from kv cache", K(ret), K(key));
    }
  } else if (OB_ISNULL(pvalue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache value should not be null", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(pvalue->len())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(*pvalue));
  } else {
    // TODO(yangyi.yyy): refine this method, remove the need of memory copying in 4.1
    MEMCPY(buf, pvalue->buf(), pvalue->len());
    buffer_reader.assign(buf, pvalue->len());
  }
  return ret;
}

int ObIBackupIndexStore::do_on_cache_miss_(const ObBackupFileType &backup_file_type, const int64_t offset,
    const int64_t length, common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  if (OB_FAIL(get_backup_file_path(backup_path))) {
    LOG_WARN("failed to get backup file path", K(ret));
  } else if (OB_FAIL(fetch_index_block_from_dest_(
                 backup_path.get_obstr(), backup_dest_.get_storage_info(), offset, length, allocator, buffer_reader))) {
    LOG_WARN("failed to fetch index block from dest", K(ret), K(backup_path), K(offset), K(length));
  } else if (OB_FAIL(put_block_to_cache_(backup_file_type, offset, length, buffer_reader))) {
    LOG_WARN("failed to put block to cache", K(ret), K(backup_file_type), K(offset), K(length));
  }
  return ret;
}

int ObIBackupIndexStore::put_block_to_cache_(const ObBackupFileType &backup_file_type, const int64_t offset,
    const int64_t length, const blocksstable::ObBufferReader &buffer)
{
  int ret = OB_SUCCESS;
  ObBackupIndexCacheKey key;
  ObBackupIndexCacheValue value(buffer.data(), buffer.capacity());
  if (offset < 0 || length <= 0 || !buffer.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(offset), K(length), K(buffer));
  } else if (OB_ISNULL(index_kv_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index cache should not be null", K(ret));
  } else if (OB_FAIL(get_backup_index_cache_key(backup_file_type, offset, length, key))) {
    LOG_WARN("failed to get backup index cache key", K(ret), K(backup_file_type), K(offset), K(length));
  } else if (OB_FAIL(index_kv_cache_->put(key, value))) {
    LOG_WARN("failed to put to kv cache", K(ret), K(key), K(value));
  }
  return ret;
}

int ObIBackupIndexStore::read_file_trailer_(
    const common::ObString &path, const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_util;
  bool exist = false;
  int64_t file_length = 0;
  char *buf = NULL;
  ObArenaAllocator allocator;
  const int64_t trailer_len = sizeof(ObBackupMultiLevelIndexTrailer);
  if (OB_FAIL(io_util.is_exist(path, storage_info, exist))) {
    LOG_WARN("failed to check file exist", K(ret), K(path), KP(storage_info));
  } else if (OB_UNLIKELY(!exist)) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    LOG_WARN("index file do not exist", K(ret), K(path));
  } else if (OB_FAIL(io_util.get_file_length(path, storage_info, file_length))) {
    LOG_WARN("failed to get file length", K(ret), K(path), KP(storage_info));
  } else if (OB_UNLIKELY(file_length <= trailer_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup index file too small", K(ret), K(file_length), K(trailer_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(trailer_len));
  } else if (OB_FAIL(pread_file_(path, storage_info, file_length - trailer_len, trailer_len, buf))) {
    LOG_WARN("failed to pread file", K(ret), K(path), KP(storage_info), K(file_length), K(trailer_len));
  } else {
    ObBufferReader buffer_reader(buf, trailer_len);
    const ObBackupMultiLevelIndexTrailer *trailer = NULL;
    if (OB_FAIL(buffer_reader.get(trailer))) {
      LOG_WARN("failed to get file trailer", K(ret));
    } else if (OB_ISNULL(trailer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup data file trailer is null", K(ret));
    } else if (OB_FAIL(trailer->check_valid())) {
      LOG_WARN("failed to check is valid", K(ret));
    } else {
      trailer_ = *trailer;
    }
  }
  return ret;
}

int ObIBackupIndexStore::fetch_index_block_from_dest_(const common::ObString &path,
    const share::ObBackupStorageInfo *storage_info, const int64_t offset, const int64_t length,
    common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(offset), K(length));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(length));
  } else if (OB_FAIL(pread_file_(path, storage_info, offset, length, buf))) {
    LOG_WARN("failed to pread file", K(ret), K(path), KP(storage_info), K(offset), K(length));
  } else {
    buffer.assign(buf, length);
  }
  return ret;
}

/* ObBackupMetaIndexStore */

ObBackupMetaIndexStore::ObBackupMetaIndexStore() : ObIBackupIndexStore(), is_sec_meta_(false)
{}

ObBackupMetaIndexStore::~ObBackupMetaIndexStore()
{
  reset();
}

int ObBackupMetaIndexStore::init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
    const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc, const bool is_sec_meta,
    ObBackupIndexKVCache &index_kv_cache)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("index store init twice", K(ret));
  } else if (!param.is_valid() || !backup_dest.is_valid() || !backup_set_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_dest), K(backup_set_desc));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    mode_ = mode;
    is_tenant_level_ = param.is_tenant_level_;
    index_level_ = param.index_level_;
    tenant_id_ = param.tenant_id_;
    ls_id_ = param.ls_id_;
    turn_id_ = param.turn_id_;
    retry_id_ = param.retry_id_;
    backup_set_desc_ = backup_set_desc;
    backup_data_type_ = param.backup_data_type_;
    index_kv_cache_ = &index_kv_cache;
    is_sec_meta_ = is_sec_meta;
    if (OB_FAIL(get_backup_file_path(backup_path))) {
      LOG_WARN("failed to get backup file path", K(ret));
    } else if (OB_FAIL(read_file_trailer_(backup_path.get_obstr(), backup_dest_.get_storage_info()))) {
      LOG_WARN("failed to read file trailer", K(ret), K(backup_path), K(backup_dest_.get_storage_info()));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupMetaIndexStore::get_backup_meta_index(
    const common::ObTabletID &tablet_id, const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  ObBackupMetaKey meta_key;
  meta_key.tablet_id_ = tablet_id;
  meta_key.meta_type_ = meta_type;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta index store do not init", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_meta_index_(meta_key, meta_index))) {
    LOG_WARN("failed to get tablet meta index", K(ret), K_(tenant_id), K_(ls_id), K(tablet_id), K(meta_type));
  }
  return ret;
}

void ObBackupMetaIndexStore::reset()
{
  is_inited_ = false;
}

int ObBackupMetaIndexStore::get_backup_file_path(share::ObBackupPath &backup_path) const
{
  int ret = OB_SUCCESS;
  if (is_tenant_level_) {
    if (OB_FAIL(share::ObBackupPathUtil::get_tenant_meta_index_backup_path(
            backup_dest_, backup_set_desc_, backup_data_type_, turn_id_, retry_id_, is_sec_meta_, backup_path))) {
      LOG_WARN("failed to get tenant meta index file path",
          K(ret),
          K_(backup_dest),
          K_(backup_set_desc),
          K_(backup_data_type),
          K_(turn_id),
          K_(retry_id));
    }
  } else {
    if (OB_FAIL(share::ObBackupPathUtil::get_ls_meta_index_backup_path(backup_dest_,
            backup_set_desc_,
            ls_id_,
            backup_data_type_,
            turn_id_,
            retry_id_,
            is_sec_meta_,
            backup_path))) {
      LOG_WARN("failed to get log stream meta index backup path",
          K(ret),
          K_(backup_dest),
          K_(backup_set_desc),
          K_(ls_id),
          K_(backup_data_type));
    }
  }
  return ret;
}

int ObBackupMetaIndexStore::get_backup_index_cache_key(const ObBackupFileType &backup_file_type, const int64_t offset,
    const int64_t length, ObBackupIndexCacheKey &cache_key) const
{
  int ret = OB_SUCCESS;
  const int64_t file_id = OB_DEFAULT_BACKUP_INDEX_FILE_ID;
  ObBackupBlockDesc block_desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("index store do not init", K(ret));
  } else if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(offset), K(length));
  } else if (OB_FAIL(block_desc.set(turn_id_, retry_id_, backup_file_type, file_id, offset, length))) {
    LOG_WARN("failed to set block desc", K(ret), K_(turn_id), K(backup_file_type), K(offset), K(length));
  } else if (OB_FAIL(cache_key.set(
                 mode_, tenant_id_, backup_set_desc_.backup_set_id_, ls_id_, backup_data_type_, block_desc))) {
    LOG_WARN("failed to set cache key", K(ret));
  }
  return ret;
}

int ObBackupMetaIndexStore::get_tablet_meta_index_(const ObBackupMetaKey &meta_key, ObBackupMetaIndex &output)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObBufferReader buffer_reader;
  int64_t offset = trailer_.last_block_offset_;
  int64_t length = trailer_.last_block_length_;
  int64_t current_level = trailer_.tree_height_;
  ObBackupFileType backup_file_type = BACKUP_FILE_TYPE_MAX;
  if (OB_UNLIKELY(!meta_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(meta_key));
  } else if (trailer_.is_empty_index()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("tablet meta index not exist", K(ret), K_(trailer), K(meta_key));
  } else if (OB_FAIL(meta_key.get_backup_index_file_type(backup_file_type))) {
    LOG_WARN("failed to get backup index file type", K(ret), K(meta_key));
  } else {
    ObBackupMetaIndex index;
    ObArray<ObBackupMetaIndex> index_list;
    ObBackupMetaIndexIndex index_index;
    ObArray<ObBackupMetaIndexIndex> index_index_list;
    while (OB_SUCC(ret)) {
      index_index.reset();
      index_index_list.reset();
      buffer_reader.assign(NULL, 0);
      ObBackupCommonHeader common_header;
      ObBackupMultiLevelIndexHeader index_header;
      int64_t data_size = 0;
      if (OB_FAIL(fetch_block_(backup_file_type, offset, length, allocator, buffer_reader))) {
        LOG_WARN("failed to get block from cache", K(ret), K(backup_file_type), K(offset), K(length));
      } else if (OB_FAIL(decode_headers_(buffer_reader, common_header, index_header, data_size))) {
        LOG_WARN("failed to decode headers", K(ret), K(offset), K(length), K(buffer_reader));
      } else {
        const int64_t end_pos = buffer_reader.pos() + data_size;
        if (OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL == index_header.index_level_) {
          if (OB_FAIL(decode_meta_index_from_buffer_(end_pos, buffer_reader, index_list))) {
            LOG_WARN("failed to decode range index from block", K(ret), K(end_pos), K(buffer_reader));
          } else if (OB_FAIL(find_index_lower_bound_(meta_key, index_list, index))) {
            LOG_WARN("failed to find lower bound", K(ret), K(index_header), K(meta_key), K(index_list));
          } else {
            output = index;
            LOG_DEBUG("find tablet meta index", K(meta_key), K(index));
            break;
          }
        } else {
          if (OB_FAIL(decode_meta_index_index_from_buffer_(end_pos, buffer_reader, index_index_list))) {
            LOG_WARN("failed to decode range index from block", K(ret), K(end_pos), K(buffer_reader));
          } else if (OB_FAIL(find_index_index_lower_bound_(meta_key, index_index_list, index_index))) {
            LOG_WARN("failed to find lower bound", K(ret), K(meta_key), K(index_index_list));
          } else {
            offset = index_index.offset_;
            length = index_index.length_;
            LOG_DEBUG("find tablet meta index index", K(meta_key), K(index_index), K(index_index_list));
            allocator.reuse();
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupMetaIndexStore::find_index_lower_bound_(
    const ObBackupMetaKey &meta_key, const common::ObArray<ObBackupMetaIndex> &index_list, ObBackupMetaIndex &index)
{
  int ret = OB_SUCCESS;
  if (!meta_key.is_valid() || index_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(meta_key), K(index_list));
  } else {
    ObCompareBackupMetaIndexTabletId compare;
    typedef common::ObArray<ObBackupMetaIndex>::const_iterator Iter;
    bool found = false;
    Iter iter = std::lower_bound(index_list.begin(), index_list.end(), meta_key, compare);
    if (iter != index_list.end()) {
      if (iter->meta_key_ != meta_key) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("meta key do not exist", K(meta_key), K(index_list));
      } else {
        index = *iter;
        LOG_DEBUG("found key succeed", K(meta_key), K(index_list), K(index));
      }
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not found entry", K(ret), K(meta_key), K(index_list));
    }
  }
  return ret;
}

int ObBackupMetaIndexStore::find_index_index_lower_bound_(const ObBackupMetaKey &meta_key,
    const common::ObArray<ObBackupMetaIndexIndex> &index_index_list, ObBackupMetaIndexIndex &index_index)
{
  int ret = OB_SUCCESS;
  typedef common::ObArray<ObBackupMetaIndexIndex>::const_iterator Iter;
  if (!meta_key.is_valid() || index_index_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(meta_key), K(index_index_list));
  } else {
    ObCompareBackupMetaIndexIndexTabletId compare;
    Iter iter = std::lower_bound(index_index_list.begin(), index_index_list.end(), meta_key, compare);
    if (iter != index_index_list.end()) {
      index_index = *iter;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("do not find such index", K(ret), K(meta_key), K(index_index_list));
    }
  }
  return ret;
}

int ObBackupMetaIndexStore::decode_meta_index_from_buffer_(
    const int64_t end_pos, blocksstable::ObBufferReader &buffer, common::ObArray<ObBackupMetaIndex> &index_list)
{
  return decode_index_from_block_<ObBackupMetaIndex>(end_pos, buffer, index_list);
}

int ObBackupMetaIndexStore::decode_meta_index_index_from_buffer_(const int64_t end_pos,
    blocksstable::ObBufferReader &buffer, common::ObArray<ObBackupMetaIndexIndex> &index_index_list)
{
  return decode_index_from_block_<ObBackupMetaIndexIndex>(end_pos, buffer, index_index_list);
}

/* ObBackupMacroBlockIndexStore */

ObBackupMacroBlockIndexStore::ObBackupMacroBlockIndexStore() : ObIBackupIndexStore(), backup_set_desc_list_()
{}

ObBackupMacroBlockIndexStore::~ObBackupMacroBlockIndexStore()
{
  reset();
}

int ObBackupMacroBlockIndexStore::init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
    const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
    ObBackupIndexKVCache &index_kv_cache, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("index store init twice", K(ret));
  } else if (!param.is_valid() || !backup_dest.is_valid() || !backup_set_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_dest), K(backup_set_desc));
  } else if (OB_FAIL(fill_backup_set_descs_(param.tenant_id_, backup_set_desc.backup_set_id_, sql_proxy))) {
    LOG_WARN("failed to fill backup set descs", K(ret), K(param), K(backup_set_desc));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    mode_ = mode;
    is_tenant_level_ = param.is_tenant_level_;
    index_level_ = param.index_level_;
    tenant_id_ = param.tenant_id_;
    ls_id_ = param.ls_id_;
    turn_id_ = param.turn_id_;
    retry_id_ = param.retry_id_;
    backup_set_desc_ = backup_set_desc;
    backup_data_type_ = param.backup_data_type_;
    index_kv_cache_ = &index_kv_cache;
    if (OB_FAIL(get_backup_file_path(backup_path))) {
      LOG_WARN("failed to get backup file path", K(ret));
    } else if (OB_FAIL(read_file_trailer_(backup_path.get_obstr(), backup_dest_.get_storage_info()))) {
      LOG_WARN("failed to read file trailer", K(ret), K(backup_path), K(backup_dest_.get_storage_info()));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
    const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
    ObBackupIndexKVCache &index_kv_cache)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("index store init twice", K(ret));
  } else if (!param.is_valid() || !backup_dest.is_valid() || !backup_set_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_dest), K(backup_set_desc));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    mode_ = mode;
    is_tenant_level_ = param.is_tenant_level_;
    index_level_ = param.index_level_;
    tenant_id_ = param.tenant_id_;
    ls_id_ = param.ls_id_;
    turn_id_ = param.turn_id_;
    retry_id_ = param.retry_id_;
    backup_set_desc_ = backup_set_desc;
    backup_data_type_ = param.backup_data_type_;
    index_kv_cache_ = &index_kv_cache;
    if (OB_FAIL(get_backup_file_path(backup_path))) {
      LOG_WARN("failed to get backup file path", K(ret));
    } else if (OB_FAIL(read_file_trailer_(backup_path.get_obstr(), backup_dest_.get_storage_info()))) {
      LOG_WARN("failed to read file trailer", K(ret), K(backup_path));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::get_macro_block_index(
    const blocksstable::ObLogicMacroBlockId &macro_id, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  macro_index.reset();
  ObBackupMacroRangeIndex range_index;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block index store do not init", K(ret), K(macro_id));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(macro_id));
  } else if (OB_FAIL(inner_get_macro_block_range_index_(macro_id, range_index))) {
    LOG_WARN("failed to inner get macro block range index", K(ret), K(macro_id));
  } else if (OB_FAIL(get_macro_block_index_(macro_id, range_index, macro_index))) {
    LOG_WARN("failed to get macro block index", K(ret), K(macro_id), K(range_index));
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::get_macro_range_index(
    const blocksstable::ObLogicMacroBlockId &macro_id, ObBackupMacroRangeIndex &range_index)
{
  int ret = OB_SUCCESS;
  range_index.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block index store do not init", K(ret), K(macro_id));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(macro_id));
  } else if (OB_FAIL(inner_get_macro_block_range_index_(macro_id, range_index))) {
    LOG_WARN("failed to inner get macro block range index", K(ret), K(macro_id));
  }
  return ret;
}

void ObBackupMacroBlockIndexStore::reset()
{
  turn_id_ = -1;
  is_inited_ = false;
}

int ObBackupMacroBlockIndexStore::get_backup_file_path(share::ObBackupPath &backup_path) const
{
  int ret = OB_SUCCESS;
  if (is_tenant_level_) {
    if (OB_FAIL(share::ObBackupPathUtil::get_tenant_macro_range_index_backup_path(
            backup_dest_, backup_set_desc_, backup_data_type_, turn_id_, retry_id_, backup_path))) {
      LOG_WARN("failed to get tenant macro range index file path", K(ret));
    }
  } else {
    if (OB_FAIL(share::ObBackupPathUtil::get_ls_macro_range_index_backup_path(
            backup_dest_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, backup_path))) {
      LOG_WARN("failed to get log stream macro range index backup path", K(ret));
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::get_backup_index_cache_key(const ObBackupFileType &backup_file_type,
    const int64_t offset, const int64_t length, ObBackupIndexCacheKey &cache_key) const
{
  int ret = OB_SUCCESS;
  const int64_t file_id = OB_DEFAULT_BACKUP_INDEX_FILE_ID;
  ObBackupBlockDesc block_desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("index store do not init", K(ret));
  } else if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(offset), K(length));
  } else if (OB_FAIL(block_desc.set(turn_id_, retry_id_, backup_file_type, file_id, offset, length))) {
    LOG_WARN("failed to set block desc", K(ret));
  } else if (OB_FAIL(cache_key.set(
                 mode_, tenant_id_, backup_set_desc_.backup_set_id_, ls_id_, backup_data_type_, block_desc))) {
    LOG_WARN("failed to set cache key", K(ret));
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::inner_get_macro_block_range_index_(
    const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupMacroRangeIndex &output)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObBufferReader buffer_reader;
  int64_t offset = trailer_.last_block_offset_;
  int64_t length = trailer_.last_block_length_;
  int64_t current_level = trailer_.tree_height_;
  const ObBackupFileType &backup_file_type = BACKUP_MACRO_RANGE_INDEX_FILE;
  if (OB_UNLIKELY(!logic_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(logic_id));
  } else if (trailer_.is_empty_index()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("macro block index not exist", K(ret), K_(trailer), K(logic_id));
  } else {
    ObBackupMacroRangeIndex range_index;
    ObArray<ObBackupMacroRangeIndex> index_list;
    ObBackupMacroRangeIndexIndex index_index;
    ObArray<ObBackupMacroRangeIndexIndex> index_index_list;
    int64_t round = 0;
    while (OB_SUCC(ret)) {
      index_index.reset();
      index_index_list.reset();
      buffer_reader.assign(NULL, 0);
      ObBackupCommonHeader common_header;
      ObBackupMultiLevelIndexHeader index_header;
      int64_t data_size = 0;
      if (OB_FAIL(fetch_block_(backup_file_type, offset, length, allocator, buffer_reader))) {
        LOG_WARN("failed to fetch block", K(ret), K(offset), K(length));
      } else if (OB_FAIL(decode_headers_(buffer_reader, common_header, index_header, data_size))) {
        LOG_WARN("failed to decode header", K(ret), K(offset), K(length), K(buffer_reader));
      } else {
        const int64_t end_pos = buffer_reader.pos() + data_size;
        if (OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL == index_header.index_level_) {
          if (OB_FAIL(decode_range_index_from_block_(end_pos, buffer_reader, index_list))) {
            LOG_WARN("failed to decode range index index from block", K(ret), K(end_pos), K(common_header));
          } else if (OB_FAIL(find_index_lower_bound_(logic_id, index_list, range_index))) {
            LOG_WARN("failed to find index lower bound", K(ret), K(logic_id), K(index_list));
          } else {
            output = range_index;
            break;
          }
        } else {
          if (OB_FAIL(decode_range_index_index_from_block_(end_pos, buffer_reader, index_index_list))) {
            LOG_WARN("failed to decode index index from block", K(ret), K(end_pos), K(common_header));
          } else if (OB_FAIL(find_index_index_lower_bound_(logic_id, index_index_list, index_index))) {
            LOG_WARN("failed to find lower bound", K(ret), K(logic_id), K(index_index_list));
          } else {
            offset = index_index.offset_;
            length = index_index.length_;
            allocator.reuse();
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::decode_range_index_from_block_(const int64_t end_pos,
    blocksstable::ObBufferReader &buffer_reader, common::ObArray<ObBackupMacroRangeIndex> &index_list)
{
  return decode_index_from_block_<ObBackupMacroRangeIndex>(end_pos, buffer_reader, index_list);
}

int ObBackupMacroBlockIndexStore::decode_range_index_index_from_block_(const int64_t end_pos,
    blocksstable::ObBufferReader &buffer_reader, common::ObArray<ObBackupMacroRangeIndexIndex> &index_index_list)
{
  return decode_index_from_block_<ObBackupMacroRangeIndexIndex>(end_pos, buffer_reader, index_index_list);
}

int ObBackupMacroBlockIndexStore::find_index_lower_bound_(const blocksstable::ObLogicMacroBlockId &logic_id,
    const common::ObArray<ObBackupMacroRangeIndex> &index_list, ObBackupMacroRangeIndex &index)
{
  int ret = OB_SUCCESS;
  typedef common::ObArray<ObBackupMacroRangeIndex>::const_iterator Iter;
  if (!logic_id.is_valid() || index_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(logic_id), K(index_list));
  } else {
    ObCompareBackupMacroRangeIndexLogicId compare;
    Iter iter = std::lower_bound(index_list.begin(), index_list.end(), logic_id, compare);
    if (iter != index_list.end()) {
      index = *iter;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("do not find such index", K(ret), K(logic_id), K(index_list));
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::find_index_index_lower_bound_(const blocksstable::ObLogicMacroBlockId &logic_id,
    const common::ObArray<ObBackupMacroRangeIndexIndex> &index_index_list, ObBackupMacroRangeIndexIndex &index_index)
{
  int ret = OB_SUCCESS;
  typedef common::ObArray<ObBackupMacroRangeIndexIndex>::const_iterator Iter;
  if (!logic_id.is_valid() || index_index_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(logic_id), K(index_index_list));
  } else {
    ObCompareBackupMacroRangeIndexIndexLogicId compare;
    Iter iter = std::lower_bound(index_index_list.begin(), index_index_list.end(), logic_id, compare);
    if (iter != index_index_list.end()) {
      index_index = *iter;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("do not find such index", K(ret), K(logic_id), K(index_index_list));
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::get_macro_block_backup_path_(
    const ObBackupMacroRangeIndex &range_index, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObBackupSetDesc backup_set_desc;
  if (OB_UNLIKELY(!range_index.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(range_index));
  } else if (OB_FAIL(get_backup_set_desc_(range_index, backup_set_desc))) {
    LOG_WARN("failed to get backup set desc", K(ret), K(range_index));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(backup_dest_,
                 backup_set_desc,
                 range_index.ls_id_,
                 backup_data_type_,
                 range_index.turn_id_,
                 range_index.retry_id_,
                 range_index.file_id_,
                 backup_path))) {
    LOG_WARN("failed to get macro block backup path",
        K(ret),
        K_(backup_dest),
        K_(tenant_id),
        K(backup_set_desc),
        K(range_index));
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::get_backup_set_desc_(
    const ObBackupMacroRangeIndex &range_index, ObBackupSetDesc &backup_set_desc)
{
  int ret = OB_SUCCESS;
  if (!range_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(range_index));
  } else if (backup_set_desc_list_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup set desc should not be empty", K(ret));
  } else {
    bool found = false;
    const int64_t backup_set_id = range_index.backup_set_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_desc_list_.count(); ++i) {
      const ObBackupSetDesc &tmp_desc = backup_set_desc_list_.at(i);
      if (tmp_desc.backup_set_id_ == backup_set_id) {
        backup_set_desc = tmp_desc;
        found = true;
        break;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("do not found backup set desc", K(ret), K_(backup_set_desc_list));
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexStore::get_macro_block_index_(const blocksstable::ObLogicMacroBlockId &macro_id,
    const ObBackupMacroRangeIndex &range_index, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  ObBufferReader buffer_reader;
  ObArenaAllocator allocator;
  ObArray<ObBackupMacroBlockIndex> index_list;
  ObArray<ObBackupIndexBlockDesc> block_desc_list;

  if (OB_UNLIKELY(!macro_id.is_valid() || !range_index.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(macro_id), K(range_index));
  } else if (OB_FAIL(get_macro_block_backup_path_(range_index, backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K(range_index));
  } else if (OB_FAIL(ObIBackupIndexIterator::read_backup_index_block_(backup_path,
                 backup_dest_.get_storage_info(),
                 range_index.offset_,
                 range_index.length_,
                 allocator,
                 buffer_reader))) {
    LOG_WARN("failed to read index block", K(ret), K(backup_path), K(range_index));
  } else if (OB_FAIL(ObIBackupIndexIterator::parse_from_index_blocks_impl_(
                 range_index.offset_, buffer_reader, index_list, block_desc_list))) {
    LOG_WARN("failed to parse from block", K(ret), K(backup_path), K(range_index), K(buffer_reader));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
      if (index_list.at(i).logic_id_ == macro_id) {
        macro_index = index_list.at(i);
        found = true;
        break;
      }
    }
    if (OB_SUCC(ret) && found) {
      LOG_INFO("found macro block index success", K(macro_id), K(range_index), K(macro_id));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no macro block index exist", K(ret), K(macro_id), K(range_index), K(index_list));
    }
  }
#ifdef ERRSIM
  if (macro_id.tablet_id_ == GCONF.errsim_backup_tablet_id) {
    SERVER_EVENT_SYNC_ADD("backup_errsim", "get_macro_block_index",
                         "logic_id", macro_id,
                         "range_index", range_index,
                         "macro_index", macro_index,
                         "backup_path", backup_path,
                         "result", ret);
  }
#endif
  return ret;
}

int ObBackupMacroBlockIndexStore::fill_backup_set_descs_(
    const uint64_t tenant_id, const int64_t backup_set_id, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const bool need_lock = false;
  const int64_t incarnation = OB_START_INCARNATION;
  ObBackupSetFileDesc backup_set_file_desc;
  if (OB_FAIL(share::ObBackupSetFileOperator::get_one_backup_set_file(
          sql_proxy, need_lock, backup_set_id, incarnation, tenant_id, backup_set_file_desc))) {
    LOG_WARN("failed to get backup set desc", K(ret), K(backup_set_id), K(tenant_id), K(backup_set_id));
  } else if (backup_set_file_desc.backup_type_.is_full_backup()) {
    ObBackupSetDesc backup_set_desc;
    backup_set_desc.backup_set_id_ = backup_set_file_desc.backup_set_id_;
    backup_set_desc.backup_type_ = backup_set_file_desc.backup_type_;
    if (OB_FAIL(backup_set_desc_list_.push_back(backup_set_desc))) {
      LOG_WARN("failed to push back", K(ret), K(backup_set_desc));
    }
  } else {
    const int64_t prev_full_backup_set_id = backup_set_file_desc.prev_full_backup_set_id_;
    ObArray<ObBackupSetFileDesc> all_backup_set_list;
    if (OB_FAIL(share::ObBackupSetFileOperator::get_all_backup_set_between(
            sql_proxy, tenant_id, incarnation, prev_full_backup_set_id, backup_set_id, all_backup_set_list))) {
      LOG_WARN(
          "failed to get all backup set between", K(ret), K(tenant_id), K(prev_full_backup_set_id), K(backup_set_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < all_backup_set_list.count(); ++i) {
        const ObBackupSetFileDesc &tmp_file_desc = all_backup_set_list.at(i);
        ObBackupSetDesc backup_set_desc;
        backup_set_desc.backup_set_id_ = tmp_file_desc.backup_set_id_;
        backup_set_desc.backup_type_ = tmp_file_desc.backup_type_;
        if (OB_FAIL(backup_set_desc_list_.push_back(backup_set_desc))) {
          LOG_WARN("failed to push back", K(ret), K(backup_set_desc));
        }
      }
    }
  }
  LOG_INFO("fill backup set descs", K(backup_set_file_desc), K(backup_set_id), K_(backup_set_desc_list));
  return ret;
}

/* ObBackupIndexStoreWrapper */

ObBackupIndexStoreWrapper::ObBackupIndexStoreWrapper()
{}

ObBackupIndexStoreWrapper::~ObBackupIndexStoreWrapper()
{}

int ObBackupIndexStoreWrapper::get_type_by_idx_(const int64_t idx, share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  if (0 == idx) {
    backup_data_type.set_sys_data_backup();
  } else if (1 == idx) {
    backup_data_type.set_minor_data_backup();
  } else if (2 == idx) {
    backup_data_type.set_major_data_backup();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup data type not valid", K(ret), K(backup_data_type));
  }
  return ret;
}

int ObBackupIndexStoreWrapper::get_idx_(const share::ObBackupDataType &backup_data_type, int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (backup_data_type.is_sys_backup()) {
    idx = 0;
  } else if (backup_data_type.is_minor_backup()) {
    idx = 1;
  } else if (backup_data_type.is_major_backup()) {
    idx = 2;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup data type not valid", K(ret), K(backup_data_type));
  }
  return ret;
}

/*ObRestoreMetaIndexStore*/

int ObRestoreMetaIndexStore::init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
    const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc, const bool is_sec_meta,
    const uint64_t data_version, ObBackupIndexKVCache &index_kv_cache)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("index store init twice", K(ret));
  } else if (!param.is_valid() || !backup_dest.is_valid() || !backup_set_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_dest), K(backup_set_desc));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    mode_ = mode;
    is_tenant_level_ = param.is_tenant_level_;
    index_level_ = param.index_level_;
    tenant_id_ = param.tenant_id_;
    ls_id_ = param.ls_id_;
    turn_id_ = param.turn_id_;
    retry_id_ = param.retry_id_;
    backup_set_desc_ = backup_set_desc;
    backup_data_type_ = param.backup_data_type_;
    index_kv_cache_ = &index_kv_cache;
    is_sec_meta_ = is_sec_meta;
    data_version_ = data_version;
    if (OB_FAIL(get_backup_file_path(backup_path))) {
      LOG_WARN("failed to get backup file path", K(ret));
    } else if (OB_FAIL(read_file_trailer_(backup_path.get_obstr(), backup_dest_.get_storage_info()))) {
      LOG_WARN("failed to read file trailer", K(ret), K(backup_path), K(backup_dest_.get_storage_info()));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObRestoreMetaIndexStore::get_backup_file_path(share::ObBackupPath &backup_path) const
{
  int ret = OB_SUCCESS;
  if (is_tenant_level_) {
    if (DATA_VERSION_4_2_0_0 > data_version_) {
      if (OB_FAIL(share::ObBackupPathUtilV_4_1::get_tenant_meta_index_backup_path(
          backup_dest_, backup_data_type_, turn_id_, retry_id_, is_sec_meta_, backup_path))) {
        LOG_WARN("failed to get tenant meta index file path", K(ret), K_(backup_dest), K_(backup_data_type), K_(turn_id));
      }
    } else if (OB_FAIL(share::ObBackupPathUtil::get_tenant_meta_index_backup_path(
        backup_dest_, backup_data_type_, turn_id_, retry_id_, is_sec_meta_, backup_path))) {
      LOG_WARN("failed to get tenant meta index file path", K(ret), K_(backup_dest), K_(backup_data_type),
          K_(turn_id));
    }
  } else {
    if (OB_FAIL(share::ObBackupPathUtil::get_ls_meta_index_backup_path(backup_dest_, ls_id_, backup_data_type_,
        turn_id_, retry_id_, is_sec_meta_, backup_path))) {
      LOG_WARN("failed to get log stream meta index backup path", K(ret), K_(backup_dest), K_(ls_id),
          K_(backup_data_type));
    }
  }
  return ret;
}

/* ObBackupMetaIndexStoreWrapper */

ObBackupMetaIndexStoreWrapper::ObBackupMetaIndexStoreWrapper()
    : ObBackupIndexStoreWrapper(), is_inited_(false), is_sec_meta_(false), store_list_()
{}

ObBackupMetaIndexStoreWrapper::~ObBackupMetaIndexStoreWrapper()
{}

int ObBackupMetaIndexStoreWrapper::init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
    const share::ObBackupDest &backup_dest, const share::ObBackupSetFileDesc &backup_set_info, const bool is_sec_meta,
    const bool init_sys_tablet_index_store, ObBackupIndexKVCache &index_kv_cache)
{
  int ret = OB_SUCCESS;
  share::ObBackupSetDesc backup_set_desc;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("index store init twice", K(ret));
  } else {
    backup_set_desc.backup_set_id_ = backup_set_info.backup_set_id_;
    backup_set_desc.backup_type_ = backup_set_info.backup_type_;
    ObBackupIndexStoreParam share_param = param;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAY_SIZE; ++i) {
      ObBackupDataType backup_data_type;
      ObRestoreMetaIndexStore *store = NULL;
      int64_t retry_id = 0;
      if (OB_FAIL(get_type_by_idx_(i, backup_data_type))) {
        LOG_WARN("failed to get type by idx", K(ret), K(i));
      } else if (backup_data_type.is_sys_backup() && !init_sys_tablet_index_store) {
        continue;
      }

      if (OB_FAIL(ret)) {
      } else if (backup_set_info.tenant_compatible_ < DATA_VERSION_4_2_0_0) {
        // In 4.1.x, sys tablet only has 1 backup turn.
        // minor and major both use the data turn id.
        if (backup_data_type.is_sys_backup() && OB_FALSE_IT(share_param.turn_id_ = 1)) {
        } else if (!backup_data_type.is_sys_backup() && OB_FALSE_IT(share_param.turn_id_ = backup_set_info.data_turn_id_)) {
        }
      } else if (backup_data_type.is_minor_backup() && OB_FALSE_IT(share_param.turn_id_ = backup_set_info.minor_turn_id_)) {
      } else if (backup_data_type.is_major_backup() && OB_FALSE_IT(share_param.turn_id_ = backup_set_info.major_turn_id_)) {
      }

      if (OB_SUCC(ret) && !backup_data_type.is_sys_backup()) {
        if (backup_set_info.tenant_compatible_ < DATA_VERSION_4_2_0_0) {
          if (OB_FAIL(get_tenant_meta_index_retry_id_v_4_1_x_(
              backup_dest, backup_data_type, share_param.turn_id_, is_sec_meta, retry_id))) {
            LOG_WARN("failed to get tenant meta index retry id", K(ret));
          }
        } else if (OB_FAIL(get_tenant_meta_index_retry_id_(
            backup_dest, backup_data_type, share_param.turn_id_, is_sec_meta, retry_id))) {
          LOG_WARN("failed to get tenant meta index retry id", K(ret));
        }

        if (OB_SUCC(ret)) {
          share_param.retry_id_ = retry_id;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_index_store_(backup_data_type, store))) {
        LOG_WARN("failed to get index store", K(ret), K(backup_data_type));
      } else if (OB_ISNULL(store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get index store", K(ret), K(backup_data_type));
      } else {
        share_param.backup_data_type_ = backup_data_type;
        share_param.is_tenant_level_ = backup_data_type.is_sys_backup() ? false : true;
        if (OB_FAIL(store->init(mode,
                                share_param,
                                backup_dest,
                                backup_set_desc,
                                is_sec_meta,
                                backup_set_info.tenant_compatible_,
                                index_kv_cache))) {
          LOG_WARN("failed to init store", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupMetaIndexStoreWrapper::get_backup_meta_index(const share::ObBackupDataType &backup_data_type,
    const common::ObTabletID &tablet_id, const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  ObRestoreMetaIndexStore *index_store = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("index store not init", K(ret));
  } else if (OB_FAIL(get_index_store_(backup_data_type, index_store))) {
    LOG_WARN("failed to get index store", K(ret), K(backup_data_type));
  } else if (OB_ISNULL(index_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index store should not be null", K(ret));
  } else if (OB_FAIL(index_store->get_backup_meta_index(tablet_id, meta_type, meta_index))) {
    LOG_WARN("failed to get macro block index", K(ret));
  } else {
    LOG_INFO("get meta index", K(tablet_id), K(meta_index));
  }
  return ret;
}

int ObBackupMetaIndexStoreWrapper::get_index_store_(
    const share::ObBackupDataType &backup_data_type, ObRestoreMetaIndexStore *&index_store)
{
  int ret = OB_SUCCESS;
  index_store = NULL;
  int64_t idx = 0;
  if (OB_FAIL(get_idx_(backup_data_type, idx))) {
    LOG_WARN("failed to get idx", K(ret), K(backup_data_type));
  } else if (idx < 0 || idx >= ARRAY_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx is not valid", K(ret), K(idx));
  } else {
    index_store = &store_list_[idx];
  }
  return ret;
}

int ObBackupMetaIndexStoreWrapper::get_tenant_meta_index_retry_id_(
    const share::ObBackupDest &backup_dest, const share::ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t is_sec_meta, int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  ObBackupTenantIndexRetryIDGetter retry_id_getter;
  const bool is_restore = true;
  const bool is_macro_index = false;
  if (!backup_dest.is_valid() || !backup_data_type.is_valid() || turn_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_data_type), K(turn_id));
  } else if (OB_FAIL(retry_id_getter.init(backup_dest, backup_data_type,
      turn_id, is_restore, is_macro_index, is_sec_meta))) {
    LOG_WARN("failed to init retry id getter", K(ret), K(backup_dest), K(backup_data_type), K(turn_id));
  } else if (OB_FAIL(retry_id_getter.get_max_retry_id(retry_id))) {
    LOG_WARN("failed to get max retry id", K(ret));
  }
  return ret;
}

int ObBackupMetaIndexStoreWrapper::get_tenant_meta_index_retry_id_v_4_1_x_(
    const share::ObBackupDest &backup_dest, const share::ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t is_sec_meta, int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  ObBackupTenantIndexRetryIDGetter retry_id_getter;
  const bool is_restore = true;
  const bool is_macro_index = false;
  if (!backup_dest.is_valid() || !backup_data_type.is_valid() || turn_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_data_type), K(turn_id));
  } else if (OB_FAIL(retry_id_getter.init(backup_dest, backup_data_type,
      turn_id, is_restore, is_macro_index, is_sec_meta))) {
    LOG_WARN("failed to init retry id getter", K(ret), K(backup_dest), K(backup_data_type), K(turn_id));
  } else if (OB_FAIL(retry_id_getter.get_max_retry_id_v_4_1_x(retry_id))) {
    LOG_WARN("failed to get max retry id", K(ret));
  }
  return ret;
}

/* ObBackupMacroBlockIndexStoreWrapper */

ObBackupMacroBlockIndexStoreWrapper::ObBackupMacroBlockIndexStoreWrapper()
    : ObBackupIndexStoreWrapper(), is_inited_(false), store_list_()
{}

ObBackupMacroBlockIndexStoreWrapper::~ObBackupMacroBlockIndexStoreWrapper()
{}

int ObBackupMacroBlockIndexStoreWrapper::init(const ObBackupRestoreMode &mode, const ObBackupIndexStoreParam &param,
    const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
    ObBackupIndexKVCache &index_kv_cache)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("index store init twice", K(ret));
  } else {
    ObBackupIndexStoreParam share_param = param;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAY_SIZE; ++i) {
      ObBackupDataType backup_data_type;
      ObBackupMacroBlockIndexStore *store = NULL;
      if (OB_FAIL(get_type_by_idx_(i, backup_data_type))) {
        LOG_WARN("failed to get type by idx", K(ret), K(i));
      } else if (OB_FAIL(get_index_store_(backup_data_type, store))) {
        LOG_WARN("failed to get index store", K(ret), K(backup_data_type));
      } else if (OB_ISNULL(store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get index store", K(ret), K(backup_data_type));
      } else {
        share_param.backup_data_type_ = backup_data_type;
        if (OB_FAIL(store->init(mode, share_param, backup_dest, backup_set_desc, index_kv_cache))) {
          LOG_WARN("failed to init store", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupMacroBlockIndexStoreWrapper::get_macro_block_index(const share::ObBackupDataType &backup_data_type,
    const blocksstable::ObLogicMacroBlockId &macro_id, ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  ObBackupMacroBlockIndexStore *index_store = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("index store not init", K(ret));
  } else if (OB_FAIL(get_index_store_(backup_data_type, index_store))) {
    LOG_WARN("failed to get index store", K(ret), K(backup_data_type));
  } else if (OB_ISNULL(index_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index store should not be null", K(ret));
  } else if (OB_FAIL(index_store->get_macro_block_index(macro_id, macro_index))) {
    LOG_WARN("failed to get macro block index", K(ret));
  } else {
    LOG_INFO("get macro block index", K(backup_data_type), K(macro_id), K(macro_index));
  }
  return ret;
}

int ObBackupMacroBlockIndexStoreWrapper::get_index_store_(
    const share::ObBackupDataType &backup_data_type, ObBackupMacroBlockIndexStore *&index_store)
{
  int ret = OB_SUCCESS;
  index_store = NULL;
  int64_t idx = 0;
  if (OB_FAIL(get_idx_(backup_data_type, idx))) {
    LOG_WARN("failed to get idx", K(ret), K(backup_data_type));
  } else if (idx < 0 || idx >= ARRAY_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx is not valid", K(ret), K(idx));
  } else {
    index_store = &store_list_[idx];
  }
  return ret;
}

/* ObBackupTenantIndexRetryIDGetter */

ObBackupTenantIndexRetryIDGetter::ObBackupTenantIndexRetryIDGetter()
  : is_inited_(false),
    backup_dest_(),
    backup_set_desc_(),
    backup_data_type_(),
    turn_id_(-1),
    is_restore_(false),
    is_macro_index_(false),
    is_sec_meta_(false)
{}

ObBackupTenantIndexRetryIDGetter::~ObBackupTenantIndexRetryIDGetter()
{}

// for backup
int ObBackupTenantIndexRetryIDGetter::init(const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
    const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const bool is_restore,
    const bool is_macro_index, const bool is_sec_meta)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("retry id getter init twice", K(ret));
  } else if (is_restore || (is_macro_index && is_sec_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(is_restore), K(is_macro_index), K(is_sec_meta));
  } else if (!backup_dest.is_valid() || !backup_set_desc.is_valid() || turn_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_set_desc), K(turn_id));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    backup_set_desc_ = backup_set_desc;
    backup_data_type_ = backup_data_type;
    turn_id_ = turn_id;
    is_restore_ = is_restore;
    is_macro_index_ = is_macro_index;
    is_sec_meta_ = is_sec_meta;
    is_inited_ = true;
  }
  return ret;
}

// for restore
int ObBackupTenantIndexRetryIDGetter::init(const share::ObBackupDest &backup_dest, const share::ObBackupDataType &backup_data_type,
    const int64_t turn_id, const bool is_restore, const bool is_macro_index, const bool is_sec_meta)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("retry id getter init twice", K(ret));
  } else if (!is_restore || (is_macro_index && is_sec_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(is_restore), K(is_macro_index), K(is_sec_meta));
  } else if (!backup_dest.is_valid() || turn_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(turn_id));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    backup_data_type_ = backup_data_type;
    turn_id_ = turn_id;
    is_restore_ = is_restore;
    is_macro_index_ = is_macro_index;
    is_sec_meta_ = is_sec_meta;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTenantIndexRetryIDGetter::get_max_retry_id(int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  retry_id = 0;
  ObArray<int64_t> id_list;
  const char *file_name_prefix = NULL;
  share::ObBackupPath backup_path;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("retry id getter not init", K(ret));
  } else if (OB_FAIL(get_tenant_index_file_name_(file_name_prefix))) {
    LOG_WARN("failed to get tenant index file name", K(ret));
  } else if (OB_FAIL(get_ls_info_data_info_dir_path_(backup_path))) {
    LOG_WARN("failed to get dir path", K(ret));
  } else if (OB_FAIL(list_files_(backup_path, backup_dest_.get_storage_info(), file_name_prefix, id_list))) {
    LOG_WARN("failed to list files", K(ret), K(backup_path), K(backup_dest_), K(file_name_prefix));
  } else if (OB_FAIL(find_largest_id_(id_list, retry_id))) {
    LOG_WARN("failed to find largest id", K(ret), K(id_list));
  } else {
    LOG_INFO("get max tenant index retry id", K_(backup_dest), K_(backup_data_type),
        K_(turn_id), K_(is_restore), K_(is_macro_index), K_(is_sec_meta), K(retry_id));
  }
  return ret;
}

int ObBackupTenantIndexRetryIDGetter::get_max_retry_id_v_4_1_x(int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  retry_id = 0;
  ObArray<int64_t> id_list;
  const char *file_name_prefix = NULL;
  share::ObBackupPath backup_path;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("retry id getter not init", K(ret));
  } else if (OB_FAIL(get_tenant_index_file_name_(file_name_prefix))) {
    LOG_WARN("failed to get tenant index file name", K(ret));
  } else if (is_restore_ &&
      OB_FAIL(share::ObBackupPathUtilV_4_1::get_ls_info_data_info_dir_path(backup_dest_,
                                                                              turn_id_,
                                                                              backup_path))) {
    LOG_WARN("failed to get ls info data info dir path",
             K(ret), K_(backup_dest), K_(backup_data_type), K_(backup_set_desc), K_(turn_id));
  } else if (OB_FAIL(list_files_(backup_path, backup_dest_.get_storage_info(), file_name_prefix, id_list))) {
    LOG_WARN("failed to list files", K(ret), K(backup_path), K(backup_dest_), K(file_name_prefix));
  } else if (OB_FAIL(find_largest_id_(id_list, retry_id))) {
    LOG_WARN("failed to find largest id", K(ret), K(id_list));
  } else {
    LOG_INFO("get max tenant index retry id", K_(backup_dest), K_(backup_data_type),
        K_(turn_id), K_(is_restore), K_(is_macro_index), K_(is_sec_meta), K(retry_id));
  }
  return ret;
}

int ObBackupTenantIndexRetryIDGetter::get_ls_info_data_info_dir_path_(ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  if (is_restore_) {
    if (OB_FAIL(share::ObBackupPathUtil::get_ls_info_data_info_dir_path(
        backup_dest_, backup_data_type_, turn_id_, backup_path))) {
      LOG_WARN("failed to get ls info data info dir path",
          K(ret), K_(backup_dest), K_(backup_data_type), K_(backup_set_desc), K_(turn_id));
    }
  } else {
    if (OB_FAIL(share::ObBackupPathUtil::get_ls_info_data_info_dir_path(
        backup_dest_, backup_set_desc_, backup_data_type_, turn_id_, backup_path))) {
      LOG_WARN("failed to get ls info data info dir path",
          K(ret), K_(backup_dest), K_(backup_set_desc), K_(backup_data_type), K_(turn_id));
    }
  }
  return ret;
}

int ObBackupTenantIndexRetryIDGetter::get_tenant_index_file_name_(const char *&file_name)
{
  int ret = OB_SUCCESS;
  file_name = NULL;
  if (backup_data_type_.is_minor_backup()) {
    if (is_macro_index_) {
      file_name = OB_STR_TENANT_MINOR_MACRO_INDEX;
    } else {
      if (is_sec_meta_) {
        file_name = OB_STR_TENANT_MINOR_SEC_META_INDEX;
      } else {
        file_name = OB_STR_TENANT_MINOR_META_INDEX;
      }
    }
  } else if (backup_data_type_.is_major_backup()) {
    if (is_macro_index_) {
      file_name = OB_STR_TENANT_MAJOR_MACRO_INDEX;
    } else {
      if (is_sec_meta_) {
        file_name = OB_STR_TENANT_MAJOR_SEC_META_INDEX;
      } else {
        file_name = OB_STR_TENANT_MAJOR_META_INDEX;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup data type not correct", K(ret), K_(backup_data_type));
  }
  return ret;
}

int ObBackupTenantIndexRetryIDGetter::list_files_(const ObBackupPath &backup_path, const ObBackupStorageInfo *storage_info,
    const char *file_name_prefix, common::ObArray<int64_t> &id_list)
{
  int ret = OB_SUCCESS;
  id_list.reset();
  ObBackupIoAdapter util;
  ObBackupDataFileRangeOp file_range_op(file_name_prefix);
  if (backup_path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_path));
  } else if (OB_FAIL(util.list_files(backup_path.get_obstr(), storage_info, file_range_op))) {
    LOG_WARN("failed to list files", K(ret), K(backup_path), K_(backup_dest));
  } else if (OB_FAIL(file_range_op.get_file_list(id_list))) {
    LOG_WARN("failed to get file list", K(ret));
  } else if (id_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("id list should not be empty", K(ret), K(backup_path), K(file_name_prefix));
  }
  return ret;
}

int ObBackupTenantIndexRetryIDGetter::find_largest_id_(const common::ObIArray<int64_t> &id_list, int64_t &largest_id)
{
  int ret = OB_SUCCESS;
  largest_id = -1;
  int64_t tmp_largest_id = -1;
  FOREACH_CNT_X(tmp_id, id_list, OB_SUCC(ret)) {
    if (OB_ISNULL(tmp_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tmp id should not be null", K(ret));
    } else if (*tmp_id > tmp_largest_id) {
      tmp_largest_id = *tmp_id;
    }
  }
  if (OB_SUCC(ret)) {
    if (-1 == tmp_largest_id) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no entry exist", K(ret), K(id_list));
    } else {
      largest_id = tmp_largest_id;
      LOG_INFO("get largest id", K_(backup_dest), K_(backup_data_type), K_(turn_id), K(largest_id));
    }
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
