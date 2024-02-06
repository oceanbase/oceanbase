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

#include "ob_tablet_block_aggregated_info.h"
#include "storage/ob_super_block_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#include "storage/slog_ckpt/ob_linked_macro_block_reader.h"
#include "storage/tablet/ob_tablet_block_header.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
/**
 * ---------------------------------------ObSharedBlockInfo----------------------------------------
 */
int ObSharedBlockInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(shared_macro_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize shared macro id", K(ret), K(shared_macro_id_), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, occupy_size_))) {
    LOG_WARN("fail to serialize occupy size", K(ret), K(occupy_size_), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedBlockInfo::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(shared_macro_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize shared macro id", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &occupy_size_))) {
    LOG_WARN("fail to deserialize occupy size", K(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObSharedBlockInfo::get_serialize_size() const
{
  return shared_macro_id_.get_serialize_size() + serialization::encoded_length_i64(occupy_size_);
}

/**
 * ---------------------------------------ObBlockInfoSet----------------------------------------
 */
int ObBlockInfoSet::init(
      const int64_t meta_bucket_num,
      const int64_t data_bucket_num,
      const int64_t shared_meta_bucket_num,
      const int64_t shared_data_bucket_num)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_block_info_set_.create(meta_bucket_num, "ObBlockInfoSet", "ObBlockSetNode", MTL_ID()))) {
    LOG_WARN("fail to create meta block id set", K(ret), K(meta_bucket_num));
  } else if (OB_FAIL(data_block_info_set_.create(data_bucket_num, "ObBlockInfoSet", "ObBlockSetNode", MTL_ID()))) {
    LOG_WARN("fail to create data block id set", K(ret), K(data_bucket_num));
  } else if (OB_FAIL(shared_meta_block_info_set_.create(shared_meta_bucket_num, "ObBlockInfoSet", "ObBlockSetNode", MTL_ID()))) {
    LOG_WARN("fail to create shared meta block id set", K(ret), K(shared_meta_bucket_num));
  } else if (OB_FAIL(shared_data_block_info_map_.create(shared_data_bucket_num, "ObBlockInfoMap", "ObBlockMapNode", MTL_ID()))) {
    LOG_WARN("fail to create shared data block id set", K(ret), K(shared_meta_bucket_num));
  }
  return ret;
}
/**
 * ---------------------------------------ObBlockInfoArray----------------------------------------
 */
template <typename T>
ObBlockInfoArray<T>::ObBlockInfoArray()
  : cnt_(0), arr_(nullptr), capacity_(0), is_inited_(false)
{
}

template <typename T>
ObBlockInfoArray<T>::~ObBlockInfoArray()
{
  reset();
}

template <typename T>
void ObBlockInfoArray<T>::reset()
{
  cnt_ = 0;
  capacity_ = 0;
  arr_ = nullptr;
  is_inited_ = false;
}

template <typename T>
int ObBlockInfoArray<T>::init(const int64_t cnt, ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBlockInfoArray has been inited", K(ret));
  } else if (0 == cnt) {
    // no macro id
    arr_ = nullptr;
  } else if (OB_ISNULL(arr_ = reinterpret_cast<T *>(allocator.alloc(sizeof(T) * cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(T) * cnt));
  }
  if (OB_SUCC(ret)) {
    cnt_ = cnt;
    capacity_ = cnt;
    is_inited_ = true;
  }
  return ret;
}

template <typename T>
int ObBlockInfoArray<T>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockInfoArray hasn't been inited", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, cnt_))) {
    LOG_WARN("fail to encode count", K(ret), K_(cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
    if (OB_UNLIKELY(!arr_[i].is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block id is invalid", K(ret), K(i), K(arr_[i]));
    } else if (OB_FAIL(arr_[i].serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize macro block id", K(ret), K(i), KP(buf), K(buf_len), K(pos));
    }
  }
  return ret;
}

template <typename T>
int ObBlockInfoArray<T>::deserialize(ObArenaAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBlockInfoArray has been inited", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &cnt_))) {
    LOG_WARN("fail to decode count", K(ret), K(data_len), K(pos));
  } else if (0 == cnt_) {
    // no macro id
  } else if (OB_UNLIKELY(cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count shouldn't be less than 0", K(ret), K_(cnt));
  } else {
    if (OB_ISNULL(arr_ = static_cast<T *>(allocator.alloc(cnt_ * sizeof(T))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for macro id array", K(ret), K_(cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
      if (OB_FAIL(arr_[i].deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize macro block id", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(!arr_[i].is_valid())) {
        LOG_WARN("deserialized macro id is invalid", K(ret), K(arr_[i]));
      }
    }
  }
  if (OB_FAIL(ret) && nullptr != arr_) {
    allocator.free(arr_);
    reset();
  } else if (OB_SUCC(ret)) {
    is_inited_ = true;
    capacity_ = cnt_;
  }
  return ret;
}

template <typename T>
int64_t ObBlockInfoArray<T>::get_serialize_size() const
{
  T block_info;
  return serialization::encoded_length_i64(cnt_) + block_info.get_serialize_size() * cnt_;
}

template <typename T>
int64_t ObBlockInfoArray<T>::get_deep_copy_size() const
{
  return sizeof(T) * cnt_;
}

template <typename T>
int ObBlockInfoArray<T>::deep_copy(char *buf, const int64_t buf_len, int64_t &pos, ObBlockInfoArray &dest_obj) const
{
  int ret = OB_SUCCESS;
  const int64_t memory_size = get_deep_copy_size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockInfoArray hasn't been inited", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0 || buf_len - pos < memory_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos), K(memory_size));
  } else if (OB_NOT_NULL(arr_) && 0 != cnt_) {
    dest_obj.arr_ = reinterpret_cast<T *>(buf + pos);
    MEMCPY(dest_obj.arr_, arr_, sizeof(T) * cnt_);
  } else {
    dest_obj.arr_ = nullptr;
  }
  if (OB_SUCC(ret)) {
    dest_obj.cnt_ = cnt_;
    dest_obj.capacity_ = capacity_;
    pos += memory_size;
    dest_obj.is_inited_ = is_inited_;
  }
  return ret;
}

/**
 * ---------------------------------------ObTabletMacroInfo----------------------------------------
 */
ObTabletMacroInfo::ObTabletMacroInfo()
  : entry_block_(ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK),
    meta_block_info_arr_(), data_block_info_arr_(), shared_meta_block_info_arr_(),
    shared_data_block_info_arr_(), is_inited_(false)
{
}

ObTabletMacroInfo::~ObTabletMacroInfo()
{
  reset();
}

void ObTabletMacroInfo::reset()
{
  entry_block_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  meta_block_info_arr_.reset();
  data_block_info_arr_.reset();
  shared_meta_block_info_arr_.reset();
  shared_data_block_info_arr_.reset();
  is_inited_ = false;
}

int ObTabletMacroInfo::init(
    ObArenaAllocator &allocator,
    ObBlockInfoSet &info_set,
    ObLinkedMacroBlockItemWriter &linked_writer)
{
  int ret = OB_SUCCESS;
  ObBlockInfoSet::TabletMacroSet &meta_block_info_set = info_set.meta_block_info_set_;
  ObBlockInfoSet::TabletMacroSet &data_block_info_set = info_set.data_block_info_set_;
  ObBlockInfoSet::TabletMacroSet &shared_meta_block_info_set = info_set.shared_meta_block_info_set_;
  ObBlockInfoSet::TabletMacroMap &shared_data_block_info_map = info_set.shared_data_block_info_map_;
  int64_t total_macro_cnt = meta_block_info_set.size()
                            + data_block_info_set.size()
                            + shared_meta_block_info_set.size()
                            + shared_data_block_info_map.size();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletMacroInfo has been inited", K(ret));
  } else if (OB_FAIL(meta_block_info_arr_.init(meta_block_info_set.size(), allocator))) {
    LOG_WARN("fail to init meta block id arr", K(ret));
  } else if (OB_FAIL(data_block_info_arr_.init(data_block_info_set.size(), allocator))) {
    LOG_WARN("fail to init data block id arr", K(ret));
  } else if (OB_FAIL(shared_meta_block_info_arr_.init(shared_meta_block_info_set.size(), allocator))) {
    LOG_WARN("fail to init shared meta block info arr", K(ret));
  } else if (OB_FAIL(shared_data_block_info_arr_.init(shared_data_block_info_map.size(), allocator))) {
    LOG_WARN("fail to init shared data block info arr", K(ret));
  } else if (OB_FAIL(construct_block_id_arr(meta_block_info_set, meta_block_info_arr_))) {
    LOG_WARN("fail to construct meta block id arr", K(ret));
  } else if (OB_FAIL(construct_block_id_arr(data_block_info_set, data_block_info_arr_))) {
    LOG_WARN("fail to construct data block id arr", K(ret));
  } else if (OB_FAIL(construct_block_id_arr(shared_meta_block_info_set, shared_meta_block_info_arr_))) {
    LOG_WARN("fail to construct shared meta block id arr", K(ret));
  } else if (OB_FAIL(construct_block_info_arr(shared_data_block_info_map, shared_data_block_info_arr_))) {
    LOG_WARN("fail to construct shared data block info arr", K(ret));
  } else if (ID_COUNT_THRESHOLD < total_macro_cnt && OB_FAIL(persist_macro_ids(allocator, linked_writer))) {
    LOG_WARN("fail to persist macro ids", K(ret));
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else if (!is_inited_) {
    reset();
  }
  return ret;
}

int ObTabletMacroInfo::construct_block_id_arr(
    ObBlockInfoSet::TabletMacroSet &id_set,
    ObBlockInfoArray<MacroBlockId> &block_id_arr)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  for (ObBlockInfoSet::SetIterator iter = id_set.begin(); OB_SUCC(ret) && iter != id_set.end(); ++iter) {
    const MacroBlockId &macro_id = iter->first;
    if (OB_UNLIKELY(cnt >= block_id_arr.cnt_ || !macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected macro_cnt/macro_id", K(ret), K(macro_id), K(cnt), K(block_id_arr));
    } else {
      block_id_arr.arr_[cnt] = macro_id;
      cnt++;
    }
  }
  return ret;
}

int ObTabletMacroInfo::construct_block_info_arr(
    ObBlockInfoSet::TabletMacroMap &block_info_map,
    ObBlockInfoArray<ObSharedBlockInfo> &block_info_arr)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  for (ObBlockInfoSet::MapIterator iter = block_info_map.begin(); OB_SUCC(ret) && iter != block_info_map.end(); ++iter) {
    const MacroBlockId &macro_id = iter->first;
    const int64_t occupy_size = iter->second;
    if (OB_UNLIKELY(cnt >= block_info_arr.cnt_ || !macro_id.is_valid() || occupy_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected macro_cnt/macro_id/occupy_size", K(ret), K(macro_id), K(cnt), K(occupy_size));
    } else {
      new (&block_info_arr.arr_[cnt]) ObSharedBlockInfo(macro_id, occupy_size);
      cnt++;
    }
  }
  return ret;
}

int ObTabletMacroInfo::persist_macro_ids(
    ObArenaAllocator &allocator,
    ObLinkedMacroBlockItemWriter &linked_writer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(linked_writer.init(false /*whether need addr*/))) {
    LOG_WARN("fail to init linked writer", K(ret));
  } else if (OB_FAIL(do_flush_ids(ObTabletMacroType::META_BLOCK, meta_block_info_arr_, allocator, linked_writer))) {
    LOG_WARN("fail to persist meta block ids", K(ret));
  } else if (OB_FAIL(do_flush_ids(ObTabletMacroType::DATA_BLOCK, data_block_info_arr_, allocator, linked_writer))) {
    LOG_WARN("fail to persist data block ids", K(ret));
  } else if (OB_FAIL(do_flush_ids(ObTabletMacroType::SHARED_META_BLOCK, shared_meta_block_info_arr_, allocator, linked_writer))) {
    LOG_WARN("fail to persist shared meta block ids", K(ret));
  } else if (OB_FAIL(do_flush_ids(shared_data_block_info_arr_, allocator, linked_writer))) {
    LOG_WARN("fail to persist shared data block infos", K(ret));
  } else if (OB_FAIL(linked_writer.close())) {
    LOG_WARN("fail to close linked writer", K(ret));
  } else if (OB_FAIL(linked_writer.get_entry_block(entry_block_))) {
    LOG_WARN("fail to get entry block", K(ret));
  }
  return ret;
}

int ObTabletMacroInfo::do_flush_ids(
    const ObTabletMacroType macro_type,
    ObBlockInfoArray<blocksstable::MacroBlockId> &block_id_arr,
    ObArenaAllocator &allocator,
    ObLinkedMacroBlockItemWriter &linked_writer)
{
  int ret = OB_SUCCESS;
  MacroBlockId dummy_id;
  char *buf = nullptr;
  const int64_t buf_len = serialization::encoded_length_i16(static_cast<int16_t>(macro_type))
                          + block_id_arr.get_serialize_size();
  int64_t pos = 0;
  if (OB_ISNULL(buf = (char *)(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for flush buf", K(ret), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, static_cast<int16_t>(macro_type)))) {
    LOG_WARN("fail to serialize macro type", K(ret), K(macro_type));
  } else if (OB_FAIL(block_id_arr.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize block id arr", K(ret), K(block_id_arr));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(linked_writer.write_item(buf, buf_len))) {
    LOG_WARN("fail to write linked item", K(ret), KP(buf), K(buf_len));
  }
  return ret;
}

int ObTabletMacroInfo::do_flush_ids(
    ObBlockInfoArray<ObSharedBlockInfo> &block_info_arr,
    ObArenaAllocator &allocator,
    ObLinkedMacroBlockItemWriter &linked_writer)
{
  int ret = OB_SUCCESS;
  ObSharedBlockInfo dummy_info;
  char *buf = nullptr;
  int16_t dummy_type = 0;
  const int64_t buf_len = serialization::encoded_length_i16(dummy_type)
                          + block_info_arr.get_serialize_size();
  int64_t pos = 0;
  if (OB_ISNULL(buf = (char *)(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for flush buf", K(ret), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, static_cast<int16_t>(ObTabletMacroType::SHARED_DATA_BLOCK)))) {
    LOG_WARN("fail to serialize macro type", K(ret));
  } else if (OB_FAIL(block_info_arr.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize block info arr", K(ret), K(block_info_arr));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(linked_writer.write_item(buf, buf_len))) {
    LOG_WARN("fail to write linked item", K(ret), KP(buf), K(buf_len));
  }
  return ret;
}

int ObTabletMacroInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObSecondaryMetaHeader meta_header;
  const int64_t header_size = meta_header.get_serialize_size();
  const int64_t total_size = get_serialize_size();
  int64_t meta_pos = pos + header_size;
  int64_t header_pos = pos;
  int64_t version = TABLET_MACRO_INFO_VERSION;
  int64_t size = get_serialize_size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockInfoArray hasn't been inited", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || buf_len - pos < total_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, meta_pos, version))) {
    LOG_WARN("fail to serialize version", K(ret), KP(buf), K(buf_len), K(version));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, meta_pos, size))) {
    LOG_WARN("fail to serialize size", K(ret), KP(buf), K(buf_len), K(size));
  } else if (OB_FAIL(entry_block_.serialize(buf, buf_len, meta_pos))) {
    LOG_WARN("fail to serialize entry block", K(ret), KP(buf), K(buf_len), K(entry_block_));
  } else if (OB_FAIL(meta_block_info_arr_.serialize(buf, buf_len, meta_pos))) {
    LOG_WARN("fail to serialize meta block id arr", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(data_block_info_arr_.serialize(buf, buf_len, meta_pos))) {
    LOG_WARN("fail to serialize data block id arr", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(shared_meta_block_info_arr_.serialize(buf, buf_len, meta_pos))) {
    LOG_WARN("fail to serialize shared meta block id arr", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(shared_data_block_info_arr_.serialize(buf, buf_len, meta_pos))) {
    LOG_WARN("fail to serialize shared data block id arr", K(ret), KP(buf), K(buf_len));
  } else {
    meta_header.checksum_ = ob_crc64(buf + pos + header_size, total_size - header_size);
    meta_header.payload_size_ = total_size - header_size;
    if (OB_FAIL(meta_header.serialize(buf, buf_len, header_pos))) {
      LOG_WARN("fail to serialize secondary meta header", K(ret), K(meta_header));
    } else {
      pos = meta_pos;
    }
  }
  return ret;
}

int ObTabletMacroInfo::deserialize(ObArenaAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObSecondaryMetaHeader meta_header;
  int32_t crc = 0;
  int64_t new_pos = pos;
  int64_t version = 0;
  int64_t size = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletMacroInfo has been inited", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(meta_header.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize secondary meta header", K(ret), KP(buf), K(data_len), K(new_pos));
  } else if (FALSE_IT(crc = ob_crc64(buf + new_pos, meta_header.payload_size_))) {
  } else if (OB_UNLIKELY(crc != meta_header.checksum_)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("tablet macro info's checksum doesn't match", K(ret), K(meta_header), K(crc));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &version))) {
    LOG_WARN("fail to deserialize version", K(ret), KP(buf), K(data_len));
  } else if (OB_UNLIKELY(TABLET_MACRO_INFO_VERSION != version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tablet macro info's version doesn't match", K(ret), K(version));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &size))) {
    LOG_WARN("fail to deserialize size", K(ret), KP(buf), K(data_len));
  } else if (new_pos - pos < size && OB_FAIL(entry_block_.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize entry block", K(ret), KP(buf), K(data_len));
  } else if (new_pos - pos < size && OB_FAIL(meta_block_info_arr_.deserialize(allocator, buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize meta block id array", K(ret), KP(buf), K(data_len));
  } else if (new_pos - pos < size && OB_FAIL(data_block_info_arr_.deserialize(allocator, buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize data block id array", K(ret), KP(buf), K(data_len));
  } else if (new_pos - pos < size && OB_FAIL(shared_meta_block_info_arr_.deserialize(allocator, buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize shared meta block id array", K(ret), KP(buf), K(data_len));
  } else if (new_pos - pos < size && OB_FAIL(shared_data_block_info_arr_.deserialize(allocator, buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize shared data block id array", K(ret), KP(buf), K(data_len));
  } else if (OB_UNLIKELY(new_pos - pos != size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet macro info's size doesn't match", K(ret), K(new_pos), K(pos), K(size), K(meta_block_info_arr_), K(data_block_info_arr_));
  } else {
    pos = new_pos;
    is_inited_ = true;
  }
  return ret;
}

int64_t ObTabletMacroInfo::get_serialize_size() const
{
  ObSecondaryMetaHeader meta_header;
  int64_t version = 0;
  int64_t size = 0;
  int64_t len = serialization::encoded_length_i64(version);
  len += serialization::encoded_length_i64(size);
  len += entry_block_.get_serialize_size();
  len += meta_block_info_arr_.get_serialize_size();
  len += data_block_info_arr_.get_serialize_size();
  len += shared_meta_block_info_arr_.get_serialize_size();
  len += shared_data_block_info_arr_.get_serialize_size();
  len += meta_header.get_serialize_size();
  return len;
}

int64_t ObTabletMacroInfo::get_deep_copy_size() const
{
  int64_t len = sizeof(ObTabletMacroInfo);
  if (IS_EMPTY_BLOCK_LIST(entry_block_)) {
    len += meta_block_info_arr_.get_deep_copy_size();
    len += data_block_info_arr_.get_deep_copy_size();
    len += shared_meta_block_info_arr_.get_deep_copy_size();
    len += shared_data_block_info_arr_.get_deep_copy_size();
  }
  return len;
}

int ObTabletMacroInfo::deep_copy(char *buf, const int64_t buf_len, ObTabletMacroInfo *&dest_obj) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t memory_size = get_deep_copy_size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockInfoArray hasn't been inited", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || buf_len < memory_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(memory_size));
  } else {
    ObTabletMacroInfo *tablet_macro_info = new (buf) ObTabletMacroInfo();
    pos = sizeof(ObTabletMacroInfo);
    tablet_macro_info->entry_block_ = entry_block_;
    if (IS_EMPTY_BLOCK_LIST(entry_block_)) {
      if (OB_FAIL(meta_block_info_arr_.deep_copy(buf, buf_len, pos, tablet_macro_info->meta_block_info_arr_))) {
        LOG_WARN("fail to deep copy meta block id arr", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(data_block_info_arr_.deep_copy(buf, buf_len, pos, tablet_macro_info->data_block_info_arr_))) {
        LOG_WARN("fail to deep copy data block id arr", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(shared_meta_block_info_arr_.deep_copy(buf, buf_len, pos, tablet_macro_info->shared_meta_block_info_arr_))) {
        LOG_WARN("fail to deep copy shared meta block id arr", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(shared_data_block_info_arr_.deep_copy(buf, buf_len, pos, tablet_macro_info->shared_data_block_info_arr_))) {
        LOG_WARN("fail to deep copy shared data block info arr", K(ret), K(buf_len), K(pos));
      }
    } else {
      ObArenaAllocator dummy_allocator;
      if (OB_FAIL(tablet_macro_info->meta_block_info_arr_.init(0, dummy_allocator))) {
        LOG_WARN("fail to init empty meta block info arr", K(ret));
      } else if (OB_FAIL(tablet_macro_info->data_block_info_arr_.init(0, dummy_allocator))) {
        LOG_WARN("fail to init empty data block info arr", K(ret));
      } else if (OB_FAIL(tablet_macro_info->shared_meta_block_info_arr_.init(0, dummy_allocator))) {
        LOG_WARN("fail to init empty shared meta block info arr", K(ret));
      } else if (OB_FAIL(tablet_macro_info->shared_data_block_info_arr_.init(0, dummy_allocator))) {
        LOG_WARN("fail to init empty shared data block info arr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dest_obj = tablet_macro_info;
      dest_obj->is_inited_ = is_inited_;
    }
  }
  return ret;
}

bool ObTabletMacroInfo::is_valid() const
{
  return entry_block_.is_valid()
         && meta_block_info_arr_.is_valid()
         && data_block_info_arr_.is_valid()
         && shared_meta_block_info_arr_.is_valid()
         && shared_data_block_info_arr_.is_valid();
}

int ObTabletMacroInfo::get_all_macro_ids(
    ObIArray<blocksstable::MacroBlockId> &meta_block_arr,
    ObIArray<blocksstable::MacroBlockId> &data_block_arr,
    ObIArray<blocksstable::MacroBlockId> &shared_meta_block_arr,
    ObIArray<blocksstable::MacroBlockId> &shared_data_block_arr)
{
  int ret = OB_SUCCESS;
  if (IS_EMPTY_BLOCK_LIST(entry_block_)) {
    if (OB_FAIL(get_macro_ids_without_io(
        meta_block_arr,
        data_block_arr,
        shared_meta_block_arr,
        shared_data_block_arr))) {
      LOG_WARN("fail to get macro ids without io", K(ret));
    }
  } else {
    if (OB_FAIL(get_macro_ids_with_io(
        meta_block_arr,
        data_block_arr,
        shared_meta_block_arr,
        shared_data_block_arr))) {
      LOG_WARN("fail to get macro ids with io", K(ret));
    }
  }
  return ret;
}

int ObTabletMacroInfo::get_macro_ids_without_io(
    ObIArray<blocksstable::MacroBlockId> &meta_block_arr,
    ObIArray<blocksstable::MacroBlockId> &data_block_arr,
    ObIArray<blocksstable::MacroBlockId> &shared_meta_block_arr,
    ObIArray<blocksstable::MacroBlockId> &shared_data_block_arr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_id_arr(meta_block_info_arr_, meta_block_arr))) {
    LOG_WARN("fail to parse meta id array", K(ret), K(meta_block_info_arr_));
  } else if (OB_FAIL(parse_id_arr(data_block_info_arr_, data_block_arr))) {
    LOG_WARN("fail to parse data id array", K(ret), K(data_block_info_arr_));
  } else if (OB_FAIL(parse_id_arr(shared_meta_block_info_arr_, shared_meta_block_arr))) {
    LOG_WARN("fail to parse shared meta id array", K(ret), K(shared_meta_block_info_arr_));
  } else if (OB_FAIL(parse_info_arr(shared_data_block_info_arr_, shared_data_block_arr))) {
    LOG_WARN("fail to parse shared data id array", K(ret), K(shared_data_block_info_arr_));
  }
  return ret;
}

int ObTabletMacroInfo::parse_id_arr(
    const ObBlockInfoArray<blocksstable::MacroBlockId> &info_arr,
    ObIArray<blocksstable::MacroBlockId> &id_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < info_arr.cnt_; i++) {
    if (OB_FAIL(id_arr.push_back(info_arr.arr_[i]))) {
      LOG_WARN("fail to push back macro id", K(ret), K(i), K(info_arr.arr_[i]));
    }
  }
  return ret;
}

int ObTabletMacroInfo::parse_info_arr(
    const ObBlockInfoArray<ObSharedBlockInfo> &info_arr,
    ObIArray<blocksstable::MacroBlockId> &id_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < info_arr.cnt_; i++) {
    if (OB_FAIL(id_arr.push_back(info_arr.arr_[i].shared_macro_id_))) {
      LOG_WARN("fail to push back macro id", K(ret), K(i), K(info_arr.arr_[i]));
    }
  }
  return ret;
}

int ObTabletMacroInfo::get_macro_ids_with_io(
    ObIArray<blocksstable::MacroBlockId> &meta_block_arr,
    ObIArray<blocksstable::MacroBlockId> &data_block_arr,
    ObIArray<blocksstable::MacroBlockId> &shared_meta_block_arr,
    ObIArray<blocksstable::MacroBlockId> &shared_data_block_arr)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemReader block_reader;
  char *buf = nullptr;
  int64_t buf_len = 0;
  ObMetaDiskAddr addr;
  if (OB_UNLIKELY(!entry_block_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entry block is invalid", K(ret), K(entry_block_));
  } else {
    if (OB_FAIL(block_reader.init(entry_block_))) {
      LOG_WARN("fail to init linked block item reader", K(ret), K(entry_block_));
    } else if (OB_FAIL(block_reader.get_next_item(buf, buf_len, addr))) {
      LOG_WARN("fail to get next item", K(ret));
    } else if (OB_FAIL(parse_id_buf(buf, buf_len, meta_block_arr))) {
      LOG_WARN("fail to parse meta block info buf", K(ret), K(buf_len), KP(buf));
    } else if (OB_FAIL(block_reader.get_next_item(buf, buf_len, addr))) {
      LOG_WARN("fail to get next item", K(ret));
    } else if (OB_FAIL(parse_id_buf(buf, buf_len, data_block_arr))) {
      LOG_WARN("fail to parse data block info buf", K(ret), K(buf_len), KP(buf));
    } else if (OB_FAIL(block_reader.get_next_item(buf, buf_len, addr))) {
      LOG_WARN("fail to get next item", K(ret));
    } else if (OB_FAIL(parse_id_buf(buf, buf_len, shared_meta_block_arr))) {
      LOG_WARN("fail to parse shared meta block info buf", K(ret), K(buf_len), KP(buf));
    } else if (OB_FAIL(block_reader.get_next_item(buf, buf_len, addr))) {
      LOG_WARN("fail to get next item", K(ret));
    } else if (OB_FAIL(parse_info_buf(buf, buf_len, shared_data_block_arr))) {
      LOG_WARN("fail to parse shared data block info buf", K(ret), K(buf_len), KP(buf));
    }
  }
  return ret;
}

int ObTabletMacroInfo::parse_info_buf(
    const char *buf,
    const int64_t buf_len,
    ObIArray<blocksstable::MacroBlockId> &block_id_arr)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  int64_t pos = 0;
  ObSharedBlockInfo block_info;
  int16_t macro_type;
  ObArenaAllocator allocator;
  ObBlockInfoArray<ObSharedBlockInfo> block_info_arr;
  if (OB_FAIL(serialization::decode_i16(buf, buf_len, pos, &macro_type))) {
    LOG_WARN("fail to deserialize macro type", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(block_info_arr.deserialize(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize block info arr", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(parse_info_arr(block_info_arr, block_id_arr))) {
    LOG_WARN("fail to parse info arr", K(ret), K(block_info_arr));
  }
  return ret;
}

int ObTabletMacroInfo::parse_id_buf(
    const char *buf,
    const int64_t buf_len,
    ObIArray<blocksstable::MacroBlockId> &block_id_arr)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  int64_t pos = 0;
  MacroBlockId macro_id;
  int16_t macro_type;
  ObArenaAllocator allocator;
  ObBlockInfoArray<MacroBlockId> block_info_arr;
  if (OB_FAIL(serialization::decode_i16(buf, buf_len, pos, &macro_type))) {
    LOG_WARN("fail to deserialize macro type", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(block_info_arr.deserialize(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize block info arr", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(parse_id_arr(block_info_arr, block_id_arr))) {
    LOG_WARN("fail to parse id arr", K(ret), K(block_info_arr));
  }
  return ret;
}

int ObTabletMacroInfo::inc_macro_ref(bool &inc_success) const
{
  int ret = OB_SUCCESS;
  inc_success = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet macro info hasnt' been inited", K(ret));
  } else if (IS_EMPTY_BLOCK_LIST(entry_block_)) {
    if (OB_FAIL(inc_macro_ref_without_io())) {
      LOG_WARN("fail to increase macro ref cnt without io", K(ret));
    }
  } else {
    if (OB_FAIL(inc_macro_ref_with_io())) {
      LOG_WARN("fail to increase macro ref cnt with io", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    inc_success = true;
  }
  return ret;
}

void ObTabletMacroInfo::dec_macro_ref() const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet macro info hasnt' been inited", K(ret));
  } else if (IS_EMPTY_BLOCK_LIST(entry_block_)) {
    dec_macro_ref_without_io();
  } else {
    dec_macro_ref_with_io();
  }
}

void ObTabletMacroInfo::dec_macro_ref_with_io() const
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemReader block_reader;
  char *meta_id_buf = nullptr;
  int64_t meta_id_len = 0;
  char *data_id_buf = nullptr;
  int64_t data_id_len = 0;
  char *shared_meta_id_buf = nullptr;
  int64_t shared_meta_id_len = 0;
  char *shared_data_info_buf = nullptr;
  int64_t shared_data_info_len = 0;
  ObMetaDiskAddr addr;
  if (OB_UNLIKELY(!entry_block_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entry block is invalid", K(ret), K(entry_block_));
  } else {
    do {
      block_reader.reset();
      if (OB_FAIL(block_reader.init(entry_block_))) {
        LOG_WARN("fail to init linked block item reader", K(ret), K(entry_block_));
      } else if (OB_FAIL(block_reader.get_next_item(meta_id_buf, meta_id_len, addr))) {
        LOG_WARN("fail to get next item", K(ret));
      } else if (OB_FAIL(block_reader.get_next_item(data_id_buf, data_id_len, addr))) {
        LOG_WARN("fail to get next item", K(ret));
      } else if (OB_FAIL(block_reader.get_next_item(shared_meta_id_buf, shared_meta_id_len, addr))) {
        LOG_WARN("fail to get next item", K(ret));
      } else if (OB_FAIL(block_reader.get_next_item(shared_data_info_buf, shared_data_info_len, addr))) {
        LOG_WARN("fail to get next item", K(ret));
      }
    } while(ignore_ret(ret));
    if (OB_FAIL(ret)) {
      LOG_ERROR("fail to read macro id from disk, macro blocks may leak", K(ret));
    } else {
      deserialize_and_dec_macro_ref(meta_id_buf, meta_id_len);
      deserialize_and_dec_macro_ref(data_id_buf, data_id_len);
      deserialize_and_dec_macro_ref(shared_meta_id_buf, shared_meta_id_len);
      deserialize_and_dec_shared_macro_ref(shared_data_info_buf, shared_data_info_len);
      dec_linked_block_ref_cnt(block_reader.get_meta_block_list());
    }
  }
}

int ObTabletMacroInfo::inc_macro_ref_with_io() const
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemReader block_reader;
  char *meta_id_buf = nullptr;
  int64_t meta_id_len = 0;
  char *data_id_buf = nullptr;
  int64_t data_id_len = 0;
  char *shared_meta_id_buf = nullptr;
  int64_t shared_meta_id_len = 0;
  char *shared_data_info_buf = nullptr;
  int64_t shared_data_info_len = 0;
  ObMetaDiskAddr addr;
  bool inc_meta_id_success = false;
  bool inc_data_id_success = false;
  bool inc_shared_meta_id_success = false;
  bool inc_shared_data_id_success = false;
  bool inc_linked_id_success = false;
  if (OB_UNLIKELY(!entry_block_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entry block is invalid", K(ret), K(entry_block_));
  } else if (OB_FAIL(block_reader.init(entry_block_))) {
    LOG_WARN("fail to init linked block item reader", K(ret), K(entry_block_));
  } else if (OB_FAIL(block_reader.get_next_item(meta_id_buf, meta_id_len, addr))) {
    LOG_WARN("fail to get next item", K(ret));
  } else if (OB_FAIL(deserialize_and_inc_macro_ref(meta_id_buf, meta_id_len, inc_meta_id_success))) {
    LOG_WARN("fail to deserialize and inc macro ref", K(ret));
  } else if (OB_FAIL(block_reader.get_next_item(data_id_buf, data_id_len, addr))) {
    LOG_WARN("fail to get next item", K(ret));
  } else if (OB_FAIL(deserialize_and_inc_macro_ref(data_id_buf, data_id_len, inc_data_id_success))) {
    LOG_WARN("fail to deserialize and inc macro ref", K(ret));
  } else if (OB_FAIL(block_reader.get_next_item(shared_meta_id_buf, shared_meta_id_len, addr))) {
    LOG_WARN("fail to get next item", K(ret));
  } else if (OB_FAIL(deserialize_and_inc_macro_ref(shared_meta_id_buf, shared_meta_id_len, inc_shared_meta_id_success))) {
    LOG_WARN("fail to deserialize and inc macro ref", K(ret));
  } else if (OB_FAIL(block_reader.get_next_item(shared_data_info_buf, shared_data_info_len, addr))) {
    LOG_WARN("fail to get next item", K(ret));
  } else if (OB_FAIL(deserialize_and_inc_shared_macro_ref(shared_data_info_buf, shared_data_info_len, inc_shared_data_id_success))) {
    LOG_WARN("fail to deserialize and inc macro ref", K(ret));
  } else if (OB_FAIL(inc_linked_block_ref_cnt(block_reader.get_meta_block_list(), inc_linked_id_success))) {
    LOG_WARN("fail to inc linked macro ref", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (inc_meta_id_success) {
      deserialize_and_dec_macro_ref(meta_id_buf, meta_id_len);
    }
    if (inc_data_id_success) {
      deserialize_and_dec_macro_ref(data_id_buf, data_id_len);
    }
    if (inc_shared_meta_id_success) {
      deserialize_and_dec_macro_ref(shared_meta_id_buf, shared_meta_id_len);
    }
    if (inc_shared_data_id_success) {
      deserialize_and_dec_shared_macro_ref(shared_data_info_buf, shared_data_info_len);
    }
    if (inc_linked_id_success) {
      dec_linked_block_ref_cnt(block_reader.get_meta_block_list());
    }
  }
  return ret;
}

int ObTabletMacroInfo::inc_linked_block_ref_cnt(
    const ObIArray<blocksstable::MacroBlockId> &linked_block_list,
    bool &inc_macro_id_success) const
{
  int ret = OB_SUCCESS;
  int64_t inc_cnt = 0;
  inc_macro_id_success = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < linked_block_list.count(); i++) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(linked_block_list.at(i)))) {
      LOG_WARN("fail to increase ref cnt for linked block", K(ret), K(i), K(linked_block_list.at(i)));
    } else {
      inc_cnt++;
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < inc_cnt; i++) {
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(linked_block_list.at(i)))) {
        LOG_WARN("fail to decrease ref cnt for linked block", K(tmp_ret), K(i), K(linked_block_list.at(i)));
      }
    }
  } else {
    inc_macro_id_success = true;
  }
  return ret;
}

void ObTabletMacroInfo::dec_linked_block_ref_cnt(const ObIArray<blocksstable::MacroBlockId> &linked_block_list) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < linked_block_list.count(); i++) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(linked_block_list.at(i)))) {
      LOG_WARN("fail to decrease ref cnt for linked block", K(ret), K(i), K(linked_block_list.at(i)));
    }
  }
}

void ObTabletMacroInfo::deserialize_and_dec_macro_ref(const char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  int64_t pos = 0;
  MacroBlockId macro_id;
  int16_t macro_type;
  ObArenaAllocator allocator;
  ObBlockInfoArray<MacroBlockId> block_id_arr;
  if (OB_FAIL(serialization::decode_i16(buf, buf_len, pos, &macro_type))) {
    LOG_WARN("fail to deserialize macro type", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(block_id_arr.deserialize(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize block id arr", K(ret), K(buf_len), K(pos));
  } else {
    do_dec_macro_ref(block_id_arr);
  }
}

void ObTabletMacroInfo::deserialize_and_dec_shared_macro_ref(const char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  int64_t pos = 0;
  ObSharedBlockInfo block_info;
  int16_t macro_type;
  ObArenaAllocator allocator;
  ObBlockInfoArray<MacroBlockId> block_info_arr;
  if (OB_FAIL(serialization::decode_i16(buf, buf_len, pos, &macro_type))) {
    LOG_WARN("fail to deserialize macro type", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(block_info_arr.deserialize(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize block id arr", K(ret), K(buf_len), K(pos));
  } else {
    do_dec_macro_ref(block_info_arr);
  }
}

int ObTabletMacroInfo::deserialize_and_inc_macro_ref(const char *buf, const int64_t buf_len, bool &inc_success) const
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  int64_t pos = 0;
  int64_t inc_cnt = 0;
  int64_t id_pos = 0;
  MacroBlockId macro_id;
  inc_success = false;
  int16_t macro_type;
  ObArenaAllocator allocator;
  ObBlockInfoArray<MacroBlockId> block_id_arr;
  if (OB_FAIL(serialization::decode_i16(buf, buf_len, pos, &macro_type))) {
    LOG_WARN("fail to deserialize macro type", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(block_id_arr.deserialize(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize block id arr", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(do_inc_macro_ref(block_id_arr, inc_success))) {
    LOG_WARN("fail to increase macro ref", K(ret), K(block_id_arr));
  }
  return ret;
}

int ObTabletMacroInfo::deserialize_and_inc_shared_macro_ref(const char *buf, const int64_t buf_len, bool &inc_success) const
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  int64_t pos = 0;
  int64_t id_pos = 0;
  int64_t macro_ref_inc_cnt = 0;
  int64_t occupy_size_inc_cnt = 0;
  ObSharedBlockInfo block_info;
  int16_t macro_type;
  inc_success = false;
  ObArenaAllocator allocator;
  ObBlockInfoArray<ObSharedBlockInfo> block_info_arr;
  if (OB_FAIL(serialization::decode_i16(buf, buf_len, pos, &macro_type))) {
    LOG_WARN("fail to deserialize macro id cnt", K(ret), K(buf_len));
  } else if (OB_FAIL(block_info_arr.deserialize(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize block id arr", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(do_inc_macro_ref(block_info_arr, inc_success))) {
    LOG_WARN("fail to increase macro ref", K(ret), K(block_info_arr));
  }
  return ret;
}

void ObTabletMacroInfo::dec_macro_ref_without_io() const
{
  int ret = OB_SUCCESS;
  do_dec_macro_ref(data_block_info_arr_);
  do_dec_macro_ref(meta_block_info_arr_);
  do_dec_macro_ref(shared_meta_block_info_arr_);
  do_dec_macro_ref(shared_data_block_info_arr_);
}

int ObTabletMacroInfo::inc_macro_ref_without_io() const
{
  int ret = OB_SUCCESS;
  bool inc_data_macro_id_success = false;
  bool inc_meta_macro_id_success = false;
  bool inc_shared_meta_macro_id_success = false;
  bool inc_shared_data_macro_id_success = false;
  if (OB_FAIL(do_inc_macro_ref(meta_block_info_arr_, inc_meta_macro_id_success))) {
    LOG_WARN("fail to increase meta blocks' ref cnt", K(ret), K(meta_block_info_arr_));
  } else if (OB_FAIL(do_inc_macro_ref(data_block_info_arr_, inc_data_macro_id_success))) {
    LOG_WARN("fail to increase data blocks' ref cnt", K(ret), K(data_block_info_arr_));
  } else if (OB_FAIL(do_inc_macro_ref(shared_meta_block_info_arr_, inc_shared_meta_macro_id_success))) {
    LOG_WARN("fail to increase shared meta blocks' ref cnt", K(ret), K(shared_meta_block_info_arr_));
  } else if (OB_FAIL(do_inc_macro_ref(shared_data_block_info_arr_, inc_shared_data_macro_id_success))) {
    LOG_WARN("fail to increase shared data blocks' ref cnt and block size", K(ret), K(shared_data_block_info_arr_));
  }
  if (OB_FAIL(ret)) {
    if (inc_data_macro_id_success) {
      do_dec_macro_ref(data_block_info_arr_);
    }
    if (inc_meta_macro_id_success) {
      do_dec_macro_ref(meta_block_info_arr_);
    }
    if (inc_shared_meta_macro_id_success) {
      do_dec_macro_ref(shared_meta_block_info_arr_);
    }
    if (inc_shared_data_macro_id_success) {
      do_dec_macro_ref(shared_data_block_info_arr_);
    }
  }
  return ret;
}

int ObTabletMacroInfo::do_inc_macro_ref(const ObBlockInfoArray<ObSharedBlockInfo> &block_info_arr, bool &inc_macro_id_success) const
{
  int ret = OB_SUCCESS;
  inc_macro_id_success = false;
  int64_t macro_ref_inc_cnt = 0;
  int64_t occupy_size_inc_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_info_arr.cnt_; i++) {
    const MacroBlockId &macro_id = block_info_arr.arr_[i].shared_macro_id_;
    const int64_t occupy_size = block_info_arr.arr_[i].occupy_size_;
    if (OB_UNLIKELY(!macro_id.is_valid() || occupy_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro_id/occupy_size is invalid", K(ret), K(macro_id), K(occupy_size));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
      LOG_WARN("fail to increase macro ref cnt", K(ret), K(macro_id));
    } else if (FALSE_IT(macro_ref_inc_cnt++)) {
    } else if (OB_FAIL(MTL(ObSharedMacroBlockMgr*)->add_block(macro_id, occupy_size))) {
      LOG_WARN("fail to increase shared block's occupy size", K(ret), K(macro_id), K(occupy_size));
    } else {
      occupy_size_inc_cnt++;
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    // no need to check OB_SUCC(ret)
    for (int64_t i = 0; i < macro_ref_inc_cnt; i++) {
      const MacroBlockId &macro_id = block_info_arr.arr_[i].shared_macro_id_;
      const int64_t occupy_size = block_info_arr.arr_[i].occupy_size_;
      if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
        LOG_WARN("fail to decrease macro ref cnt", K(tmp_ret), K(macro_id));
      } else if (i < occupy_size_inc_cnt && OB_TMP_FAIL(MTL(ObSharedMacroBlockMgr*)->free_block(macro_id, occupy_size))) {
        LOG_WARN("fail to decrease shared block's occupy size", K(tmp_ret), K(macro_id), K(occupy_size));
      }
    }
  } else {
    inc_macro_id_success = true;
  }
  return ret;
}

int ObTabletMacroInfo::do_inc_macro_ref(const ObBlockInfoArray<blocksstable::MacroBlockId> &block_info_arr, bool &inc_macro_id_success) const
{
  int ret = OB_SUCCESS;
  inc_macro_id_success = false;
  int64_t increased_id_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_info_arr.cnt_; i++) {
    const MacroBlockId &macro_id = block_info_arr.arr_[i];
    if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro id is invalid", K(ret), K(macro_id));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
      LOG_WARN("fail to increase macro ref cnt", K(ret), K(macro_id));
    } else {
      increased_id_cnt++;
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    // no need to check OB_SUCC(ret)
    for (int64_t i = 0; i < increased_id_cnt; i++) {
      const MacroBlockId &macro_id = block_info_arr.arr_[i];
      if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
        LOG_WARN("fail to decrease macro ref cnt", K(tmp_ret), K(macro_id));
      }
    }
  } else {
    inc_macro_id_success = true;
  }
  return ret;
}

void ObTabletMacroInfo::do_dec_macro_ref(const ObBlockInfoArray<ObSharedBlockInfo> &block_info_arr) const
{
  int ret = OB_SUCCESS;
  // no need to check OB_SUCC(ret)
  for (int64_t i = 0; i < block_info_arr.cnt_; i++) {
    const MacroBlockId &macro_id = block_info_arr.arr_[i].shared_macro_id_;
    const int64_t occupy_size = block_info_arr.arr_[i].occupy_size_;
    if (OB_UNLIKELY(!macro_id.is_valid() || occupy_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro_id/occupy_size is invalid", K(ret), K(macro_id), K(occupy_size));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
      LOG_WARN("fail to decrease macro ref cnt", K(ret), K(macro_id));
    } else if (OB_FAIL(MTL(ObSharedMacroBlockMgr*)->free_block(macro_id, occupy_size))) {
      LOG_WARN("fail to decrease shared block's occupy size", K(ret), K(macro_id), K(occupy_size));
    }
  }
}

void ObTabletMacroInfo::do_dec_macro_ref(const ObBlockInfoArray<blocksstable::MacroBlockId> &block_info_arr) const
{
  int ret = OB_SUCCESS;
  // no need to check OB_SUCC(ret)
  for (int64_t i = 0; i < block_info_arr.cnt_; i++) {
    const MacroBlockId &macro_id = block_info_arr.arr_[i];
    if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro id is invalid", K(ret), K(macro_id));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
      LOG_WARN("fail to decrease macro ref cnt", K(ret), K(macro_id));
    }
  }
}

bool ObTabletMacroInfo::ignore_ret(const int ret)
{
  return OB_ALLOCATE_MEMORY_FAILED == ret || OB_TIMEOUT == ret || OB_DISK_HUNG == ret;
}
} // storage
} // oceanbase