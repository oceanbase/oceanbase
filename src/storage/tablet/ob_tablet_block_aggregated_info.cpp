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
ObTabletMacroInfo::ObBlockInfoArray<T>::ObBlockInfoArray()
  : cnt_(0), arr_(nullptr), capacity_(0)
{
}

template <typename T>
ObTabletMacroInfo::ObBlockInfoArray<T>::~ObBlockInfoArray()
{
  reset();
}

template <typename T>
void ObTabletMacroInfo::ObBlockInfoArray<T>::reset()
{
  cnt_ = 0;
  capacity_ = 0;
  arr_ = nullptr;
}

template <typename T>
int ObTabletMacroInfo::ObBlockInfoArray<T>::reserve(const int64_t cnt, ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (0 == cnt) {
    // no macro id
    arr_ = nullptr;
  } else if (OB_ISNULL(arr_ = reinterpret_cast<T *>(allocator.alloc(sizeof(T) * cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(T) * cnt));
  }
  if (OB_SUCC(ret)) {
    cnt_ = cnt;
    capacity_ = cnt;
  }
  return ret;
}

template <typename T>
int ObTabletMacroInfo::ObBlockInfoArray<T>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
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
int ObTabletMacroInfo::ObBlockInfoArray<T>::deserialize(ObArenaAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &cnt_))) {
    LOG_WARN("fail to decode count", K(ret), K(data_len), K(pos));
  } else if (0 == cnt_) {
    // no macro id
    arr_ = nullptr;
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
    capacity_ = cnt_;
  }
  return ret;
}

template <typename T>
int64_t ObTabletMacroInfo::ObBlockInfoArray<T>::get_serialize_size() const
{
  T block_info;
  return serialization::encoded_length_i64(cnt_) + block_info.get_serialize_size() * cnt_;
}

template <typename T>
int64_t ObTabletMacroInfo::ObBlockInfoArray<T>::get_deep_copy_size() const
{
  return sizeof(T) * cnt_;
}

template <typename T>
int ObTabletMacroInfo::ObBlockInfoArray<T>::deep_copy(char *buf, const int64_t buf_len, int64_t &pos, ObBlockInfoArray &dest_obj) const
{
  int ret = OB_SUCCESS;
  const int64_t memory_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0 || buf_len - pos < memory_size)) {
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
    const ObBlockInfoSet &info_set,
    ObLinkedMacroBlockItemWriter &linked_writer)
{
  int ret = OB_SUCCESS;
  const ObBlockInfoSet::TabletMacroSet &meta_block_info_set = info_set.meta_block_info_set_;
  const ObBlockInfoSet::TabletMacroSet &data_block_info_set = info_set.data_block_info_set_;
  const ObBlockInfoSet::TabletMacroSet &shared_meta_block_info_set = info_set.shared_meta_block_info_set_;
  const ObBlockInfoSet::TabletMacroMap &shared_data_block_info_map = info_set.shared_data_block_info_map_;
  const int64_t total_macro_cnt = meta_block_info_set.size()
                            + data_block_info_set.size()
                            + shared_meta_block_info_set.size()
                            + shared_data_block_info_map.size();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletMacroInfo has been inited", K(ret));
  } else if (OB_FAIL(meta_block_info_arr_.reserve(meta_block_info_set.size(), allocator))) {
    LOG_WARN("fail to init meta block id arr", K(ret));
  } else if (OB_FAIL(data_block_info_arr_.reserve(data_block_info_set.size(), allocator))) {
    LOG_WARN("fail to init data block id arr", K(ret));
  } else if (OB_FAIL(shared_meta_block_info_arr_.reserve(shared_meta_block_info_set.size(), allocator))) {
    LOG_WARN("fail to init shared meta block info arr", K(ret));
  } else if (OB_FAIL(shared_data_block_info_arr_.reserve(shared_data_block_info_map.size(), allocator))) {
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
    const ObBlockInfoSet::TabletMacroSet &id_set,
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
    const ObBlockInfoSet::TabletMacroMap &block_info_map,
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
  ObMemAttr mem_attr(MTL_ID(), "TabletBlockId");
  if (OB_FAIL(linked_writer.init(false /*whether need addr*/, mem_attr))) {
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
  } else {
    meta_block_info_arr_.reset();
    data_block_info_arr_.reset();
    shared_meta_block_info_arr_.reset();
    shared_data_block_info_arr_.reset();
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
      if (OB_FAIL(tablet_macro_info->meta_block_info_arr_.reserve(0, dummy_allocator))) {
        LOG_WARN("fail to init empty meta block info arr", K(ret));
      } else if (OB_FAIL(tablet_macro_info->data_block_info_arr_.reserve(0, dummy_allocator))) {
        LOG_WARN("fail to init empty data block info arr", K(ret));
      } else if (OB_FAIL(tablet_macro_info->shared_meta_block_info_arr_.reserve(0, dummy_allocator))) {
        LOG_WARN("fail to init empty shared meta block info arr", K(ret));
      } else if (OB_FAIL(tablet_macro_info->shared_data_block_info_arr_.reserve(0, dummy_allocator))) {
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
} // storage
} // oceanbase