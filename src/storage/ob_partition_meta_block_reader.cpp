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

#include "ob_partition_meta_block_reader.h"
#include "ob_partition_service.h"

using namespace oceanbase::blocksstable;
namespace oceanbase {
namespace storage {

ObPartitionMetaBlockReader::ObPartitionMetaBlockReader(ObPartitionService& partition_service)
    : partition_service_(partition_service),
      allocator_(ObModIds::OB_PARTITION_META),
      read_buf_(NULL),
      offset_(0),
      buf_size_(0)
{}

ObPartitionMetaBlockReader::~ObPartitionMetaBlockReader()
{}

int ObPartitionMetaBlockReader::parse(const blocksstable::ObMacroBlockCommonHeader& common_header,
    const blocksstable::ObLinkedMacroBlockHeader& linked_header, const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_UNLIKELY(ObMacroBlockCommonHeader::PartitionMeta != common_header.get_attr())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The header attr is not partition meta, ", K(ret), K(common_header.get_attr()));
  } else if (OB_UNLIKELY(ObMacroBlockCommonHeader::PartitionMeta != linked_header.attr_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The header attr is not partition meta, ", K(ret), K(linked_header.attr_));
  } else {
    if (0 == linked_header.meta_data_count_) {
      int64_t serialize_size = linked_header.user_data2_;
      if (serialize_size > 0) {
        // first serialize block with the partition meta larger than 2M
        allocator_.reuse();
        offset_ = 0;

        int64_t size = serialize_size + OB_FILE_SYSTEM.get_macro_block_size();
        if (NULL == (read_buf_ = static_cast<char*>(allocator_.alloc(size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(size));
        } else {
          MEMCPY(read_buf_, buf, buf_len);
          offset_ += buf_len;
          buf_size_ = serialize_size;
        }
      } else {
        // medium serialize block with the partition meta larger than 2M
        MEMCPY(read_buf_ + offset_, buf, buf_len);
        offset_ += buf_len;
      }
    } else {
      if (0 == offset_) {
        read_buf_ = const_cast<char*>(buf);
        buf_size_ = buf_len;
      } else {
        // last serialize block with the partition meta larger than 2M
        MEMCPY(read_buf_ + offset_, buf, buf_len);
        offset_ = 0;
      }
    }
    ObStorageFileHandle file_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < linked_header.meta_data_count_; ++i) {
      if (OB_FAIL(partition_service_.load_partition(read_buf_, buf_size_, pos, file_handle))) {
        STORAGE_LOG(WARN, "deserialize current partition failed.", K(ret), K(i), K(buf_size_), K(offset_), K(pos));
      }
    }
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
