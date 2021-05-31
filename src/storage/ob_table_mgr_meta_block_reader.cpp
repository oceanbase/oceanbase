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

#include "ob_table_mgr_meta_block_reader.h"
#include "ob_partition_service.h"
#include "ob_table_mgr.h"

using namespace oceanbase;
using namespace storage;
using namespace blocksstable;
using namespace common;

ObTableMgrMetaBlockReader::ObTableMgrMetaBlockReader()
    : allocator_(ObModIds::OB_TABLE_MGR_ALLOCATOR), read_buf_(NULL), offset_(0), buf_size_(0)
{}

ObTableMgrMetaBlockReader::~ObTableMgrMetaBlockReader()
{}

int ObTableMgrMetaBlockReader::parse(const blocksstable::ObMacroBlockCommonHeader& common_header,
    const blocksstable::ObLinkedMacroBlockHeader& linked_header, const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_UNLIKELY(ObMacroBlockCommonHeader::TableMgrMeta != common_header.get_attr())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The header attr is not sstable mgr meta, ", K(ret), K(common_header.get_attr()));
  } else if (OB_UNLIKELY(ObMacroBlockCommonHeader::TableMgrMeta != linked_header.attr_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The header attr is not sstable mgr meta, ", K(ret), K(linked_header.attr_));
  } else {
    if (0 == linked_header.meta_data_count_) {
      int64_t serialize_size = linked_header.user_data2_;
      if (serialize_size > 0) {
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
    int64_t log_seq_num = linked_header.user_data1_;
    for (int64_t i = 0; OB_SUCC(ret) && i < linked_header.meta_data_count_; ++i) {
      if (OB_FAIL(ObTableMgr::get_instance().load_sstable(read_buf_, buf_size_, pos))) {
        STORAGE_LOG(WARN, "deserialize current partition failed.", K(ret), K_(buf_size), K(pos));
      } else {
        STORAGE_LOG(TRACE, "replay load sstable", K(i), K(pos), KP(read_buf_), K_(buf_size), K(log_seq_num));
      }
    }
  }

  return ret;
}
