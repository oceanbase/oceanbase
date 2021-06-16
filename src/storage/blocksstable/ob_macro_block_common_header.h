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

#ifndef OB_MACRO_BLOCK_COMMON_HEADER_H_
#define OB_MACRO_BLOCK_COMMON_HEADER_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"

namespace oceanbase {
namespace blocksstable {
struct ObMacroBlockCommonHeader {
public:
  static const int32_t MACRO_BLOCK_COMMON_HEADER_VERSION = 1;
  static const int32_t MACRO_BLOCK_COMMON_HEADER_MAGIC = 1001;

  enum MacroBlockType {
    Free = 0,
    SSTableData = 1,
    PartitionMeta = 2,
    // SchemaData = 3,
    // Compressor = 4,
    MacroMeta = 5,
    Reserved = 6,
    MacroBlockSecondIndex = 7,  // deprecated
    SortTempData = 8,
    LobData = 9,
    LobIndex = 10,
    TableMgrMeta = 11,
    TenantConfigMeta = 12,
    BloomFilterData = 13,
    MaxMacroType,
  };
  static_assert(static_cast<int64_t>(MacroBlockType::MaxMacroType) < common::OB_MAX_MACRO_BLOCK_TYPE,
      "MacroBlockType max value should less than 4 bits");

  ObMacroBlockCommonHeader();
  // DO NOT use virtual function. It is by design that sizeof(ObMacroBlockCommonHeader) equals to
  // the serialization length of all data members.
  ~ObMacroBlockCommonHeader(){};

  void reset();
  int build_serialized_header(char* buf, const int64_t len) const;
  int check_integrity();
  bool is_valid() const;

  OB_INLINE bool is_data_block() const
  {
    return is_sstable_data_block() || is_lob_data_block() || is_bloom_filter_data_block();
  }
  OB_INLINE bool is_sstable_data_block() const
  {
    return SSTableData == attr_;
  }
  OB_INLINE bool is_lob_data_block() const
  {
    return LobData == attr_ || LobIndex == attr_;
  }
  OB_INLINE bool is_bloom_filter_data_block() const
  {
    return BloomFilterData == attr_;
  }
  OB_INLINE int32_t get_header_size() const
  {
    return header_size_;
  }
  OB_INLINE int32_t get_version() const
  {
    return version_;
  }
  OB_INLINE int32_t get_magic() const
  {
    return magic_;
  }
  OB_INLINE int32_t get_attr() const
  {
    return attr_;
  }
  OB_INLINE void set_attr(const MacroBlockType type)
  {
    attr_ = type;
  }
  OB_INLINE void set_data_version(const int64_t version)
  {
    data_version_ = version;
  }
  OB_INLINE int64_t get_previous_block_index() const
  {
    return previous_block_index_;
  }
  OB_INLINE void set_previous_block_index(const int64_t index)
  {
    previous_block_index_ = index;
  }
  OB_INLINE void set_reserved(const int64_t reserved)
  {
    reserved_ = reserved;
  }
  OB_INLINE void set_payload_size(const int32_t payload_size)
  {
    payload_size_ = payload_size;
  }
  OB_INLINE void set_payload_checksum(const int32_t payload_checksum)
  {
    payload_checksum_ = payload_checksum;
  }
  OB_INLINE int32_t get_payload_size() const
  {
    return payload_size_;
  }
  OB_INLINE int32_t get_payload_checksum() const
  {
    return payload_checksum_;
  }

  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;
  static int get_attr_name(int32_t attr, common::ObString& attr_name);

  TO_STRING_KV(
      K_(header_size), K_(version), K_(magic), K_(attr), K_(data_version), K_(payload_size), K_(payload_checksum));

private:
  // NOTE: data members should be 64 bits aligned!!!
  int32_t header_size_;  // struct size
  int32_t version_;      // header version
  int32_t magic_;        // magic number
  // each bits of the lower 8 bits represents:
  // is_free, sstable meta, tablet meta, compressor name, macro block meta, 0, 0, 0
  int32_t attr_;
  union {
    uint64_t data_version_;  // sstable macro block: major version(48 bits) + minor version(16 bits)
    /**
     * For meta block, it is organized as a link block. The next block has a index point to
     * its previous block. The entry block index is store in upper layer meta (super block)
     * 0 or -1 means no previous block
     */
    int64_t previous_block_index_;
  };
  union {
    int64_t reserved_;
    struct {
      int32_t payload_size_;      // not include the size of common header
      int32_t payload_checksum_;  // crc64 of payload
    };
  };

  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockCommonHeader);
};
}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_MACRO_BLOCK_COMMON_HEADER_H_ */
