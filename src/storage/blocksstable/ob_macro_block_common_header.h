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

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace blocksstable
{
struct ObMacroBlockCommonHeader final
{
public:
  static const int32_t MACRO_BLOCK_COMMON_HEADER_VERSION = 1;
  static const int32_t MACRO_BLOCK_COMMON_HEADER_MAGIC = 1001;

  enum MacroBlockType : int32_t
  {
    None = 0,
    SSTableData = 1,
    LinkedBlock = 2,
    TmpFileData = 3,
    SSTableMacroID = 4,
    BloomFilterData = 5,
    SSTableIndex = 6,
    SSTableMacroMeta = 7,
    SharedSSTableData = 8,
    SharedMetaData = 9,
    MaxMacroType,
  };
  static_assert(
    static_cast<int64_t>(MacroBlockType::MaxMacroType) < common::OB_MAX_MACRO_BLOCK_TYPE,
    "MacroBlockType max value should less than 4 bits");

  ObMacroBlockCommonHeader();
  // DO NOT use virtual function. It is by design that sizeof(ObMacroBlockCommonHeader) equals to
  // the serialization length of all data members.
  ~ObMacroBlockCommonHeader() {};

  void reset();
  int build_serialized_header(char *buf, const int64_t len) const;
  int check_integrity() const;
  bool is_valid() const;

  bool is_data_block() const
  {
    return is_sstable_data_block() || is_bloom_filter_data_block();
  }
  bool is_sstable_data_block() const { return MacroBlockType::SSTableData == attr_; }
  bool is_linked_macro_block() const { return MacroBlockType::LinkedBlock == attr_; }
  bool is_bloom_filter_data_block() const { return MacroBlockType::BloomFilterData == attr_; }
  bool is_sstable_index_block() const { return MacroBlockType::SSTableIndex ==  attr_; }
  bool is_sstable_macro_meta_block() const { return MacroBlockType::SSTableMacroMeta == attr_; }
  bool is_shared_macro_block() const
  {
    return MacroBlockType::SharedSSTableData == attr_ || MacroBlockType::SharedMetaData == attr_;
  }
  int32_t get_header_size() const { return header_size_; }
  int32_t get_version() const { return version_; }
  int32_t get_magic() const { return magic_; }
  int32_t get_attr() const { return attr_; }
  MacroBlockType get_type() const { return static_cast<MacroBlockType>(attr_); }
  int32_t get_payload_size() const { return payload_size_; }
  int32_t get_payload_checksum() const { return payload_checksum_; }
  void set_attr(const MacroBlockType type) { attr_ = type; }
  void set_attr(const int64_t seq);
  void set_payload_size(const int32_t payload_size) { payload_size_ = payload_size; }
  void set_payload_checksum(const int32_t payload_checksum) { payload_checksum_ = payload_checksum; }

  int serialize(char *buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  static int64_t get_serialize_size()
  {
    return sizeof(ObMacroBlockCommonHeader);
  }

  static int get_attr_name(const int32_t attr, common::ObString &attr_name);

  TO_STRING_KV(K_(header_size),
               K_(version),
               K_(magic),
               K_(attr),
               K_(payload_size),
               K_(payload_checksum));

private:
  // NOTE: data members should be 64 bits aligned!!!
  int32_t header_size_; //struct size
  int32_t version_;     //header version
  int32_t magic_;       //magic number
  int32_t attr_;        // MacroBlockType
  int32_t payload_size_; // not include the size of common header
  int32_t payload_checksum_; // crc64 of payload

  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockCommonHeader);
};
} // blocksstable
} // oceanbase

#endif /* OB_MACRO_BLOCK_COMMON_HEADER_H_ */
