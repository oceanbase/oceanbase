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

#ifndef OCEANBASE_STORAGE_OB_TABLET_BLOCK_HEADER_H
#define OCEANBASE_STORAGE_OB_TABLET_BLOCK_HEADER_H

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/container/ob_array_serialization.h"

namespace oceanbase
{
namespace storage
{
enum class ObSecondaryMetaType : uint8_t
{
  TABLET_MACRO_INFO = 0,
  TABLE_STORE = 1,
  STORAGE_SCHEMA = 2,
  MDS_DATA = 3,
  MAX = 4
};

struct ObInlineSecondaryMetaDesc final
{
public:
  ObInlineSecondaryMetaDesc()
    : type_(ObSecondaryMetaType::MAX), length_(0)
  {
  }
  ObInlineSecondaryMetaDesc(const ObSecondaryMetaType type, const int32_t length)
    : type_(type), length_(length)
  {
  }

  ObSecondaryMetaType type_;
  int32_t length_;

  TO_STRING_KV(K_(type), K_(length));

} __attribute__((packed));

class ObTabletBlockHeader final
{
public:
  static const int32_t TABLET_VERSION_V1 = 1;
  static const int32_t TABLET_VERSION_V2 = 2;
  static const int32_t TABLET_VERSION_V3 = 3;
  static const int32_t TABLET_VERSION_V4 = 4;

public:
  ObTabletBlockHeader()
    : is_inited_(false),
      pushed_inline_meta_cnt_(0),
      version_(TABLET_VERSION_V4),
      first_level_size_(0),
      first_level_checksum_(0),
      data_version_(0),
      inline_meta_count_(0)
  {
  }
  ~ObTabletBlockHeader() = default;
  static bool is_valid_version(const int32_t version)
  {
    return version >= TABLET_VERSION_V1 && version <= TABLET_VERSION_V4;
  }
  static bool is_supported_version(const int32_t version)
  {
    return version == TABLET_VERSION_V3 || version == TABLET_VERSION_V4;
  }
  int init(const int32_t secondary_meta_count);
  bool is_valid() const
  {
    return is_inited_
        && is_supported_version(version_)
        && first_level_size_ > 0
        && first_level_checksum_ > 0
        && inline_meta_count_ >= 0;
  }

  int check_data_version_for_compat() const;
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size(void) const;
  int push_inline_meta(const ObInlineSecondaryMetaDesc &desc);

  TO_STRING_KV(K_(version),
               K_(first_level_size),
               K_(first_level_checksum),
               "desc_array", common::ObArrayWrap<ObInlineSecondaryMetaDesc>(desc_array_, inline_meta_count_),
               K_(data_version));
private:
  static const int32_t MAX_INLINE_META_COUNT = 8;
  int deserialize_v3(const char* buf, const int64_t data_len, int64_t& pos);
  int deserialize_v4(const char* buf, const int64_t data_len, int64_t& pos);

  bool is_inited_;
  int32_t pushed_inline_meta_cnt_;
public:
  // below need serialize
  int32_t version_;
  int32_t length_;
  int32_t first_level_size_; // tablet first-level meta size
  int32_t first_level_checksum_; // checksum for tablet first-level meta
  uint64_t data_version_; // data version at write.
  int32_t inline_meta_count_; // inline meta refers the secondary meta which is stored consecutively with tablet first-level meta
  ObInlineSecondaryMetaDesc desc_array_[MAX_INLINE_META_COUNT];
};

struct ObSecondaryMetaHeader final
{
public:
  static const int32_t SECONDARY_META_HEADER_VERSION = 1;
public:
  ObSecondaryMetaHeader()
    : version_(SECONDARY_META_HEADER_VERSION),
      size_(sizeof(ObSecondaryMetaHeader)), checksum_(0), payload_size_(0)
  {
  }
  ~ObSecondaryMetaHeader() { destroy(); }
  void destroy();
  TO_STRING_KV(K_(version), K_(checksum), K_(size), K_(payload_size));
  NEED_SERIALIZE_AND_DESERIALIZE;
public:
  int32_t version_;
  int32_t size_;
  int32_t checksum_;
  int32_t payload_size_;
};

struct ObInlineSecondaryMeta final
{
public:
  ObInlineSecondaryMeta()
  {
  }
  ObInlineSecondaryMeta(const void *obj, const ObSecondaryMetaType meta_type)
    : obj_(obj), meta_type_(meta_type)
  {
  }
  TO_STRING_KV(KP_(obj), K_(meta_type));
  const void *obj_;
  ObSecondaryMetaType meta_type_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_BLOCK_HEADER_H
