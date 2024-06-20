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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_H_
#include "stdint.h"
#include  "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace blocksstable
{
struct ObMicroBlockHeader
{
  static const int64_t COLUMN_CHECKSUM_PTR_OFFSET;
public:
  int16_t magic_;
  int16_t version_;
  uint32_t header_size_;
  int16_t header_checksum_;
  uint16_t column_count_;
  uint16_t rowkey_column_count_;
  struct {
    uint16_t has_column_checksum_ : 1;
    uint16_t has_string_out_row_ : 1; // flag for furture, varchar and char can be overflowed as lob handle
    uint16_t all_lob_in_row_ : 1; // compatible with 4.0, we assume that all lob is out row in old data
    uint16_t contains_hash_index_   : 1;
    uint16_t hash_index_offset_from_end_ : 10;
    uint16_t has_min_merged_trans_version_   : 1;
    uint16_t reserved16_          : 1;
  };
  uint32_t row_count_;
  uint8_t row_store_type_;
  union {
    struct {
      uint8_t row_index_byte_    :3;
      uint8_t extend_value_bit_  :3;
      uint8_t reserved_          :2;
    }; // For encoding format
    struct {
      uint8_t single_version_rows_: 1;
      uint8_t contain_uncommitted_rows_: 1;
      uint8_t is_last_row_last_flag_ : 1;
      uint8_t not_used_ : 5;
    }; // For flat format
    uint8_t opt_;
  };
  union {
    uint16_t var_column_count_; // For pax encoding format
    struct { // For cs encoding format
      uint8_t compressor_type_;
      uint8_t cs_reserved_;
    };
    uint16_t opt2_;
  };

  union {
    uint32_t row_index_offset_; // For flat format
    uint32_t row_data_offset_; // For encoding format
    uint32_t row_offset_;
  };
  int32_t original_length_;
  int64_t max_merged_trans_version_;
  int32_t data_length_;
  int32_t data_zlength_;
  int64_t data_checksum_;
  union {
    int64_t *column_checksums_;
    int64_t min_merged_trans_version_;
  };
public:
  ObMicroBlockHeader();
  ~ObMicroBlockHeader() = default;
  void reset() { new (this) ObMicroBlockHeader(); }
  bool check_data_valid() const;
  bool is_valid() const;
  void set_header_checksum();
  int check_header_checksum() const;
  int check_payload_checksum(const char *buf, const int64_t len) const;
  static int deserialize_and_check_record(
      const char *ptr, const int64_t size,
      const int16_t magic, const char *&payload_ptr, int64_t &payload_size);
  static int deserialize_and_check_record(const char *ptr, const int64_t size, const int16_t magic);
  int deserialize_and_check_header(const char *ptr, const int64_t size);
  int check_and_get_record(
      const char *ptr, const int64_t size, const int16_t magic,
      const char *&payload_ptr, int64_t &payload_size) const;
  int check_record(const char *ptr, const int64_t size, const int16_t magic) const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int deep_copy(char *buf, const int64_t buf_len, int64_t &pos, ObMicroBlockHeader *&new_header) const;
  uint32_t get_serialize_size() const { return get_serialize_size(column_count_, has_column_checksum_); }

  static uint32_t get_serialize_size(const int64_t column_count, const bool need_calc_column_chksum) {
    return static_cast<uint32_t>(ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET +
        (need_calc_column_chksum ? column_count * sizeof(int64_t) : 0));
  }
  TO_STRING_KV(K_(magic), K_(version), K_(header_size), K_(header_checksum),
      K_(column_count), K_(rowkey_column_count), K_(has_column_checksum), K_(row_count), K_(row_store_type),
      K_(opt), K_(var_column_count), K_(compressor_type), K_(row_offset), K_(original_length), K_(max_merged_trans_version), K_(min_merged_trans_version),
      K_(data_length), K_(data_zlength), K_(data_checksum), KP_(column_checksums), K_(single_version_rows),
      K_(contain_uncommitted_rows),  K_(is_last_row_last_flag), K(is_valid()));
public:
  bool is_compressed_data() const { return data_length_ != data_zlength_; }
  bool contain_uncommitted_rows() const { return contain_uncommitted_rows_; }
  OB_INLINE bool has_string_out_row() const { return has_string_out_row_; }
  OB_INLINE bool has_lob_out_row() const { return !all_lob_in_row_; }
  bool is_last_row_last_flag() const { return is_last_row_last_flag_; }
  bool is_contain_hash_index() const;
}__attribute__((packed));
}//end namespace blocksstable
}//end namespace oceanbase
#endif
