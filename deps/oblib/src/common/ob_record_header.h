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

#ifndef OCEANBASE_CHUNKSERVER_RECORD_HEADER_H_
#define OCEANBASE_CHUNKSERVER_RECORD_HEADER_H_

#include "lib/ob_define.h"
#include "lib/utility/serialization.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{

inline void format_i64(const int64_t value, int16_t &check_sum)
{
  int i = 0;
  while (i < 4) {
    check_sum =  static_cast<int16_t>(check_sum ^ ((value >> i * 16) & 0xFFFF));
    ++i;
  }
}

inline void format_i32(const int32_t value, int16_t &check_sum)
{
  int i = 0;
  while (i < 2) {
    check_sum =  static_cast<int16_t>(check_sum ^ ((value >> i * 16) & 0xFFFF));
    ++i;
  }
}

struct ObRecordHeader
{
  static const int16_t MAGIC_NUMER = static_cast<int16_t>(0xB0CC);

  int16_t magic_;          // magic number
  int16_t header_length_;  // header length
  int16_t version_;        // version
  int16_t header_checksum_;// header checksum
  int64_t timestamp_;       //
  int32_t data_length_;    // length before compress
  int32_t data_zlength_;   // length after compress, if without compresssion
  // data_length_= data_zlength_
  int64_t data_checksum_;  // record checksum
  ObRecordHeader();

  TO_STRING_KV(K_(magic),
               K_(header_length),
               K_(version),
               K_(header_checksum),
               K_(timestamp),
               K_(data_length),
               K_(data_zlength),
               K_(data_checksum));

  /**
   * sert magic number of record header
   *
   * @param magic magic number to set
   */
  inline void set_magic_num(const int16_t magic)
  {
    magic_ = magic;
  }

  /**
   * after deserialize the record header, check whether the
   * payload data is compressed
   *
   * @return bool if payload is compressed, return true, else
   *         return false
   */
  inline bool is_compressed_data() const
  {
    return (data_length_ != data_zlength_);
  }

  /**
   * set checksum of record header
   */
  void set_header_checksum();

  /**
   * check whether the checksum of record header is correct after
   * deserialize record header
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR
   */
  int check_header_checksum() const;


  /**
   * this method should use after deserialization,use it to check
   * check_sum of record
   *
   * @param buf record buf
   * @param len record len
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR
   */
  int check_payload_checksum(const char *buf, const int64_t len) const;

  /**
   * after read a record and record_header, should check it
   *
   * @param buf record and record_header byte string which is read
   *            from disk
   * @param len total length of record and record_header
   * @param magic magic number to check
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR
   */
  static int check_record(const char *buf, const int64_t len, const int16_t magic);

  /**
   * if user deserializes record header, he can use this fucntion
   * to check record
   *
   * @param record_header record header to check
   * @param payload_buf payload data following the record header
   * @param payload_len payload data length
   * @param magic magic number to check
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR
   */
  static int check_record(const ObRecordHeader &record_header,
                          const char *payload_buf,
                          const int64_t payload_len,
                          const int16_t magic);

  /**
   * give the buffer of record and record size, check whether the
   * record is ok, return the record header, payload buffer, and
   * payload buffer size
   *
   * @param ptr pointer of record buffer
   * @param size size of record buffer
   * @param magic magic number to check
   * @param header [out] record header to return
   * @param payload_ptr [out] payload pointer to return
   * @param payload_size [out] payload data size
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR or OB_INVALID_ARGUMENT
   */
  static int check_record(const char *ptr, const int64_t size,
                          const int16_t magic, ObRecordHeader &header,
                          const char *&payload_ptr, int64_t &payload_size);

  /**
   * give the buffer of record and record size, doesn't check
   * whether the record is ok, return the record header, payload
   * buffer, and payload buffer size
   *
   * @param ptr pointer of record buffer
   * @param size size of record buffer
   * @param header [out] record header to return
   * @param payload_ptr [out] payload pointer to return
   * @param payload_size [out] payload data size
   *
   * @return int if success, return OB_SUCCESS, else return
   *         OB_ERROR or OB_INVALID_ARGUMENT
   */
  static int get_record_header(const char *ptr, const int64_t size,
                               ObRecordHeader &header,
                               const char *&payload_ptr, int64_t &payload_size);

  NEED_SERIALIZE_AND_DESERIALIZE;
};

static const int16_t OB_RECORD_HEADER_LENGTH = sizeof(ObRecordHeader);

} // namespace Oceanbase::common
}// namespace Oceanbase
#endif
