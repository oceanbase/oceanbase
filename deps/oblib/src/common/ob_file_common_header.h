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

#ifndef OCEANBASE_COMMON_OB_FILE_COMMON_HEADER_H_
#define OCEANBASE_COMMON_OB_FILE_COMMON_HEADER_H_

#include "lib/ob_define.h"
#include "lib/utility/serialization.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{

struct ObFileCommonHeader final
{
public:
  static const uint16_t OB_FILE_COMMON_HEADER_VERSION = 1;
  static const uint16_t OB_FILE_COMMON_HEADER_MAGIC = 0xC12A;

  int16_t magic_;
  int16_t header_version_;
  int16_t header_length_;
  int16_t header_checksum_;
  int16_t payload_version_;
  // length before compression
  int32_t payload_length_;
  // length after compression. if without compression, data_zlength_= data_length_
  int32_t payload_zlength_;
  int64_t payload_checksum_;
  ObFileCommonHeader();
  ~ObFileCommonHeader();

  TO_STRING_KV(K_(magic), K_(header_version), K_(header_length), K_(header_checksum),
               K_(payload_version), K_(payload_length), K_(payload_zlength), K_(payload_checksum));

  /**
   * after deserialize the file header, check whether the payload data is compressed
   *
   * @return bool if payload is compressed, return true, else return false
   */
  inline bool is_compressed_data() const { return (payload_length_ != payload_zlength_); }

  /**
   * set checksum of file header
   */
  void set_header_checksum();

  /**
   * check whether the checksum of file header is correct after deserialize file header
   *
   * @return int if success, return OB_SUCCESS, else return OB_CHECKSUM_ERROR
   */
  int check_header_checksum() const;

  int16_t calc_header_checksum() const;

  /**
   * this method should use after deserialization, use it to check checksum of file
   *
   * @param buf file buf
   * @param len file len
   *
   * @return int if success, return OB_SUCCESS, else return OB_CHECKSUM_ERROR
   */
  int check_payload_checksum(const char *buf, const int64_t len) const;

  NEED_SERIALIZE_AND_DESERIALIZE;
};


class ObFileCommonHeaderUtil
{
public:
  template<typename T>
  static int serialize(const T &payload, const int16_t payload_version, char *buf,
                       const int64_t buf_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    ObFileCommonHeader header;
    const int64_t header_length = header.get_serialize_size();
    const int64_t payload_length = payload.get_serialize_size();
    const int64_t ori_pos = pos;
    int64_t header_pos = ori_pos;
    int64_t data_pos = ori_pos + header_length;
    if (OB_ISNULL(buf) || OB_UNLIKELY((buf_len < (pos + header_length + payload_length)) || (pos < 0))) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(pos), K(buf_len), K(header_length), K(payload_length));
    } else if (OB_FAIL(payload.serialize(buf, buf_len, data_pos))) {
      COMMON_LOG(WARN, "fail to serialize payload", K(ret), K(payload), K(buf_len), K(data_pos));
    } else if (OB_UNLIKELY(data_pos != (ori_pos + header_length + payload_length))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "data pos is unexpected", K(ret), K(data_pos), K(ori_pos), K(header_length), K(payload_length));
    } else {
      header.payload_version_ = payload_version;
      header.payload_length_ = static_cast<int32_t>(payload_length);
      header.payload_zlength_ = header.payload_length_;
      header.payload_checksum_ = ob_crc64(buf + ori_pos + header_length, payload_length);
      header.set_header_checksum();
      if (OB_FAIL(header.serialize(buf, buf_len, header_pos))) {
        COMMON_LOG(WARN, "fail to serialize header", K(ret), K(header), K(buf_len), K(header_pos));
      } else if (OB_UNLIKELY(header_pos != header_length)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "header pos is unexpected", K(ret), K(header_pos), K(header_length));
      } else {
        pos = data_pos;
      }
    }
    return ret;
  }

  // Note: this deserialize function is only for payload that stores version inside itself.
  // payload that does not store version inside itself, and stores version inside
  // ObFileCommonHeader::payload_version_ need to implement deserialize function independently.
  template<typename T>
  static int deserialize(const char *buf, const int64_t buf_len, int64_t &pos, T &payload)
  {
    int ret = OB_SUCCESS;
    const int64_t ori_pos = pos;
    ObFileCommonHeader header;
    const int64_t header_serialize_size = header.get_serialize_size();
    if (OB_ISNULL(buf) || OB_UNLIKELY((buf_len < (pos + header_serialize_size)) || (pos < 0))) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len), K(pos), K(header_serialize_size));
    } else if (OB_FAIL(header.deserialize(buf, buf_len, pos))) {
      COMMON_LOG(WARN, "fail to deserialize header", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_UNLIKELY((pos - ori_pos) != header_serialize_size)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "unexpected pos", K(ret), K(ori_pos), K(pos), K(header_serialize_size));
    } else if (OB_FAIL(header.check_header_checksum())) {
      COMMON_LOG(WARN, "fail to check header checksum", K(ret), K(header));
    } else if (OB_UNLIKELY((buf_len - pos) < header.payload_zlength_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "unexpected buf len", K(ret), K(buf_len), K(pos), K(header));
    } else if (OB_FAIL(header.check_payload_checksum(buf + pos, header.payload_zlength_))) {
      COMMON_LOG(WARN, "fail to check payload checksum", K(ret), K(pos), K(header));
    } else if (OB_FAIL(payload.deserialize(buf, buf_len, pos))) {
      COMMON_LOG(WARN, "fail to deserialize payload", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_UNLIKELY(pos != (ori_pos + header_serialize_size + header.payload_zlength_))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "unexpected pos", K(ret), K(pos), K(ori_pos), K(header_serialize_size), K(header));
    }
    return ret;
  }
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_FILE_COMMON_HEADER_H_
