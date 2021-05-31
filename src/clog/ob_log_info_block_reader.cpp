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

#include "ob_log_block.h"
#include "ob_log_info_block_reader.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace clog {
ObLogInfoBlockReader::ObLogInfoBlockReader() : is_inited_(false), direct_reader_(NULL), result_buffer_(), rbuf_()
{}

ObLogInfoBlockReader::~ObLogInfoBlockReader()
{
  destroy();
}

int ObLogInfoBlockReader::init(ObILogDirectReader* direct_reader)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (NULL == direct_reader) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument: direct_reader = NULL");
  } else if (OB_FAIL(result_buffer_.init(
                 CLOG_INFO_BLOCK_SIZE_LIMIT, CLOG_DIO_ALIGN_SIZE, ObModIds::OB_CLOG_INFO_BLK_HNDLR))) {
    CLOG_LOG(WARN, "alloc buf fail", K(ret));
  } else {
    direct_reader_ = direct_reader;
    rbuf_.buf_ = result_buffer_.get_align_buf();
    rbuf_.buf_len_ = result_buffer_.get_size();
    is_inited_ = true;
  }
  return ret;
}

void ObLogInfoBlockReader::reuse()
{
  result_buffer_.reuse();
}

void ObLogInfoBlockReader::destroy()
{
  result_buffer_.destroy();
  rbuf_.reset();
  direct_reader_ = NULL;
  is_inited_ = false;
}

int ObLogInfoBlockReader::read_info_block_data(const ObReadParam& param, ObReadRes& res, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(direct_read_info_block_data(param, res, cost))) {
    if (OB_READ_NOTHING != ret) {
      CLOG_LOG(WARN, "direct_read_info_block_data fail", K(ret), K(param), K(res), K(cost));
    }
  }
  return ret;
}

// 1. According to the trailed, get the start address of InfoBlock
// 2. Read data and check data integrity
int ObLogInfoBlockReader::direct_read_info_block_data(const ObReadParam& param, ObReadRes& res, ObReadCost& cost)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t start_pos = -1;
  file_id_t next_file_id = OB_INVALID_FILE_ID;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(direct_reader_->read_trailer(param, rbuf_, start_pos, next_file_id, cost))) {
    if (OB_READ_NOTHING == ret) {
      CLOG_LOG(TRACE, "read trailer fail", K(start_pos), K(next_file_id), K(ret));
    } else {
      CLOG_LOG(WARN, "read trailer fail", K(start_pos), K(next_file_id), K(ret));
    }
  } else {
    ObReadParam new_param;
    ObLogBlockMetaV2 block;
    new_param.file_id_ = param.file_id_;
    new_param.offset_ = static_cast<offset_t>(start_pos);
    new_param.read_len_ = param.read_len_ <= 0 ? OB_MAX_LOG_BUFFER_SIZE : param.read_len_;
    new_param.timeout_ = param.timeout_;
    if (OB_FAIL(direct_reader_->read_data_direct(new_param, rbuf_, res, cost))) {
      CLOG_LOG(WARN, "read data fail", "offset", new_param.offset_, "ret", ret);
    } else if (OB_FAIL(block.deserialize(res.buf_, res.data_len_, pos))) {
      CLOG_LOG(WARN, "block deserialize fail", "ret", ret);
    } else if (!block.check_meta_checksum()) {
      ret = OB_INVALID_DATA;
      CLOG_LOG(WARN, "block check fail", "block", to_cstring(block));
    } else if (!block.check_integrity(res.buf_ + block.get_serialize_size(), block.get_data_len())) {
      ret = OB_INVALID_DATA;
      CLOG_LOG(WARN, "block check fail", "block", to_cstring(block), "res", to_cstring(res), "pos", pos);
    } else {
      res.data_len_ = block.get_data_len();
      res.buf_ = res.buf_ + pos;
    }
  }
  return ret;
}
}  // end namespace clog
}  // end namespace oceanbase
