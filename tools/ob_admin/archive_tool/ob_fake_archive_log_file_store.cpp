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

#define USING_LOG_PREFIX ARCHIVE
#include "ob_fake_archive_log_file_store.h"
#include "archive/ob_log_archive_struct.h"
#include "clog/ob_log_reader_interface.h"

namespace oceanbase
{
using namespace clog;
using namespace common;
using namespace archive;
namespace tools
{

void ObFakeArchiveLogFileStore::reset()
{
  is_inited_ = false;
  is_last_file_ = true;
  buf_ = NULL;
  buf_len_ = -1;
  file_id_ = -1;
}

int ObFakeArchiveLogFileStore::init(char *buf,
                                    int64_t buf_len,
                                    const int64_t file_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || file_id <=0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(file_id));
  } else {
    is_last_file_ = true;//工具默认每个文件都是最后一个文件, 末尾会有写失败的遗留数据
    buf_ = buf;
    buf_len_ = buf_len;
    file_id_ = file_id;
    is_inited_ = true;
  }
  return ret;
}

int ObFakeArchiveLogFileStore::get_file_meta(const uint64_t file_id,
                                             bool &is_last_log_file,
                                             int64_t &file_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(file_id_ != file_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file_id", K(ret));
  } else {
    is_last_log_file = is_last_file_;
    file_len = buf_len_;
  }
  return ret;
}

int ObFakeArchiveLogFileStore::read_data_direct(const ObArchiveReadParam &param,
                                                clog::ObReadBuf &rbuf,
                                                clog::ObReadRes &res)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid()
             || !rbuf.is_valid()
             || param.file_id_ != file_id_
             || param.read_len_ > rbuf.buf_len_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param), K(rbuf), K(buf_len_), K(file_id_));
  } else if (param.offset_ + param.read_len_ > buf_len_) {
    const int64_t data_len = buf_len_ - param.offset_;
    MEMCPY(rbuf.buf_, buf_ + param.offset_, data_len);
    res.buf_ = rbuf.buf_;
    res.data_len_ = data_len;
  } else {
    MEMCPY(rbuf.buf_, buf_ + param.offset_, param.read_len_);
    res.buf_ = rbuf.buf_;
    res.data_len_ = param.read_len_;
  }
  return ret;
}

}//end of namespace
}
