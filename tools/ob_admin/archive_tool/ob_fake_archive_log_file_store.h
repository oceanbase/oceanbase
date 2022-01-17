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

#ifndef OB_FAKE_ARCHIVE_LOG_FILE_STORE_H_
#define OB_FAKE_ARCHIVE_LOG_FILE_STORE_H_

#include "archive/ob_archive_log_file_store.h"

namespace oceanbase
{
namespace clog
{
struct ObReadBuf;
struct ObReadRes;
}

namespace archive
{
struct ObArchiveReadParam;
}
namespace tools
{
//used for ob_admin
class ObFakeArchiveLogFileStore : public archive::ObIArchiveLogFileStore
{
public:
  ObFakeArchiveLogFileStore() {reset();}
  virtual ~ObFakeArchiveLogFileStore(){reset();}
  void reset();
public:
  virtual int init() {return common::OB_NOT_SUPPORTED;};
  int init(char *buf, int64_t buf_len, const int64_t file_id);
public:
  virtual int get_file_meta(const uint64_t file_id, bool &is_last_log_file, int64_t &file_len);
  /*param:读取日志的文件file_id, offset, len 等信息
   * 因为源端存储介质的访问可能会出现卡住等情况，需要给本次读取加一个timeout
   * 在超时之前，该接口必须返回
   * 这个接口上层调用者保证offset+len未超过文件长度，filestore本身不需要判断
   * 所读取的数据是否越界
   * */
  //TODO(yaoying.yyy):param file_id_t
  virtual int read_data_direct(const archive::ObArchiveReadParam &param,
                               clog::ObReadBuf &rbuf,
                               clog::ObReadRes &res);
private:
  bool is_inited_;
  bool is_last_file_;
  char *buf_;
  int64_t buf_len_;
  int64_t file_id_;
};
}//end of namespace tools
}//end of namespace oceanbase

#endif /* OB_FAKE_ARCHIVE_LOG_FILE_STORE_H_ */
