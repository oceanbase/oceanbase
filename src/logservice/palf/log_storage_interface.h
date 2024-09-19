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

#ifndef OCEANBASE_LOGSERVICE_I_LOG_STORAGE_
#define OCEANBASE_LOGSERVICE_I_LOG_STORAGE_
#include <cstdint>
namespace oceanbase
{
namespace palf
{
class ReadBuf;
class LSN;
class LogIOContext;
class ILogStorage
{
public:
  // @retval
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ERR_OUT_OF_UPPER_BOUND
  //   OB_ERR_OUT_OF_LOWER_BOUND
  //   OB_ERR_UNEXPECTED, file maybe deleted by human.
  virtual int pread(const LSN &lsn,
                    const int64_t in_read_size,
                    ReadBuf &read_buf,
                    int64_t &out_read_size,
                    LogIOContext &io_ctx) = 0;
};
}
}
#endif
