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

#ifndef OCEANBASE_COMMON_OB_DIRECT_LOG_READER_
#define OCEANBASE_COMMON_OB_DIRECT_LOG_READER_

#include "common/log/ob_single_log_reader.h"

namespace oceanbase
{
namespace common
{
class ObDirectLogReader : public ObSingleLogReader
{
public:
  ObDirectLogReader();
  virtual ~ObDirectLogReader();

  /**
   * @brief 从操作日志中读取一个更新操作
   * @param [out] cmd 日志类型
   * @param [out] log_seq 日志序号
   * @param [out] log_data 日志内容
   * @param [out] data_len 缓冲区长度
   * @return OB_SUCCESS: 如果成功;
   *         OB_READ_NOTHING: 从文件中没有读到数据
   *         others: 发生了错误.
   */
  int read_log(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t &data_len);
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_DIRECT_LOG_READER_
