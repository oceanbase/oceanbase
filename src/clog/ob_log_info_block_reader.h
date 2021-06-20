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

#ifndef OCEANBASE_CLOG_OB_LOG_PARTITION_META_READER_
#define OCEANBASE_CLOG_OB_LOG_PARTITION_META_READER_

#include "ob_log_direct_reader.h"
#include "ob_log_reader_interface.h"

namespace oceanbase {
namespace clog {
class ObLogInfoBlockReader : public ObILogInfoBlockReader {
public:
  ObLogInfoBlockReader();
  virtual ~ObLogInfoBlockReader();

  int init(ObILogDirectReader* direct_reader);
  bool is_inited()
  {
    return is_inited_;
  }
  void reuse();
  void destroy();
  int read_info_block_data(const ObReadParam& param, ObReadRes& res)
  {
    ObReadCost dummy_cost;
    return read_info_block_data(param, res, dummy_cost);
  }
  int read_info_block_data(const ObReadParam& param, ObReadRes& res, ObReadCost& cost);

private:
  int direct_read_info_block_data(const ObReadParam& param, ObReadRes& res, ObReadCost& cost);

private:
  bool is_inited_;
  ObILogDirectReader* direct_reader_;
  ObAlignedBuffer result_buffer_;
  ObReadBuf rbuf_;
};
}  // namespace clog
}  // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_PARTITION_META_READER_
