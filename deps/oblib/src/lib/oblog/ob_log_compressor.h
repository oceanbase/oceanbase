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

#ifndef OB_LOG_COMPRESSOR_H_
#define OB_LOG_COMPRESSOR_H_

#include "lib/thread/thread_pool.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/list/ob_list.h"
#include "lib/queue/ob_fixed_queue.h"

namespace oceanbase {
namespace common {

class ObCompressor;
class ObString;
class ObMalloc;

class ObLogCompressor final : public lib::ThreadPool {
public:
  ObLogCompressor();
  virtual ~ObLogCompressor();
  static ObString get_compression_file_name(const ObString &file_name);
  int init();
  void destroy();
  int append_log(const ObString &file_name);
  ObCompressor *get_compressor();

private:
  int log_compress_block(char *dest, size_t dest_size, const char *src, size_t src_size, size_t &return_size);
  void log_compress();
  void run1() override;
  int start() override;
  void stop() override;
  void wait() override;

private:
  bool is_inited_;
  bool has_stoped_;
  ObFixedQueue<ObString> file_list_;
  ObThreadCond log_compress_cond_;
  ObCompressor *compressor_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* OB_LOG_COMPRESSOR_H_ */
