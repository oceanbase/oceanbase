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

#ifndef OB_ADMIN_LOG_EXECUTOR_H_
#define OB_ADMIN_LOG_EXECUTOR_H_
#include "../ob_admin_executor.h"
#include "share/ob_admin_dump_helper.h"
namespace oceanbase
{
namespace tools
{
class ObAdminLogExecutor : public ObAdminExecutor
{
public:
  ObAdminLogExecutor() : mutator_str_buf_(nullptr), mutator_buf_size_(0){}
  virtual ~ObAdminLogExecutor();
  virtual int execute(int argc, char *argv[]);

private:
  void print_usage();
  int dump_log(int argc, char **argv);
  int decompress_log(int argc, char **argv);
  int dump_meta(int argc, char **argv);
  int dump_tx_format(int argc, char **argv);
  int dump_filter(int argc, char **argv);
  int stat(int argc, char **argv);
  int parse_options(int argc, char *argv[]);
  int dump_all_blocks_(int argc, char **argv, share::LogFormatFlag flag);
  int dump_single_block_(const char *block_path,
                         share::ObAdminMutatorStringArg &str_arg);
  int dump_single_meta_block_(const char *block_path,
                              share::ObAdminMutatorStringArg &str_arg);
  int dump_single_log_block_(const char *block_path,
                             share::ObAdminMutatorStringArg &str_arg);
  int alloc_mutator_string_buf_();
  int concat_file_(const char *first_path, const char *second_path);
private:
  const static int64_t MAX_TX_LOG_STRING_SIZE = 5*1024*1024;
  const static int64_t MAX_DECOMPRESSED_BUF_SIZE = palf::MAX_LOG_BODY_SIZE;


  char *mutator_str_buf_;
  int64_t mutator_buf_size_;
  char *decompress_buf_;
  int64_t decompress_buf_size_;
  share::ObAdminLogDumpFilter filter_;
};
}
}
#endif
