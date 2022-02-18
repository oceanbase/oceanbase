/*
 * ob_admin_io_executor.h
 *
 *  Created on: Aug 29, 2017
 *      Author: yuzhong.zhao
 */

#ifndef OB_ADMIN_IO_EXECUTOR_H_
#define OB_ADMIN_IO_EXECUTOR_H_
#include "../ob_admin_executor.h"

namespace oceanbase
{
namespace tools
{

class ObAdminIOExecutor : public ObAdminExecutor
{
public:
  ObAdminIOExecutor();
  virtual ~ObAdminIOExecutor();
  virtual int execute(int argc, char *argv[]);
  void reset();
private:
  static const int64_t DEFAULT_BENCH_FILE_SIZE = 1024L * 1024L * 1024L * 100L;
  int parse_cmd(int argc, char *argv[]);
  void print_usage();
  const char *conf_dir_;
  const char *data_dir_;
  const char *file_size_;
};

}
}

#endif /* OB_ADMIN_IO_EXECUTOR_H_ */
