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

#ifndef OCEANBASE_LOGSERVICE_PALF_ENV_
#define OCEANBASE_LOGSERVICE_PALF_ENV_
#include <stdint.h>
#include "rpc/frame/ob_req_transport.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/function/ob_function.h"
#include "palf_env_impl.h"
namespace oceanbase
{
namespace commom
{
class ObAddr;
}

namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}

namespace palf
{
class PalfRoleChangeCb;
class PalfHandle;
class PalfDiskOptions;
class ILogBlockPool;

class PalfEnv
{
  friend class LogRequestHandler;
public:
  // static interface
  // create the palf env with the specified "base_dir".
  // store a pointer to a heap-allocated(may be allocate by a specified allocator) in "palf_env",
  // and return OB_SUCCESS on success.
  // store a NULL pointer ) in "palf_env", and return errno on fail.
  // caller should used destroy_palf_env to delete "palf_env" when it is no longer used.
  static int create_palf_env(const PalfOptions &options,
                             const char *base_dir,
                             const common::ObAddr &self,
                             rpc::frame::ObReqTransport *transport,
                             common::ObILogAllocator *alloc_mgr,
                             ILogBlockPool *log_block_pool,
                             PalfMonitorCb *monitor,
                             PalfEnv *&palf_env);
  // static interface
  // destroy the palf env, and set "palf_env" to NULL.
  static void destroy_palf_env(PalfEnv *&palf_env);

public:
  PalfEnv();
  ~PalfEnv();

  // 迁移场景目的端副本创建接口
  // @param [in] id，待创建日志流的标识符
  // @param [in] access_mode，palf access mode
  // @param [in] palf_base_info，palf的日志起点信息
  // @param [out] handle，创建成功后生成的palf_handle对象
  int create(const int64_t id,
             const AccessMode &access_mode,
             const PalfBaseInfo &palf_base_info,
             PalfHandle &handle);

  // 打开一个id对应的Paxos Replica，返回文件句柄
  int open(int64_t id, PalfHandle &handle);

  // 关闭一个句柄
  void close(PalfHandle &handle);

  // 删除id对应的Paxos Replica，会同时删除物理文件；
  int remove(int64_t id);

  // @brief get palf disk usage
  // @param [out] used_size_byte
  // @param [out] total_size_byte, if in shrinking status, total_size_byte is the value after shrinking.
  // NB: total_size_byte may be smaller than used_size_byte.
  int get_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte);

  // @brief get stable disk usage
  // @param [out] used_size_byte
  // @param [out] total_size_byte, if in shrinking status, total_size_byte is the value before shrinking.
  int get_stable_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte);

  // @brief update options
  // @param [in] options
  int update_options(const PalfOptions &options);
  // @brief get current options
  // @param [out] options
  int get_options(PalfOptions &options);
  // @brief check the disk space used to palf whether is enough
  bool check_disk_space_enough();
  // for failure detector
  // @brief get last io worker start time
  // @param [out] last working time
  // last_working_time will be set as current time when a io task begins,
  // and will be reset as OB_INVALID_TIMESTAMP when an io task ends, atomically.
  int get_io_start_time(int64_t &last_working_time);
  // @brief iterate each PalfHandle of PalfEnv and execute 'func'
  int for_each(const ObFunction<int(const PalfHandle&)> &func);
  // just for LogRpc
  palf::IPalfEnvImpl *get_palf_env_impl() { return &palf_env_impl_; }
  // should be removed in version 4.2.0.0
  int update_replayable_point(const SCN &replayable_scn);
private:
  int start_();
  void stop_();
  void wait_();
  void destroy_();
private:
  // the implmention of PalfEnv
  palf::PalfEnvImpl palf_env_impl_;
};
} // end namespace palf
} // end namespace oceanbase
#endif
