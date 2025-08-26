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

#ifndef OCEANBASE_LOGSERVICE_IPALF_ENV_
#define OCEANBASE_LOGSERVICE_IPALF_ENV_

#include <stdint.h>

#include "interface_structs.h"
#include "share/scn.h"
#include "lib/function/ob_function.h"
#include "ipalf_handle.h"

namespace oceanbase
{
namespace palf
{
class PalfEnv;
}
namespace libpalf
{
class LibPalfEnv;
}
namespace ipalf
{
class IPalfEnv;

// caller maintains lifecycle of palf env
int create_palf_env(PalfEnvCreateParams *params, palf::PalfEnv *&palf_env);
#ifdef OB_BUILD_SHARED_LOG_SERVICE
int create_palf_env(LibPalfEnvCreateParams *params, libpalf::LibPalfEnv *&libpalf_env);
#endif
void destroy_palf_env(IPalfEnv *&ipalf_env);

class IPalfEnv
{
public:
  IPalfEnv() {}
  virtual ~IPalfEnv() {}

public:
  virtual bool operator==(const IPalfEnv &rhs) const = 0;

  // 迁移场景目的端副本创建接口
  // @param [in] id，待创建日志流的标识符
  // @param [in] access_mode，palf access mode
  // @param [in] palf_base_info，palf的日志起点信息
  // @param [out] handle，创建成功后生成的palf_handle对象
  virtual int create(const int64_t id,
             const AccessMode &access_mode,
             const palf::PalfBaseInfo &palf_base_info,
             IPalfHandle *&handle) = 0;
  virtual int start() = 0;

  // Load a palf replica handle for restart.
  // - shared nothing: equivalent to `open`
  // - shared log service: load a palf replica handle which has been created in logservice
  virtual int load(const int64_t id, IPalfHandle *&handle) = 0;

  // 打开一个id对应的Paxos Replica，返回文件句柄
  virtual int open(const int64_t id, IPalfHandle *&handle) = 0;

  // 关闭一个句柄
  virtual int close(IPalfHandle *&handle) = 0;

  // 删除id对应的Paxos Replica，会同时删除物理文件；
  virtual int remove(int64_t id) = 0;
  virtual int for_each(const ObFunction<int(const IPalfHandle&)> &func) = 0;
  // should be removed in version 4.2.0.0
  virtual int update_replayable_point(const share::SCN &replayable_scn) = 0;

  virtual int advance_base_lsn(int64_t id, palf::LSN base_lsn) = 0;

  virtual int64_t get_tenant_id() = 0;
};

} // end namespace ipalf
} // end namespace logservice

#endif