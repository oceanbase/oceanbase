/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef UNITTEST_SHARE_MULTI_DATA_SOURCE_EXAMPLE_USER_HELPER_DEFINE_H
#define UNITTEST_SHARE_MULTI_DATA_SOURCE_EXAMPLE_USER_HELPER_DEFINE_H
#include "storage/multi_data_source/buffer_ctx.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/serialization.h"
#include "share/scn.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"

namespace oceanbase {
namespace unittest {

struct ExampleUserHelperFunction1 {
  static int on_register(const char* buf,
                         const int64_t len,
                         storage::mds::BufferCtx &ctx); // 出参，将对应修改记录在Ctx中

  static int on_replay(const char* buf,
                       const int64_t len,
                       const share::SCN &scn, // 日志scn
                       storage::mds::BufferCtx &ctx); // 备机回放
};

struct ExampleUserHelperFunction2 {
  static int on_register(const char* buf,
                         const int64_t len,
                         storage::mds::BufferCtx &ctx); // 出参，将对应修改记录在Ctx中

  static int on_replay(const char* buf,
                       const int64_t len,
                       const share::SCN &scn, // 日志scn
                       storage::mds::BufferCtx &ctx); // 备机回放
};

struct ExampleUserHelperFunction3 {
  static int on_register(const char* buf,
                         const int64_t len,
                         storage::mds::BufferCtx &ctx); // 出参，将对应修改记录在Ctx中

  static int on_replay(const char* buf,
                       const int64_t len,
                       const share::SCN &scn, // 日志scn
                       storage::mds::BufferCtx &ctx); // 备机回放
};

struct ExampleUserHelperCtx : public storage::mds::BufferCtx {
  ExampleUserHelperCtx() : call_times_(0) {}
  virtual const storage::mds::MdsWriter get_writer() const override;
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void before_prepare() override;
  virtual void on_prepare(const share::SCN &prepare_version) override;
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &scn) override;
  virtual void on_abort(const share::SCN &scn) override;
  int assign(const ExampleUserHelperCtx &rhs) {
    call_times_ = rhs.call_times_;
    return OB_SUCCESS;
  }
  int call_times_;// 这个类可以有自己的内部状态
  virtual int64_t to_string(char*, const int64_t buf_len) const { return 0; }
  // 同事务状态一起持久化以及恢复
  virtual int serialize(char*, const int64_t, int64_t&) const { return OB_SUCCESS; }
  virtual int deserialize(const char*, const int64_t, int64_t&) { return OB_SUCCESS; }
  virtual int64_t get_serialize_size(void) const { return 0; }
};

#ifndef TEST_MDS_TRANSACTION

inline int ExampleUserHelperFunction1::on_register(const char* buf,
                                            const int64_t len,
                                            storage::mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  return ret;
}

inline int ExampleUserHelperFunction1::on_replay(const char* buf,
                                        const int64_t len,
                                        const share::SCN &scn, // 日志scn
                                        storage::mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  return ret;
}

inline int ExampleUserHelperFunction2::on_register(const char*,
                                            const int64_t,
                                            storage::mds::BufferCtx &)
{
  int ret = OB_SUCCESS;
  return ret;
}

inline int ExampleUserHelperFunction2::on_replay(const char*,
                                        const int64_t,
                                        const share::SCN &, // 日志scn
                                        storage::mds::BufferCtx &)
{
  int ret = OB_SUCCESS;
  return ret;
}

inline int ExampleUserHelperFunction3::on_register(const char*,
                                            const int64_t,
                                            storage::mds::BufferCtx &)
{
  int ret = OB_SUCCESS;
  return ret;
}

inline int ExampleUserHelperFunction3::on_replay(const char* buf,
                                        const int64_t len,
                                        const share::SCN &scn, // 日志scn
                                        storage::mds::BufferCtx &ctx)
{
  UNUSED(scn);
  return on_register(buf, len, ctx);
}

inline const storage::mds::MdsWriter ExampleUserHelperCtx::get_writer() const
{
  return storage::mds::MdsWriter(storage::mds::WriterType::TRANSACTION, 1);
}

inline void ExampleUserHelperCtx::on_redo(const share::SCN &redo_scn)
{
  MDS_LOG(INFO, "[UNITTEST] call on_redo with ctx", K(++call_times_));
}

inline void ExampleUserHelperCtx::before_prepare()
{
  MDS_LOG(INFO, "[UNITTEST] call before_prepare with ctx", K(++call_times_));
}

inline void ExampleUserHelperCtx::on_prepare(const share::SCN &prepare_version)
{
  MDS_LOG(INFO, "[UNITTEST] call on_prepare with ctx", K(++call_times_));
}

inline void ExampleUserHelperCtx::on_commit(const share::SCN &commit_version, const share::SCN &scn)
{
  MDS_LOG(INFO, "[UNITTEST] call on_commit with ctx", K(++call_times_));
}

inline void ExampleUserHelperCtx::on_abort(const share::SCN &scn)
{
  MDS_LOG(INFO, "[UNITTEST] call on_abort with ctx", K(++call_times_));
}
#endif

}
}
#endif