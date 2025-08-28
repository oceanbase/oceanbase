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
 
#ifdef TEST_MDS_TRANSACTION
#include "example_user_helper_define.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "deps/oblib/src/common/meta_programming/ob_type_traits.h"

namespace oceanbase {
namespace unittest {

using namespace storage;
using namespace mds;

MdsTableHandle TestMdsTable;

const storage::mds::MdsWriter ExampleUserHelperCtx::get_writer() const
{
  return storage::mds::MdsWriter(transaction::ObTransID(0));
}

int ExampleUserHelperFunction1::on_register(const char* buf,
                                            const int64_t len,
                                            storage::mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t test_value;
  int64_t pos = 0;
  if (OB_FAIL(serialization::decode(buf, len, pos, test_value))) {
    MDS_LOG(ERROR, "[UNITTEST] ExampleUserHelperFunction1 fail to deserialize", KR(ret));
  } else {
    MDS_LOG(INFO, "[UNITTEST] ExampleUserHelperFunction1 call on_register with helper", K(test_value));
  }
  return ret;
}

int ExampleUserHelperFunction1::on_replay(const char* buf,
                                        const int64_t len,
                                        const share::SCN &scn, // 日志scn
                                        storage::mds::BufferCtx &ctx)
{
  UNUSED(scn);
  return on_register(buf, len, ctx);
}

bool ExampleUserHelperFunction1::check_can_do_tx_end(const bool is_willing_to_commit,
                                                     const bool for_replay,
                                                     const share::SCN &log_scn,
                                                     const char *buf,
                                                     const int64_t buf_len,
                                                     storage::mds::BufferCtx &ctx,
                                                     const char *&can_not_do_reason)
{
  static_assert(OB_TRAIT_HAS_CHECK_CAN_DO_TX_END(ExampleUserHelperFunction1), "static check failed");
  bool ret = true;
  UNUSED(is_willing_to_commit);
  UNUSED(for_replay);
  UNUSED(log_scn);
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(ctx);
  static int call_times = 0;
  if (call_times++ < 5) {
    ret = false;
    can_not_do_reason = "JUST FOR TEST";
  }
  return ret;
}

int ExampleUserHelperFunction2::on_register(const char* buf,
                                            const int64_t len,
                                            storage::mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t test_value;
  int64_t pos = 0;
  if (OB_FAIL(serialization::decode(buf, len, pos, test_value))) {
    MDS_LOG(ERROR, "[UNITTEST] ExampleUserHelperFunction2 fail to deserialize", KR(ret));
  } else {
    ExampleUserData1 data(test_value);
    MdsCtx &mds_ctx = static_cast<MdsCtx &>(ctx);
    if (OB_FAIL(TestMdsTable.set(data, mds_ctx))) {
      MDS_LOG(ERROR, "[UNITTEST] ExampleUserHelperFunction2 fail to set mdstable", KR(ret));
    } else {
      MDS_LOG(INFO, "[UNITTEST] ExampleUserHelperFunction2 call on_register with helper", K(test_value));
    }
  }
  return ret;
}

int ExampleUserHelperFunction2::on_replay(const char* buf,
                                        const int64_t len,
                                        const share::SCN &scn, // 日志scn
                                        storage::mds::BufferCtx &ctx)
{
  UNUSED(scn);
  return on_register(buf, len, ctx);
}

int ExampleUserHelperFunction3::on_register(const char* buf,
                                            const int64_t len,
                                            storage::mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t test_value;
  int64_t pos = 0;
  if (OB_FAIL(serialization::decode(buf, len, pos, test_value))) {
    MDS_LOG(ERROR, "[UNITTEST] ExampleUserHelperFunction3 fail to deserialize", KR(ret));
  } else {
    MDS_LOG(INFO, "[UNITTEST] ExampleUserHelperFunction3 call on_register with helper", K(test_value));
  }
  return ret;
}

int ExampleUserHelperFunction3::on_replay(const char* buf,
                                        const int64_t len,
                                        const share::SCN &scn, // 日志scn
                                        storage::mds::BufferCtx &ctx)
{
  UNUSED(scn);
  return on_register(buf, len, ctx);
}

void ExampleUserHelperCtx::on_redo(const share::SCN &)
{
  MDS_LOG(INFO, "[UNITTEST] call on_redo with ctx", K(++call_times_));
}

void ExampleUserHelperCtx::before_prepare()
{
  MDS_LOG(INFO, "[UNITTEST] call before_prepare with ctx", K(++call_times_));
}

void ExampleUserHelperCtx::on_prepare(const share::SCN &prepare_version)
{
  MDS_LOG(INFO, "[UNITTEST] call on_prepare with ctx", K(++call_times_), K(prepare_version));
}

void ExampleUserHelperCtx::on_commit(const share::SCN &commit_version, const share::SCN &)
{
  MDS_LOG(INFO, "[UNITTEST] call on_commit with ctx", K(++call_times_));
}

void ExampleUserHelperCtx::on_abort(const share::SCN &end_scn)
{
  MDS_LOG(INFO, "[UNITTEST] call on_abort with ctx", K(++call_times_), K(end_scn));
}


}
}
#endif