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

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/ob_mview_mds.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::observer;
using namespace oceanbase::transaction;

OB_SERIALIZE_MEMBER(ObMViewOpArg,
                    table_id_,
                    mview_op_type_,
                    read_snapshot_,
                    parallel_);

int ObMViewMdsOpHelper::on_register(
      const char* buf,
      const int64_t len,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMViewMdsOpCtx &user_ctx = static_cast<ObMViewMdsOpCtx&>(ctx);
  ObMViewOpArg &arg = user_ctx.get_arg();
  transaction::ObTransID tx_id = user_ctx.get_writer().writer_id_;

  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize transfer dest prepare info", KR(ret), K(len), K(pos));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else {
    int64_t ts = ObTimeUtil::current_time();
    MTL(ObMViewMaintenanceService*)->update_mview_mds_ts(ts);
  }
  return ret;
}

int ObMViewMdsOpHelper::on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObLSHandle ls_handle;
  ObMViewMdsOpCtx &user_ctx = static_cast<ObMViewMdsOpCtx&>(ctx);
  ObMViewOpArg &arg = user_ctx.get_arg();
  transaction::ObTransID tx_id = user_ctx.get_writer().writer_id_;

  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize", KR(ret), K(len), K(pos));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else {
    int64_t ts = ObTimeUtil::current_time();
    MTL(ObMViewMaintenanceService*)->update_mview_mds_ts(ts);
  }
  return ret;
}

void ObMViewMdsOpCtx::reset()
{
  op_scn_.reset();
  arg_.reset();
}

int ObMViewOpArg::assign(const ObMViewOpArg& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  mview_op_type_ = other.mview_op_type_;
  read_snapshot_ = other.read_snapshot_;
  parallel_ = other.parallel_;
  return ret;
}

int ObMViewMdsOpCtx::assign(const ObMViewMdsOpCtx &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(arg_.assign(other.arg_))) {
    LOG_WARN("transfer dest prepare info assign failed", KR(ret), K(other));
  } else {
    writer_ = other.writer_;
    op_scn_ = other.op_scn_;
  }
  return ret;
}

void ObMViewMdsOpCtx::set_writer(const mds::MdsWriter &writer)
{
  writer_.writer_type_ = writer.writer_type_;
  writer_.writer_id_ = writer.writer_id_;
}

const mds::MdsWriter ObMViewMdsOpCtx::get_writer() const { return writer_; }

void ObMViewMdsOpCtx::on_redo(const share::SCN &redo_scn)
{
  op_scn_ = redo_scn;
  transaction::ObTransID tx_id = writer_.writer_id_;
  int64_t ts = ObTimeUtil::current_time();
  MTL(ObMViewMaintenanceService*)->update_mview_mds_ts(ts);
  LOG_INFO("mview mds on_redo", K(tx_id), KPC(this), K(redo_scn));
}

void ObMViewMdsOpCtx::on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  int64_t ts = ObTimeUtil::current_time();
  MTL(ObMViewMaintenanceService*)->update_mview_mds_ts(ts);
  LOG_INFO("mview mds on_commit", K(tx_id), KPC(this), K(commit_scn), K(commit_version));
}

void ObMViewMdsOpCtx::on_abort(const share::SCN &abort_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  int64_t ts = ObTimeUtil::current_time();
  MTL(ObMViewMaintenanceService*)->update_mview_mds_ts(ts);
  LOG_INFO("mview mds on_abort", K(tx_id), KPC(this), K(abort_scn));
}

int ObMViewMdsOpHelper::register_mview_mds(const uint64_t tenant_id, const ObMViewOpArg &arg, ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = nullptr;
  ObArenaAllocator allocator("MVIEW_MDS");
  char *buf = nullptr;
  int64_t size = arg.get_serialize_size();
  int64_t pos = 0;
  uint64_t data_version = 0;
  ObRegisterMdsFlag flag;
  flag.need_flush_redo_instantly_ = true;
  flag.mds_base_scn_.reset();
  share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data_version", KR(ret));
  } else if (data_version < DATA_VERSION_4_3_5_1) {
    // do nothing
  } else if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(sql_client.get_connection()))) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     LOG_WARN("failed to allocate", K(ret));
  } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize arg", K(ret));
  } else if (OB_FAIL(conn->register_multi_data_source(tenant_id,
                                                      ls_id,
                                                      ObTxDataSourceType::MVIEW_MDS_OP,
                                                      buf,
                                                      size,
                                                      flag))) {
    LOG_WARN("register mview mds failed", KR(ret));
  } else {
    LOG_INFO("register mview mds succ", K(tenant_id), K(arg));
  }
  return ret;
}


} // end storage
} // end oceanbase
