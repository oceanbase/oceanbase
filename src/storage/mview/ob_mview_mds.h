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


#pragma once

#include "storage/multi_data_source/mds_ctx.h"

namespace oceanbase
{
namespace storage
{
enum MVIEW_OP_TYPE
{
  UNDEFINE_OP = 0,
  COMPLETE_REFRESH = 1,
  FAST_REFRESH = 2,
  PURGE_MLOG = 3
};

struct ObMViewOpArg
{
  OB_UNIS_VERSION(1);
public:
  ObMViewOpArg() { reset(); }
  void reset() {
    table_id_ = 0;
    mview_op_type_ = MVIEW_OP_TYPE::UNDEFINE_OP;
    read_snapshot_ = 0;
    parallel_ = 0;
  }
  ~ObMViewOpArg() { reset(); }
  bool is_valid() const { return table_id_ > 0 &&
                           mview_op_type_ > UNDEFINE_OP; }
  int assign(const ObMViewOpArg& other);

  TO_STRING_KV(K_(table_id), K_(mview_op_type), K_(read_snapshot),
      K_(parallel));
  int64_t table_id_;
  MVIEW_OP_TYPE mview_op_type_;
  int64_t read_snapshot_;
  int64_t parallel_;
};

class ObMViewMdsOpCtx : public mds::BufferCtx
{
  OB_UNIS_VERSION(1);
public:
  ObMViewMdsOpCtx() { reset(); }
  ~ObMViewMdsOpCtx() { reset(); }
  void reset();
  int assign(const ObMViewMdsOpCtx &other);
  virtual const mds::MdsWriter get_writer() const override;
  void set_writer(const mds::MdsWriter &writer);
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) override;
  virtual void on_abort(const share::SCN &abort_scn) override;
  ObMViewOpArg &get_arg() { return arg_; }
  const ObMViewOpArg &get_arg_const() const { return arg_; }

  TO_STRING_KV(K_(writer), K_(op_scn), K_(arg));
private:
  mds::MdsWriter writer_;
  share::SCN op_scn_;
  ObMViewOpArg arg_;
};

OB_SERIALIZE_MEMBER_TEMP(inline,
                         ObMViewMdsOpCtx,
                         writer_,
                         op_scn_,
                         arg_);

class ObMViewMdsOpHelper
{
public:
  static int on_register(
      const char* buf,
      const int64_t len,
      mds::BufferCtx &ctx);
  static int on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
  static int register_mview_mds(const uint64_t tenant_id, const ObMViewOpArg &arg, ObISQLClient &sql_client);
};



} // end storage
} // end oceanbase
