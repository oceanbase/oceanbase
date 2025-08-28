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

#ifndef OCEABASE_MAJOR_MV_MERGE_INFO_
#define OCEABASE_MAJOR_MV_MERGE_INFO_

#include "share/scn.h"
#include "storage/multi_data_source/buffer_ctx.h"

namespace oceanbase
{
namespace storage
{

struct ObMajorMVMergeInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObMajorMVMergeInfo();
  ~ObMajorMVMergeInfo() = default;
  bool is_valid() const;
  void reset();
  bool need_update_major_mv_merge_scn()
  { return major_mv_merge_scn_ < major_mv_merge_scn_safe_calc_; }
  void operator=(const ObMajorMVMergeInfo &other);
  int init(const share::SCN &major_mv_merge_scn,
           const share::SCN &major_mv_merge_scn_safe_calc,
           const share::SCN &major_mv_merge_scn_publish);

  TO_STRING_KV(K_(major_mv_merge_scn), K_(major_mv_merge_scn_safe_calc), K_(major_mv_merge_scn_publish));

  share::SCN major_mv_merge_scn_;
  share::SCN major_mv_merge_scn_safe_calc_;
  share::SCN major_mv_merge_scn_publish_;
};

struct ObUpdateMergeScnArg final
{
  OB_UNIS_VERSION(1);
public:
  ObUpdateMergeScnArg();
  ~ObUpdateMergeScnArg() = default;
  bool is_valid() const;
  void reset();
  int init(const share::ObLSID &ls_id, const share::SCN &merge_scn);
  TO_STRING_KV(K_(ls_id), K_(merge_scn));

  share::ObLSID ls_id_;
  share::SCN merge_scn_;
};

struct ObMVPublishSCNHelper 
{
  static int on_register(
      const char *buf,
      const int64_t len,
      mds::BufferCtx &ctx);

  static int on_replay(
      const char *buf,
      const int64_t len,
      const share::SCN scn,
      mds::BufferCtx &ctx);
};

struct ObMVNoticeSafeHelper
{
  static int on_register(
      const char *buf,
      const int64_t len,
      mds::BufferCtx &ctx);

  static int on_replay(
      const char *buf,
      const int64_t len,
      const share::SCN scn,
      mds::BufferCtx &ctx);
};

struct ObMVUpdateSCNHelper 
{
  static int on_register(
      const char *buf,
      const int64_t len,
      mds::BufferCtx &ctx)
  {
    return OB_SUCCESS;
  }

  static int on_replay(
      const char *buf,
      const int64_t len,
      const share::SCN scn,
      mds::BufferCtx &ctx)
  {
    return OB_SUCCESS;
  }
};

struct ObMVMergeSCNHelper 
{
  static int on_register(
      const char *buf,
      const int64_t len,
      mds::BufferCtx &ctx);

  static int on_replay(
      const char *buf,
      const int64_t len,
      const share::SCN scn,
      mds::BufferCtx &ctx);
};

struct ObMVCheckReplicaHelper
{
  static int get_and_update_merge_info(
      const share::ObLSID &ls_id, 
      ObMajorMVMergeInfo &info);
  static int check_can_add_member(
      const common::ObAddr &server,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const uint64_t rpc_timeout);
  static int get_merge_info(
      const share::ObLSID &ls_id, 
      ObMajorMVMergeInfo &info);
};

struct ObUnUseCtx : public mds::BufferCtx
{
  TO_STRING_EMPTY();
  int serialize(char *buf, const int64_t len, int64_t &pos) const { return OB_SUCCESS; }
  int deserialize(const char *buf, const int64_t len, int64_t &pos) { return OB_SUCCESS; }
  int64_t get_serialize_size() const { return 0; }
  const mds::MdsWriter get_writer() const { return mds::MdsWriter(mds::WriterType::TRANSACTION, 0); }
};

}
}

#endif
