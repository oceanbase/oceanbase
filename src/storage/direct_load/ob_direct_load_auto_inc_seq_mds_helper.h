/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_MDS_HELPER_H_
#define OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_MDS_HELPER_H_

#include "storage/tx/ob_trans_define.h"
#include "share/ob_rpc_struct.h"
#include "storage/direct_load/ob_direct_load_auto_inc_seq_data.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
struct BufferCtx;
}

class ObDirectLoadAutoIncSeqArg
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadAutoIncSeqArg();
  ~ObDirectLoadAutoIncSeqArg();
  bool is_valid() const;
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(seq));
public:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObDirectLoadAutoIncSeqData seq_;
};

class ObDirectLoadAutoIncSeqMdsHelpler
{
public:
  static int on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char* buf, const int64_t len, const share::SCN scn, mds::BufferCtx &ctx);
  static int process(const char* buf, const int64_t len, const share::SCN &scn,
                     mds::BufferCtx &ctx, bool for_replay);
};

} // namespace storage
} // namespace oceanbase

#endif /* OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_MDS_HELPER_H_ */