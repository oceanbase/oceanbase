/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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