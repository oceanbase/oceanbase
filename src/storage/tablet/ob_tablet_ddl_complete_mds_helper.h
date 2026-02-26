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

#ifndef OCEANBASE_STORAGE_OB_TABLET_DDL_COMPLETE_MDS_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_DDL_COMPLETE_MDS_HELPER

#include <stdint.h>
#include "lib/worker.h"
#include "lib/container/ob_iarray.h"
#include "storage/tx/ob_trans_define.h"
#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
namespace share
{
class ObLSID;
class SCN;
}

namespace common
{
class ObTabletID;
}


namespace storage
{
namespace mds
{
struct BufferCtx;
}

class ObTabletDDLCompleteMdsUserData;

class ObTabletDDLCompleteArg
{
public:
  ObTabletDDLCompleteArg();
  ~ObTabletDDLCompleteArg();
  bool is_valid() const;
  void reset();
  int assign(const ObTabletDDLCompleteArg &other);
  int set_storage_schema(const ObStorageSchema &other);
  ObStorageSchema *get_storage_schema() { return storage_schema_; }
  const ObStorageSchema *get_storage_schema() const { return storage_schema_; }
  int set_write_stat(const ObDDLWriteStat &write_stat) { return write_stat_.assign(write_stat); }
  ObDDLWriteStat &get_write_stat() { return write_stat_; }
  const ObDDLWriteStat &get_write_stat() const { return write_stat_; }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int from_mds_user_data(const ObTabletDDLCompleteMdsUserData &user_data);
  TO_STRING_KV(K_(has_complete), K_(ls_id), K_(tablet_id), K_(direct_load_type), K_(rec_scn), K_(start_scn), K_(data_format_version), K_(snapshot_version), K_(table_key), KPC_(storage_schema), K_(write_stat), K_(trans_id));
public:
  bool has_complete_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  /* ddl table merge param */
  ObDirectLoadType direct_load_type_;
  share::SCN rec_scn_;
  share::SCN start_scn_;
  int64_t data_format_version_;
  int64_t snapshot_version_;
  ObITable::TableKey table_key_;
  ObDDLWriteStat write_stat_;
  transaction::ObTransID trans_id_;
private:
  ObStorageSchema *storage_schema_;
  ObArenaAllocator allocator_;
};

class ObTabletDDLCompleteMdsHelper
{
public:
  static int on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char* buf, const int64_t len, const share::SCN scn, mds::BufferCtx &ctx);
  static int process(const char* buf, const int64_t len, const share::SCN &scn,
                     mds::BufferCtx &ctx, bool for_replay);
  static int record_ddl_complete_arg_to_mds(const ObTabletDDLCompleteArg &complete_arg,
                                            common::ObIAllocator &allocator);
  static int process_inc_major(mds::BufferCtx &ctx,
                               ObLSHandle &ls_handle,
                               const ObTabletID &tablet_id,
                               const ObTabletDDLCompleteMdsUserData &data,
                               const share::SCN &scn,
                               const bool for_replay);
  static int process_ddl(mds::BufferCtx &ctx,
                         ObLSHandle &ls_handle,
                         const ObTabletID &tablet_id,
                         const ObTabletDDLCompleteMdsUserData &data,
                         const share::SCN &scn,
                         const bool for_replay);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_CREATE_MDS_HELPER
