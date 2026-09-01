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

#ifndef OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_HELPER_H
#define OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_HELPER_H

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_table_schema.h"
#include "common/ob_tablet_id.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet_truncate_mds_user_data.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
struct BufferCtx;
}

class ObTabletHandle;
class ObLS;
class ObLSHandle;
class ObLSTabletService;

struct ObTabletTruncateMdsArg final
{
  OB_UNIS_VERSION(1);
public:
  ObTabletTruncateMdsArg()
    : allocator_(),
      ls_id_(),
      tablet_id_(),
      schema_version_(OB_INVALID_VERSION),
      truncate_data_(),
      table_schema_()
  {}
  ~ObTabletTruncateMdsArg() = default;
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const lib::Worker::CompatMode compat_mode,
      const share::schema::ObTableSchema &input_schema,
      const int64_t schema_version);
  bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_id_.is_valid() && schema_version_ > 0;
  }
  void reset()
  {
    ls_id_.reset();
    tablet_id_.reset();
    schema_version_ = OB_INVALID_VERSION;
    truncate_data_.reset();
    table_schema_.reset();
    allocator_.reset();
  }

  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(schema_version), K_(truncate_data), K_(table_schema));

  ObArenaAllocator allocator_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t schema_version_;
  ObTabletTruncateMdsUserData truncate_data_;
  storage::ObCreateTabletSchema table_schema_;
};

class ObTabletTruncateMdsHelper
{
public:
  static int on_register(
      const char *buf,
      const int64_t len,
      mds::BufferCtx &ctx);
  static int on_replay(
      const char *buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx);

private:
  static int register_process(
      const ObTabletTruncateMdsArg &arg,
      mds::BufferCtx &ctx);
  static int replay_process(
      const ObTabletTruncateMdsArg &arg,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
  static int truncate_tablet_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const storage::ObCreateTabletSchema &table_schema,
      const int64_t schema_version,
      const bool for_replay,
      const share::SCN &scn,
      const ObTabletTruncateMdsUserData &truncate_data,
      mds::BufferCtx &ctx);
  static int set_tablet_truncate_mds_(
      ObLSTabletService *ls_tablet_service,
      ObTabletHandle &tablet_handle,
      const bool for_replay,
      const share::SCN &scn,
      const ObTabletTruncateMdsUserData &truncate_data,
      mds::BufferCtx &ctx);
  static int get_ls_(
      const share::ObLSID &ls_id,
      ObLSHandle &ls_handle);
  // Get tablet for replay. If the tablet's truncate_commit_scn is already >= scn,
  // it means this replay has already taken effect, so set skip = true.
  static int replay_get_tablet_(
      ObLS &ls,
      const common::ObTabletID &tablet_id,
      const share::SCN &scn,
      ObTabletHandle &tablet_handle,
      bool &skip);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_HELPER_H
