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

#ifndef OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_CTX_H
#define OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_CTX_H

#include "storage/multi_data_source/mds_ctx.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{
class ObLSTabletService;
namespace mds
{

class ObTabletTruncateMdsCtx : public MdsCtx
{
public:
  ObTabletTruncateMdsCtx();
  explicit ObTabletTruncateMdsCtx(const MdsWriter &writer);
  virtual ~ObTabletTruncateMdsCtx();

public:
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) override;
  virtual void on_abort(const share::SCN &abort_scn) override;
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int deserialize(const char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int64_t get_serialize_size() const override;

public:
  int assign(const ObTabletTruncateMdsCtx &other);
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  void set_tablet_id(const common::ObTabletID &tablet_id) { tablet_id_ = tablet_id; }
  void set_nop(const bool nop) { nop_ = nop; }
  TO_STRING_KV(K_(magic), K_(version), K_(ls_id), K_(tablet_id), K_(nop));

private:
  OB_INLINE bool is_valid_() const;
  /// @brief: release pending truncate tablet and enable t3m tablet CAS
  void inner_end_(const bool is_commit);

private:
  static constexpr int32_t MAGIC = 0xbead;
  static constexpr int32_t VERSION = 1;

  const int32_t magic_;
  int32_t version_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  bool nop_; // in-memory member
};

} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_CTX_H
