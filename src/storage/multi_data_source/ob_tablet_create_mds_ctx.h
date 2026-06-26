/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_CREATE_TABLET_MDS_CTX
#define OCEANBASE_STORAGE_OB_CREATE_TABLET_MDS_CTX

#include "storage/multi_data_source/mds_ctx.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class ObTabletCreateMdsCtx : public MdsCtx
{
public:
  ObTabletCreateMdsCtx();
  explicit ObTabletCreateMdsCtx(const MdsWriter &writer);
  virtual ~ObTabletCreateMdsCtx() = default;
public:
  virtual void on_abort(const share::SCN &abort_scn) override;
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int deserialize(const char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int64_t get_serialize_size() const override;
public:
  void set_ls_id(const share::ObLSID &ls_id);
  int assign(const ObTabletCreateMdsCtx &rhs);
private:
  static constexpr int32_t MAGIC = 0xdead;
  static constexpr int32_t VERSION = 1;

  const int32_t magic_;
  int32_t version_;
  share::ObLSID ls_id_;
};

inline void ObTabletCreateMdsCtx::set_ls_id(const share::ObLSID &ls_id)
{
  ls_id_ = ls_id;
}
} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_CREATE_TABLET_MDS_CTX
