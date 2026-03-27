/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_I_META_REPORT
#define OCEANBASE_OBSERVER_OB_I_META_REPORT

namespace oceanbase
{
namespace common
{
class ObTabletID;
}
namespace share
{
class ObLSID;
}
namespace observer
{
class ObIMetaReport
{
public:
  ObIMetaReport() {}
  virtual ~ObIMetaReport() {}
  virtual int submit_ls_update_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id) = 0;
};

} // end namespace observer
} // end namespace oceanbase
#endif