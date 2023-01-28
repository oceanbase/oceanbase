#ifndef OCEANBASE_MITTEST_MOCK_OB_META_REPORTER_
#define OCEANBASE_MITTEST_MOCK_OB_META_REPORTER_


#include "observer/report/ob_i_meta_report.h"


namespace oceanbase
{
using namespace observer;
namespace unittest
{
class MockMetaReporter : public ObIMetaReport
{
public:
  MockMetaReporter() { }
  ~MockMetaReporter() { }
  int submit_ls_update_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id)
  {
    UNUSEDx(tenant_id, ls_id);
    return OB_SUCCESS;
  }
  int submit_tablet_update_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id)
  {
    UNUSEDx(tenant_id, ls_id, tablet_id);
    return OB_SUCCESS;
  }
  int submit_tablet_checksums_task(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id)
  {
    UNUSEDx(tenant_id, ls_id, tablet_id);
    return OB_SUCCESS;
  }
};
}// storage
}// oceanbase
#endif