// Copyright (c) 2022 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLET_COMPACTION_INFO_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLET_COMPACTION_INFO_H_

#include "observer/virtual_table/ob_virtual_table_tablet_iter.h"
namespace oceanbase
{
namespace observer
{
class ObAllVirtualTabletCompactionInfo : public ObVirtualTableTabletIter
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    LS_ID,
    TABLET_ID,
    FINISH_SCN,
    WAIT_CHECK_SCN,
    MAX_RECEIVED_SCN,
    SERIALIZE_SCN_LIST,
    VALIDATED_SCN,
  };
public:
  ObAllVirtualTabletCompactionInfo();
  virtual ~ObAllVirtualTabletCompactionInfo();
public:
  virtual void reset();
private:
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
private:
  char medium_info_buf_[common::OB_MAX_VARCHAR_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletCompactionInfo);
};

}
}
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLET_COMPACTION_INFO_H_ */
