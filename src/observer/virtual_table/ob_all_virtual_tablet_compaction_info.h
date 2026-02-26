//Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

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
