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

#ifndef OCEANBASE_ROOTSERVER_MOCK_FREEZE_INFO_MANAGER_H_
#define OCEANBASE_ROOTSERVER_MOCK_FREEZE_INFO_MANAGER_H_

#include <gmock/gmock.h>
#include "rootserver/ob_freeze_info_manager.h"

namespace oceanbase
{
namespace storage
{
class ObFrozenStatus;
}
namespace rootserver
{

class MockFreezeInfoManager: public ObFreezeInfoManager
{
public:
  virtual int get_freeze_info(int64_t input_frozen_version, storage::ObFrozenStatus &frozen_status)
  {
    UNUSED(input_frozen_version);
    frozen_status.frozen_version_ = ORIGIN_FROZEN_VERSION;
    frozen_status.frozen_timestamp_ = ORIGIN_FROZEN_TIMESTAMP;
    frozen_status.status_ = ORIGIN_FREEZE_STATUS;
    frozen_status.schema_version_ = ORIGIN_SCHEMA_VERSION;
    return common::OB_SUCCESS;
  }
  MOCK_METHOD2(set_freeze_info, int(storage::ObFrozenStatus &src_frozen_status,
                                    storage::ObFrozenStatus &tgt_frozen_status));
private:
  static const int64_t ORIGIN_FROZEN_VERSION = 1;
  static const common::ObFreezeStatus ORIGIN_FREEZE_STATUS = common::COMMIT_SUCCEED;
  static const int64_t ORIGIN_FROZEN_TIMESTAMP = 1;
  static const int64_t ORIGIN_SCHEMA_VERSION = 1;
};

} // end namespace rootserver
} // end namespace oceanbase
#endif
