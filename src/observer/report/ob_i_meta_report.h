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