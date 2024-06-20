/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDBLINKERROR_H
#define OBDBLINKERROR_H
#include "lib/utility/ob_edit_distance.h"
#include "lib/ob_errno.h"
#include "lib/mysqlclient/ob_isql_connection.h"

extern bool get_dblink_reuse_connection_cfg();
extern bool get_enable_dblink_cfg();
extern uint64_t get_current_tenant_id_for_dblink();

namespace oceanbase
{
namespace common
{
namespace sqlclient
{

#define TRANSLATE_CLIENT_ERR(ret, errmsg)  \
  const int orginal_ret = ret;\
  bool is_oracle_err = lib::is_oracle_mode();\
  int translate_ret = OB_SUCCESS;\
  if (OB_SUCCESS == ret) {\
  } else if (OB_SUCCESS != (translate_ret = oceanbase::common::sqlclient::ObDblinkErrorTrans::\
      external_errno_to_ob_errno(is_oracle_err, orginal_ret, errmsg, ret))) {\
    LOG_WARN("failed to translate client error code", K(translate_ret), K(orginal_ret), K(ret), K(is_oracle_err), K(errmsg));\
  } else {\
    LOG_WARN("succ to translate client error code", K(translate_ret), K(orginal_ret), K(ret), K(is_oracle_err), K(errmsg));\
  }

#define TRANSLATE_CLIENT_ERR_2(ret, is_oracle_err, errmsg)  \
  const int orginal_ret = ret;\
  int translate_ret = OB_SUCCESS;\
  if (OB_SUCCESS == ret) {\
  } else if (OB_SUCCESS != (translate_ret = oceanbase::common::sqlclient::ObDblinkErrorTrans::\
      external_errno_to_ob_errno(is_oracle_err, orginal_ret, errmsg, ret))) {\
    LOG_WARN("failed to translate client error code", K(translate_ret), K(orginal_ret), K(ret), K(is_oracle_err), K(errmsg));\
  } else {\
    LOG_WARN("succ to translate client error code", K(translate_ret), K(orginal_ret), K(ret), K(is_oracle_err), K(errmsg));\
  }

class ObDblinkErrorTrans {
public:
  static int external_errno_to_ob_errno(bool is_oci_client,
                                        int external_errno,
                                        const char *external_errmsg,
                                        int &ob_errno);
};


#ifdef OB_BUILD_DBLINK
class ObTenantDblinkKeeper
{
public:
  class CleanDblinkArrayFunc
  {
  public:
    CleanDblinkArrayFunc() {}
    virtual ~CleanDblinkArrayFunc() = default;
    int operator()(common::hash::HashMapPair<uint32_t, int64_t> &kv);
  };
public:
  static int mtl_new(ObTenantDblinkKeeper *&dblink_keeper);
  static int mtl_init(ObTenantDblinkKeeper *&dblink_keeper);
  static void mtl_destroy(ObTenantDblinkKeeper *&dblink_keeper);
public:
  ObTenantDblinkKeeper()
  {
    tenant_id_ = common::OB_INVALID_ID;
  }
  ~ObTenantDblinkKeeper()
  {
    destroy();
  }
  int init(uint64_t tenant_id);
  int set_dblink_conn(uint32_t sessid, common::sqlclient::ObISQLConnection *dblink_conn);
  int get_dblink_conn(uint32_t sessid, uint64_t dblink_id,
                      common::sqlclient::ObISQLConnection *&dblink_conn);
  int clean_dblink_conn(uint32_t sessid, bool force_disconnect);
private:
  int destroy();
private:
  uint64_t tenant_id_;
  obsys::ObRWLock lock_;
  hash::ObHashMap<uint32_t, int64_t> dblink_conn_map_;
};
#endif
} // namespace sqlclient
} // namespace common
} // namespace oceanbase
#endif //OBDBLINKERROR_H