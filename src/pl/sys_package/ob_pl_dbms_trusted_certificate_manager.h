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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_TRUSTED_CERTIFICATE_MANAGER_PL_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_TRUSTED_CERTIFICATE_MANAGER_PL_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

#define INSERT_ALL_TRUSTED_ROOT_CERTIFICAT_SQL " \
  insert into %s                       \
  (common_name, description, content)  \
  values ('%.*s', '%.*s', '%.*s')      \
  "

#define DELETE_ALL_TRUSTED_ROOT_CERTIFICAT_SQL " \
  delete from %s             \
  where common_name='%.*s'   \
  "
#define UPDATE_ALL_TRUSTED_ROOT_CERTIFICAT_SQL "     \
  update %s set description='%.*s', content='%.*s'   \
  where common_name='%.*s'                           \
  "

class ObPlDBMSTrustedCertificateManager
{
public:
  ObPlDBMSTrustedCertificateManager() {}
  virtual ~ObPlDBMSTrustedCertificateManager() {}
public:
  static int add_trusted_certificate(
          sql::ObExecContext &ctx,
          sql::ParamStore &params,
          common::ObObj &result);
  static int delete_trusted_certificate(
          sql::ObExecContext &ctx,
          sql::ParamStore &params,
          common::ObObj &result);
  static int update_trusted_certificate(
          sql::ObExecContext &ctx,
          sql::ParamStore &params,
          common::ObObj &result);
private:
  static int check_data_version_and_privilege(ObExecContext &ctx);
  DISALLOW_COPY_AND_ASSIGN(ObPlDBMSTrustedCertificateManager);
};

}
}

#endif