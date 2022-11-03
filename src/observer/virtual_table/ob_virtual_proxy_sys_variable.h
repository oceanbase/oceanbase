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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_PROXY_SYS_VARIABLE_H_
#define OCEANBASE_OBSERVER_VIRTUAL_PROXY_SYS_VARIABLE_H_

#include "share/ob_virtual_table_projector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
class ObMultiVersionSchemaService;
class ObSysVariableSchema;
}
}
namespace common
{
class ObMySQLProxy;
class ObServerConfig;
}
namespace observer
{
class ObVirtualProxySysVariable : public common::ObVirtualTableProjector
{
public:
  ObVirtualProxySysVariable();
  virtual ~ObVirtualProxySysVariable();

  int init(share::schema::ObMultiVersionSchemaService &schema_service, common::ObServerConfig *config);

  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  struct ObVarStateInfo
  {
    ObVarStateInfo();
    ~ObVarStateInfo() {}
    bool is_valid() const;
    void reset();
    TO_STRING_KV(K_(data_type), K_(flags), K_(tenant_id), K_(name_buf), K_(name_len),
                 K_(value_buf), K_(value_len), K_(gmt_modified));

    int64_t data_type_;
    int64_t flags_;
    int64_t tenant_id_;
    char name_buf_[common::OB_MAX_CONFIG_NAME_LEN+1];
    int64_t name_len_;
    char value_buf_[common::OB_MAX_CONFIG_VALUE_LEN+1];
    int64_t value_len_;
    int64_t gmt_modified_;
  };

  int get_full_row(const share::schema::ObTableSchema *table,
                   const ObVarStateInfo &var_state,
                   common::ObIArray<Column> &columns);
  int get_next_sys_variable();
  int get_all_sys_variable();

  bool is_inited_;
  bool is_queried_;
  const share::schema::ObTableSchema *table_schema_;
  const share::schema::ObTenantSchema *tenant_info_;
  ObVarStateInfo var_state_;
  int64_t idx_;
  common::ObArray<ObVarStateInfo> var_states_;
  common::ObServerConfig *config_;
  const share::schema::ObSysVariableSchema *sys_variable_schema_;

  DISALLOW_COPY_AND_ASSIGN(ObVirtualProxySysVariable);
};

}//end namespace observer
}//end namespace oceanbase
#endif  /*OCEANBASE_OBSERVER_VIRTUAL_PROXY_SYS_VARIABLE_H_*/
