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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_BASE_
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_BASE_

#include "lib/container/ob_se_array.h"
#include "share/ob_define.h"

#include "share/schema/ob_schema_struct.h"
#include "share/ob_virtual_table_iterator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace observer
{
// this class is just a base class, not for a real virtual table
class ObAllVirtualProxyBaseIterator : public common::ObVirtualTableIterator
{
public:
  ObAllVirtualProxyBaseIterator();
  virtual ~ObAllVirtualProxyBaseIterator();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual int inner_get_next_row() = 0;

  void set_schema_service(share::schema::ObMultiVersionSchemaService &schema_service);

  // convert objs(rowkey, obj) to str, we will call out_obj.set_varchar() if SUCC
  // get_xxx_str will call print_sql_literal func
  // get_xxx_bin_str will call serialize func
  int get_rowkey_str(
      const bool is_oracle_mode,
      const common::ObRowkey &rowkey,
      common::ObObj &out_obj);
  int get_rowkey_bin_str(const common::ObRowkey &rowkey, common::ObObj &out_obj);
  int get_rowkey_type_str(const common::ObRowkey &rowkey, common::ObObj &out_obj);
  int get_obj_str(const common::ObObj &obj, common::ObObj &out_obj);
  int get_obj_bin_str(const common::ObObj &obj, common::ObObj &out_obj);
  int get_rows_str(
      const bool is_oracle_mode,
      const common::ObIArray<common::ObNewRow>& rows,
      common::ObObj &out_obj);
  int get_rows_bin_str(const common::ObIArray<common::ObNewRow>& rows, common::ObObj &out_obj);
  int get_partition_value_str(
      const bool is_oracle_mode,
      const share::schema::ObPartitionFuncType type,
      const share::schema::ObBasePartition &partition,
      common::ObObj &out_obj);
  int get_partition_value_bin_str(
      const share::schema::ObPartitionFuncType type,
      const share::schema::ObBasePartition &partition,
      common::ObObj &out_obj);
  int check_schema_version(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id);
protected:
  common::ObString input_tenant_name_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::schema::ObSchemaGetterGuard tenant_schema_guard_;
  //Not sure if the incoming SQL is tenant_schema_guard, try again here for safety
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualProxyBaseIterator);
};

} // end of namespace observer
} // end of namespace oceanbase
#endif /* OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_BASE_ */
