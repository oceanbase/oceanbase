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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_ROUTINE_H
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_ROUTINE_H

#include "share/ob_virtual_table_iterator.h" // ObVirtualTableIterator
#include "lib/container/ob_se_array.h" // ObSEArray
#include "share/schema/ob_schema_getter_guard.h" // ObSchemaGetterGuard

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObRoutineInfo;
class ObMultiVersionSchemaService;
}
}

namespace observer
{
class ObAllVirtualProxyRoutine : public common::ObVirtualTableIterator
{
public:
  class ObProxyRoutineKey
  {
  public:
    ObProxyRoutineKey();
    virtual ~ObProxyRoutineKey() {}
    int init(
        const common::ObString &tenant_name,
        const common::ObString &database_name,
        const common::ObString &package_name,
        const common::ObString &routine_name);
    void reset();
    const common::ObString &get_tenant_name() const { return tenant_name_; }
    const common::ObString &get_database_name() const { return database_name_; }
    const common::ObString &get_package_name() const { return package_name_; }
    const common::ObString &get_routine_name() const { return routine_name_; }

    TO_STRING_KV(K_(tenant_name), K_(database_name), K_(package_name), K_(routine_name));
  private:
    common::ObString tenant_name_;
    common::ObString database_name_;
    common::ObString package_name_;
    common::ObString routine_name_;
  };

  enum ALL_VIRTUAL_PROXY_ROUTINE_TABLE_COLUMNS
  {
    TENANT_NAME = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    DATABASE_NAME,
    PACKAGE_NAME_FOR_ROUTINE, // can't use PACKAGE_NAME, use another name
    ROUTINE_NAME,
    ROUTINE_ID,
    ROUTINE_TYPE,
    SCHEMA_VERSION,
    ROUTINE_SQL,
    SPARE1, // int, unused
    SPARE2, // int, unused
    SPARE3, // int, unused
    SPARE4, // varchar, unused
    SPARE5, // varchar, unused
    SPARE6, // varchar, unused
  };
public:
  ObAllVirtualProxyRoutine();
  virtual ~ObAllVirtualProxyRoutine() {};
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  int init(
      share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObIAllocator *allocator);
private:
  int inner_get_next_row_();
  int fill_row_(
      const ObProxyRoutineKey &routine_key,
      const share::schema::ObRoutineInfo &routine_info);

  bool is_inited_;
  int64_t inner_idx_; // index of key_ranges_
  common::ObSEArray<ObProxyRoutineKey, 1> valid_routine_keys_; // store valid input keys
  common::ObSEArray<const share::schema::ObRoutineInfo*, 1> routine_infos_;
  share::schema::ObSchemaGetterGuard tenant_schema_guard_;
  share::schema::ObMultiVersionSchemaService *schema_service_;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualProxyRoutine);
};

} // end of namespace observer
} // end of namespace oceanbase

#endif // OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_ROUTINE_H