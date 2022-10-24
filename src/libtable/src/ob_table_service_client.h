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

#ifndef _OB_TABLE_SERVICE_CLIENT_H
#define _OB_TABLE_SERVICE_CLIENT_H 1
#include "ob_table_define.h"
#include "lib/string/ob_strings.h"
#include "share/table/ob_table.h"
#include "share/location_cache/ob_location_struct.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace obrpc
{
class ObTableRpcProxy;
}
namespace sql
{
typedef common::ObSEArray<int64_t, 1> RowkeyArray;
} // end namespace sql
namespace table
{
class ObTable;
class ObHKVTable;
class ObTableServiceClientImpl;

/// options of table service client
class ObTableServiceClientOptions final
{
public:
  ObTableServiceClientOptions();
  ~ObTableServiceClientOptions() = default;

  ObTableRequestOptions &default_request_options() { return default_request_options_; }
  void set_net_io_thread_num(int64_t num) { net_io_thread_num_ = num; }
  int64_t net_io_thread_num() const { return net_io_thread_num_; }
private:
  ObTableRequestOptions default_request_options_;
  int64_t net_io_thread_num_;
  // @todo
  // io_thread_num
};

/// A client for table service
class ObTableServiceClient final
{
public:
  static ObTableServiceClient *alloc_client();
  static void free_client(ObTableServiceClient *client);

  // must be called before init()
  void set_options(const ObTableServiceClientOptions &options);
  // init the client and login
  int init(const ObString &host,
           int32_t mysql_port,
           int32_t rpc_port,
           const ObString &tenant,
           const ObString &user,
           const ObString &password,
           const ObString &database,
           const ObString &sys_user_password);
  void destroy();

  int alloc_table(const ObString &table_name, ObTable *&table);
  void free_table(ObTable *table);

  int create_hkv_table(const ObString &table_name, int64_t partition_num, bool if_not_exists);
  int drop_hkv_table(const ObString &table_name, bool if_exists);

  int alloc_hkv_table(const ObString &table_name, ObHKVTable *&table);
  void free_hkv_table(ObHKVTable *table);

  // location service
  int get_tablet_location(const ObString &table_name, ObRowkey &rowkey,
                          share::ObTabletLocation &tablet_location,
                          uint64_t &table_id, ObTabletID &tablet_id);
  int get_tablets_locations(const ObString &table_name, const common::ObIArray<ObRowkey> &rowkeys,
                            common::ObIArray<share::ObTabletLocation> &tablets_locations,
                            uint64_t &table_id,
                            common::ObIArray<ObTabletID> &tablet_ids,
                            common::ObIArray<sql::RowkeyArray> &rowkeys_per_tablet);
  // location service for ObTableQuery
  int get_tablet_location(const ObString &table_name, const ObString &index_name,
                          const common::ObNewRange &index_prefix,
                          share::ObTabletLocation &tablet_location,
                          uint64_t &table_id, ObTabletID &tablet_id);
  // schema service
  int get_rowkey_columns(const ObString &table_name, common::ObStrings &rowkey_columns);
  int get_table_id(const ObString &table_name, uint64_t &table_id);

  common::ObMySQLProxy &get_user_sql_client();
  obrpc::ObTableRpcProxy &get_table_rpc_proxy();
  uint64_t get_tenant_id() const;
  uint64_t get_database_id() const;
  const ObString &get_credential() const;
public:
  // for debug purpose only
  int alloc_table_v1(const ObString &table_name, ObTable *&table);
  int alloc_table_v2(const ObString &table_name, ObTable *&table);
  int alloc_hkv_table_v1(const ObString &table_name, ObHKVTable *&table);
  int alloc_hkv_table_v2(const ObString &table_name, ObHKVTable *&table);
private:
  ObTableServiceClient(ObTableServiceClientImpl &impl);
  ~ObTableServiceClient();
private:
  ObTableServiceClientImpl &impl_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_SERVICE_CLIENT_H */
