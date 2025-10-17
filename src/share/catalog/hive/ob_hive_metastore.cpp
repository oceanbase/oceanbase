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

#define USING_LOG_PREFIX SHARE

#include "lib/alloc/alloc_assist.h"
#include "lib/file/ob_string_util.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_hive_metastore.h"
#include "ob_hms_client_pool.h"
#include "share/catalog/hive/ob_kerberos.h"
#include "share/catalog/hive/thrift/transport/ob_t_sasl_client_transport.h"
#include "share/catalog/ob_catalog_properties.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/external_table/ob_hdfs_storage_info.h"

namespace oceanbase
{
namespace share
{
/* --------------------- start of Hive Client Operations ---------------------*/
void ListDBNamesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  client->get_all_databases(databases_);
}

void GetDatabaseOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get the database by name", K(ret), K(db_name_.c_str()));
  try {
    client->get_database(db_, db_name_);
    found_ = true;
  } catch (ApacheHive::NoSuchObjectException &ex) {
    found_ = false;
  }
}

void GetAllTablesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get the all tables", K(ret), K(db_name_.c_str()));
  client->get_all_tables(tb_names_, db_name_);
}

void ListPartitionsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  LOG_TRACE("get partitions", K(db_name_.c_str()), K(tb_name_.c_str()), K(max_parts_));
  client->get_partitions(partitions_, db_name_, tb_name_, max_parts_);
}

void ListPartitionNamesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get partition names", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()), K(max_parts_));
  client->get_partition_names(partition_names_, db_name_, tb_name_, max_parts_);
}

void GetPartitionValuesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  PartitionValuesRequest req;
  PartitionValuesResponse resp;
  LOG_TRACE("get partition of table", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));

  req.dbName = db_name_;
  req.tblName = tb_name_;
  // Note: partition keys must be not empty.
  req.partitionKeys = partition_keys_;

  client->get_partition_values(resp, req);

  partition_values_rows_ = resp.partitionValues;
}

void GetTableOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get table schema", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));

  // Mark the hive metastore client is newest version, such as v4.x.x.
  // Method `get_table` is not supported in v4.x.x.
  if (OB_LIKELY(!is_newest_version_)) {
    try {
      client->get_table(table_, db_name_, tb_name_);
      found_ = true;
    } catch (ApacheHive::UnknownTableException &ex) {
      found_ = false;
    } catch (ApacheHive::NoSuchObjectException &ex) {
      found_ = false;
    } catch (apache::thrift::TApplicationException &ex) {
      found_ = false;
      if (OB_LIKELY(apache::thrift::TApplicationException::UNKNOWN_METHOD == ex.getType())) {
        is_newest_version_ = true;
      }
    }
  }

  // Only not found the table and the hive metastore client is newest version, then try newest
  // method `get_table_req`.
  if (OB_LIKELY(!found_ && is_newest_version_)) {
    LOG_TRACE("try to get table by newest method `get_table_req`",
              K(ret),
              K(db_name_.c_str()),
              K(tb_name_.c_str()));
    ApacheHive::GetTableRequest req;
    ApacheHive::GetTableResult result;
    req.dbName = db_name_;
    req.tblName = tb_name_;
    try {
      client->get_table_req(result, req);
      table_ = result.table;
      found_ = true;
    } catch (ApacheHive::UnknownTableException &ex) {
      found_ = false;
    } catch (ApacheHive::NoSuchObjectException &ex) {
      found_ = false;
    }
  }
}

void GetLatestPartitionDdlTimeOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get all partitions",
            K(ret),
            K(db_name_.c_str()),
            K(tb_name_.c_str()),
            K(max_parts_),
            K_(latest_ddl_time));
  Partitions partitions;
  client->get_partitions(partitions, db_name_, tb_name_, max_parts_);
  int64_t tmp_ddl_time;
  std::map<String, String>::iterator params_iter;

  for (Partitions::iterator iter = partitions.begin(); OB_SUCC(ret) && iter != partitions.end();
       iter++) {
    params_iter = iter->parameters.find(LAST_DDL_TIME);
    if (OB_UNLIKELY(params_iter != iter->parameters.end())) {
      tmp_ddl_time = ::obsys::ObStringUtil::str_to_int(params_iter->second.c_str(), 0);
      if (OB_LIKELY(latest_ddl_time_ < tmp_ddl_time)) {
        latest_ddl_time_ = tmp_ddl_time;
      }
      LOG_TRACE("get latest ddl time", K(ret), K(latest_ddl_time_));
    }
  }
}

void LockOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  client->lock(lock_response_, lock_request_);
  // Check if lock was successfully acquired
  success_ = (lock_response_.state == ApacheHive::LockState::ACQUIRED);
  LOG_TRACE("lock operation completed", K(success_), "lock_state", lock_response_.state);
}

void UnlockOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  client->unlock(unlock_request_);
  LOG_TRACE("unlock operation completed successfully");
}

void AlterTableOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  client->alter_table(db_name_, tb_name_, new_table_);
  LOG_TRACE("alter table operation completed successfully");
}
void GetTableStatisticsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get table statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));

  ApacheHive::TableStatsRequest request;
  request.dbName = db_name_;
  request.tblName = tb_name_;
  request.colNames = column_names_;
  request.engine = "hive";

  try {
    client->get_table_statistics_req(table_stats_result_, request);
    found_ = true;
    LOG_TRACE("successfully got table statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  } catch (ApacheHive::NoSuchObjectException &ex) {
    found_ = false;
    LOG_TRACE("table statistics not found", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  } catch (ApacheHive::MetaException &ex) {
    found_ = false;
    LOG_WARN("meta exception when getting table statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  }
}

void GetPartitionsStatisticsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get partitions statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));

  ApacheHive::PartitionsStatsRequest request;
  request.dbName = db_name_;
  request.tblName = tb_name_;
  request.colNames = column_names_;
  request.partNames = partition_names_;
  request.engine = "hive";

  try {
    client->get_partitions_statistics_req(partition_stats_result_, request);
    found_ = true;
    LOG_TRACE("successfully got partitions statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  } catch (ApacheHive::NoSuchObjectException &ex) {
    found_ = false;
    LOG_TRACE("partitions statistics not found", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  } catch (ApacheHive::MetaException &ex) {
    found_ = false;
    LOG_WARN("meta exception when getting partitions statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  }
}

void GetAllPartitionsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get all partitions for basic stats", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  client->get_partitions(partitions_, db_name_, tb_name_, -1);
}

void GetPartitionsByNamesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get partitions by names", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()),
            "partition_count", partition_names_.size());
  client->get_partitions_by_names(partitions_, db_name_, tb_name_, partition_names_);
}

/* --------------------- end of Hive Client Operations ---------------------*/
/* --------------------- start of ObHiveMetastoreClient ---------------------*/
ObHiveMetastoreClient::ObHiveMetastoreClient()
    : allocator_(nullptr), socket_(), transport_(), protocol_(), hive_metastore_client_(), uri_(),
      properties_(), hms_keytab_(), hms_principal_(), hms_krb5conf_(), service_(), server_FQDN_(),
      state_lock_(ObLatchIds::OBJECT_DEVICE_LOCK), conn_lock_(ObLatchIds::OBJECT_DEVICE_LOCK),
      kerberos_lock_(ObLatchIds::OBJECT_DEVICE_LOCK), is_inited_(false), is_opened_(false),
      last_kinit_ts_(0), client_id_(0), client_pool_(nullptr), is_in_use_(false), socket_timeout_(-1)
{
}

int ObHiveMetastoreClient::init(const int64_t &socket_timeout, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(state_lock_);
  is_inited_ = false;
  socket_timeout_ = -1;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHiveMetastoreClient init twice", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid allocator", K(ret), KP(allocator));
  } else {
    allocator_ = allocator;
    is_inited_ = true;
    // Use current time as client id.
    client_id_ = ObTimeUtility::current_time();
    socket_timeout_ = socket_timeout;
  }
  return ret;
}

int ObHiveMetastoreClient::setup_hive_metastore_client(const ObString &uri,
                                                       const ObString &properties)
{
  int ret = OB_SUCCESS;

  ObHMSCatalogProperties hive_catalog_properties;
  // Using temp_allocator to avoid the memory leak when the properties is too long.
  ObArenaAllocator temp_allocator;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client already setup and opened", K(ret));
  } else if (OB_ISNULL(uri) || OB_LIKELY(0 == uri.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invald meta uri str", K(ret), K(uri));
  } else if (OB_FAIL(ob_write_string(*allocator_, properties, properties_, true))) {
    LOG_WARN("failed to write properties", K(ret), K(properties));
  } else if (OB_FAIL(ob_write_string(*allocator_, uri, uri_, true))) {
    LOG_WARN("failed to write metastore uri", K(ret), K(uri));
  } else if (OB_FAIL(hive_catalog_properties.load_from_string(properties_, temp_allocator))) {
    LOG_WARN("failed to init hive catalog properties", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_,
                                     hive_catalog_properties.principal_,
                                     hms_principal_,
                                     true))) {
    LOG_WARN("failed to write principal", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_,
                                     hive_catalog_properties.keytab_,
                                     hms_keytab_,
                                     true))) {
    LOG_WARN("failed to write keytab", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_,
                                     hive_catalog_properties.krb5conf_,
                                     hms_krb5conf_,
                                     true))) {
    LOG_WARN("failed to write krb5conf", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    int port = 0;
    const int64_t uri_len = uri.length();
    char host[uri_len];
    // Length of host and port must be less than total meta uri.
    if (OB_FAIL(extract_host_and_port(uri, host, port))) {
      LOG_WARN("failed to get namenode and path", K(ret), K(uri));
    } else if (socket_timeout_ < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid socket timeout", K(ret), K(socket_timeout_));
    } else {
      LOG_TRACE("get host and port in detail", K(ret), K(ObString(host)), K(port));
      // Create socket
      String str_host = String(host);
      socket_ = std::make_shared<TSocket>(str_host, port);
      socket_->setKeepAlive(true);
      // Default is 10 seconds.
      socket_->setConnTimeout(socket_timeout_);
      socket_->setRecvTimeout(socket_timeout_);
      socket_->setSendTimeout(socket_timeout_);
    }

    if (OB_FAIL(ret)) {
    } else {
      // Create transport
      transport_ = std::make_shared<TBufferedTransport>(socket_);
      // If the keytab and pricipal is valid, init kerberos.
      if (OB_NOT_NULL(hms_keytab_) && 0 < hms_keytab_.length() && OB_NOT_NULL(hms_principal_)
          && 0 < hms_principal_.length()) {
        LOG_TRACE("step into kerberos initialize step",
                  K(ret),
                  K(hms_keytab_),
                  K(hms_principal_),
                  K(hms_krb5conf_));
        ObString cache_name;
        ObString temp_princ;
        // Using temp_allocator to store temp variables.
        ObArenaAllocator temp_allocator;
        if (OB_FAIL(init_kerberos(hms_keytab_, hms_principal_, hms_krb5conf_, cache_name))) {
          LOG_WARN("failed to init kerberos env", K(ret));
        } else if (OB_FAIL(ob_write_string(temp_allocator, hms_principal_, temp_princ, true))) {
          LOG_WARN("failed to copy the hms principal", K(ret), K_(hms_principal));
        } else {
          // service and server_FQDN can seperate by principal.
          // Because the GSSAPI would assembly by service and server_FQDN to be the principal.
          // Such as service "hive" and server_FQDN "hadoop" -> "hive/hadoop@EXAMPLE.COM".
          // TODO(bitao): this principal may be not same as "hive/hadoop@EXAMPLE.COM" in the
          // kerberos config.
          ObString tmp_service = temp_princ.split_on('/').trim_space_only();
          ObString tmp_server_FQDN = temp_princ.split_on('@').trim_space_only();
          // Note: service_ and server_FQDN_ should be c_style.
          if (OB_FAIL(ob_write_string(*allocator_, tmp_service, service_, true))) {
            LOG_WARN("failed to write service value", K(ret), K(tmp_service));
          } else if (OB_FAIL(ob_write_string(*allocator_, tmp_server_FQDN, server_FQDN_, true))) {
            LOG_WARN("failed to write service_FQDN value", K(ret), K(tmp_server_FQDN));
          } else if (OB_UNLIKELY(service_.empty())) {
            ret = OB_INVALID_HMS_SERVICE;
            LOG_WARN("service should not be empty", K(ret), K(temp_princ), K_(service));
          } else if (OB_UNLIKELY(server_FQDN_.empty())) {
            ret = OB_INVALID_HMS_SERVICE_FQDN;
            LOG_WARN("service_FQDN should not be empty", K(ret), K(temp_princ), K(server_FQDN_));
          } else {
            transport_
                = TSaslClientTransport::wrap_client_transports(service_, server_FQDN_, transport_);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        // Create protocol
        protocol_ = std::make_shared<TBinaryProtocol>(transport_);
        // Create thrift hive metastore client
        hive_metastore_client_ = std::make_shared<ThriftHiveMetastoreClient>(protocol_);
      }
    }
  }

  // Make sure that the hive client is valid.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(open())) {
    LOG_WARN("failed to open hive metastore client", K(ret));
  }
  return ret;
}

int ObHiveMetastoreClient::open()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(conn_lock_);
  try {
    if (OB_ISNULL(transport_)) {
      ret = OB_HMS_ERROR;
      LOG_WARN("invalid transport variables", K(ret));
    } else if (OB_LIKELY(!transport_->isOpen())) {
      transport_->open();
      is_opened_ = true;
    } else {
      LOG_TRACE("transport is already opened", K(ret));
    }
  } catch (TException &tx) {
    const char *err_msg = tx.what();
    ret = OB_HMS_ERROR;
    LOG_WARN("failed to open transport", K(ret), K(err_msg));
  }
  return ret;
}

int ObHiveMetastoreClient::close()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(conn_lock_);
  try {
    if (OB_LIKELY(is_valid())) {
      hive_metastore_client_->shutdown();
    }

    if (OB_ISNULL(transport_)) {
      ret = OB_INVALID_HMS_TRANSPORT;
      LOG_WARN("invalid transport variables", K(ret));
    } else if (OB_LIKELY(transport_->isOpen())) {
      LOG_TRACE("step into close transport", K(ret));
      transport_->close();
      is_opened_ = false;
    } else {
      LOG_TRACE("transport is already closed", K(ret));
    }
  } catch (TException &tx) {
    String err_msg = tx.what();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to open transport", K(ret), K(err_msg.c_str()));
  }
  return ret;
}

int ObHiveMetastoreClient::release()
{
  int ret = OB_SUCCESS;

  // Copy the client id to avoid the client is be freed in the pool.
  const int64_t hold_client_id = client_id_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("client is not inited, cannot release", K(ret), K(hold_client_id));
  } else if (!is_in_use_) {
    // Check if not in use to prevent double release
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client is not in use, cannot release", K(ret), K(hold_client_id));
  }

  // Return to pool if pool pointer is set.
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(client_pool_)) {
    ObHiveMetastoreClient *client = this;
    ObHMSClientPool *pool = this->client_pool_;
    const uint64_t tenant_id = pool->get_tenant_id();
    const uint64_t catalog_id = pool->get_catalog_id();

    if (OB_FAIL(pool->return_client(client))) {
      LOG_WARN("failed to return client to pool",
               K(ret),
               K(hold_client_id),
               K(tenant_id),
               K(catalog_id));
    } else if (OB_NOT_NULL(client)) {
      // Return the client pointer to the pool and it would be released in the pool.
      // Mark as not in use only after successful return
      is_in_use_ = false;
      LOG_TRACE("client released back to pool", K(ret), K(hold_client_id));
    } else {
      LOG_WARN("client is null which would be released in the pool",
               K(ret),
               K(tenant_id),
               K(catalog_id),
               K(hold_client_id),
               K(lbt()));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client has no pool pointer, cannot release", K(ret), K(hold_client_id));
  }

  return ret;
}

ObHiveMetastoreClient::~ObHiveMetastoreClient()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(state_lock_);
  if (OB_NOT_NULL(allocator_)) {
    // Check to avoid double free.
    if (!uri_.empty()) {
      allocator_->free(uri_.ptr());
    }
    if (!hms_keytab_.empty()) {
      allocator_->free(hms_keytab_.ptr());
    }
    if (!hms_principal_.empty()) {
      allocator_->free(hms_principal_.ptr());
    }
    if (!hms_krb5conf_.empty()) {
      allocator_->free(hms_krb5conf_.ptr());
    }
    if (!properties_.empty()) {
      allocator_->free(properties_.ptr());
    }
    if (!service_.empty()) {
      allocator_->free(service_.ptr());
    }
    if (!server_FQDN_.empty()) {
      allocator_->free(server_FQDN_.ptr());
    }
  }

  is_inited_ = false;
  client_id_ = 0;
  socket_timeout_ = -1;
  if (OB_LIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hive metastore client is invalid in destructor", K(ret));
  } else if (OB_FAIL(close())) {
    LOG_WARN("failed to close hive metastore client in destructor", K(ret));
  }

  hive_metastore_client_.reset();
  socket_.reset();
  transport_.reset();
  protocol_.reset();
  allocator_ = nullptr;
  // Ensure pool pointer and use state are cleared
  client_pool_ = nullptr;
  is_in_use_ = false;
}

int ObHiveMetastoreClient::extract_host_and_port(const ObString &uri, char *host, int &port)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(uri) || OB_LIKELY(0 == uri.length())) {
    ret = OB_INVALID_HMS_METASTORE;
    LOG_WARN("failed to handle meta uri is null", K(ret), K(uri));
  } else if (OB_ISNULL(host)) {
    ret = OB_INVALID_HMS_HOST;
    LOG_WARN("failed to handle host is null", K(ret), K(uri));
  } else if (OB_UNLIKELY(!uri.prefix_match("thrift://"))) {
    ret = OB_INVALID_HMS_METASTORE;
    LOG_WARN("invalid metastore uri without prefix - thrift://", K(ret), K(uri));
  } else {
    // Full path is qualified, i.e. "thrift://host:port".
    // Extract "host" and "port".
    // Skip the "thrift://"
    const char *p = uri.ptr() + 9;
    const int l = uri.length() - 9;
    ObString tmp_uri(l, p);

    LOG_TRACE("get metastore uri in detail", K(ret), K(uri), K(tmp_uri));
    const char *ptr = tmp_uri.ptr();
    const char *needed_colon = strchr(ptr, ':');
    if (OB_ISNULL(needed_colon)) {
      ret = OB_INVALID_HMS_METASTORE;
      LOG_WARN("failed to handle host and port", K(ret), K(uri), K(tmp_uri));
    }

    const int64_t tmp_uri_len = tmp_uri.length();
    if (OB_FAIL(ret)) {
    } else {
      // Handle host.
      const int64_t ip_len = needed_colon - ptr;
      if (OB_UNLIKELY(ip_len > tmp_uri_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected host len execeed malloc", K(ret), K(ip_len), K(tmp_uri_len));
      } else {
        strncpy(host, ptr, ip_len);
        host[ip_len] = '\0';
      }

      if (OB_FAIL(ret)) {
      } else {
        // Handle port.
        const char *port_str = needed_colon + 1;
        const int64_t port_len = tmp_uri_len - ip_len - 1;

        char tmp_port[port_len + 1];
        if (OB_UNLIKELY(port_len > tmp_uri_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected port len exceed malloc",
                   K(ret),
                   K(port_str),
                   K(port_len),
                   K(tmp_uri_len));
        } else {
          strncpy(tmp_port, port_str, port_len);
          tmp_port[port_len] = '\0';
        }
        // Port should be a int number.
        if (OB_FAIL(ret)) {
        } else {
          port = ::obsys::ObStringUtil::str_to_int(tmp_port, 0);
          if (OB_UNLIKELY(!::obsys::ObStringUtil::is_int(tmp_port))) {
            ret = OB_INVALID_HMS_PORT;
            LOG_WARN("port is not int type", K(ret), K(port), K(port_len), K(port_str));
          } else if (OB_LIKELY(port == 0)) {
            ret = OB_INVALID_HMS_PORT;
            LOG_WARN("failed to get port", K(ret));
          } else {
            LOG_TRACE("get port success", K(ret), K(port));
          }
        }
      }
    }
  }

  return ret;
}

int64_t ObHiveMetastoreClient::handle_ddl_time(String &time_str)
{
  int64_t ddl_time = 0;
  if (!::obsys::ObStringUtil::is_int(time_str.c_str())) {
    // DO NOTHING
  } else {
    ddl_time = ::obsys::ObStringUtil::str_to_int(time_str.c_str(), 0);
  }
  return ddl_time;
}

int ObHiveMetastoreClient::build_lock_request_for_operation(const LockableOperation &op,
                                                            ApacheHive::LockRequest &lock_request)
{
  int ret = OB_SUCCESS;

  try {
    ApacheHive::LockComponent lock_component;

    // Get specific lock attributes from operation
    lock_component.type = op.get_lock_type();
    lock_component.level = op.get_lock_level();
    lock_component.dbname = op.get_db_name();
    lock_component.tablename = op.get_table_name();
    lock_component.operationType = op.get_operation_type();
    lock_component.isTransactional = op.is_transactional();
    lock_component.isDynamicPartitionWrite = op.is_dynamic_partition_write();

    // Set lock request - get metadata from operation
    lock_request.component.clear();
    lock_request.component.push_back(lock_component);
    lock_request.user = op.get_user();
    lock_request.hostname = op.get_hostname();
    lock_request.agentInfo = op.get_agent_info();
    lock_request.zeroWaitReadEnabled = false;
    lock_request.exclusiveCTAS = false;
    lock_request.locklessReadsEnabled = false;

  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to build lock request for operation", K(ret));
  }

  return ret;
}
void ObHiveMetastoreClient::extract_basic_stats_from_parameters(const std::map<std::string, std::string> &parameters,
                                                               ObHiveBasicStats &basic_stats)
{
  basic_stats.reset();

  std::map<std::string, std::string>::const_iterator it = parameters.find("numFiles");
  if (it != parameters.end() && ::obsys::ObStringUtil::is_int(it->second.c_str())) {
    basic_stats.num_files_ = ::obsys::ObStringUtil::str_to_int(it->second.c_str(), 0);
  }

  it = parameters.find("numRows");
  if (it != parameters.end() && ::obsys::ObStringUtil::is_int(it->second.c_str())) {
    basic_stats.num_rows_ = ::obsys::ObStringUtil::str_to_int(it->second.c_str(), 0);
  }

  it = parameters.find("totalSize");
  if (it != parameters.end() && ::obsys::ObStringUtil::is_int(it->second.c_str())) {
    basic_stats.total_size_ = ::obsys::ObStringUtil::str_to_int(it->second.c_str(), 0);
  }

  LOG_TRACE("extracted basic stats from parameters", K(basic_stats));
}

template <typename Operation>
int ObHiveMetastoreClient::try_call_hive_client(Operation &&op)
{
  int ret = OB_SUCCESS;
  int try_times = 0;
  String err_msg;
  // check if the client is valid
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_HMS_ERROR;
    LOG_WARN("invalid hive metastore client", K(ret));
  }

  for (; OB_SUCC(ret) && try_times < MAX_HIVE_METASTORE_CLIENT_RETRY; ++try_times) {
    try {
      if (OB_FAIL(open())) {
        LOG_WARN("failed to connect to hive metastore", K(ret));
        // Reset ret to OB_SUCCESS to continue the loop.
        ret = OB_SUCCESS;
      } else {
        op.execute(hive_metastore_client_.get());
        break; // success, break the loop
      }
    } catch (apache::thrift::transport::TTransportException &e) {
      err_msg = e.what();
      LOG_WARN("error on executing to hive metastore", K(ret), K(try_times), K(err_msg.c_str()));
      // Close the transport when error occurs.
      if (OB_FAIL(close())) {
        LOG_WARN("failed to close transport in try step", K(ret));
        // Reset the ret to OB_SUCCESS to continue the loop.
        ret = OB_SUCCESS;
      }
    } catch (apache::thrift::TException &e) {
      err_msg = e.what();
      LOG_WARN("meta exception on executing to hive metastore",
               K(ret),
               K(try_times),
               K(err_msg.c_str()));
      // Close the transport when error occurs.
      if (OB_FAIL(close())) {
        LOG_WARN("failed to close transport in try step", K(ret));
        // Reset the ret to OB_SUCCESS to continue the loop.
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_FAIL(ret) || try_times >= MAX_HIVE_METASTORE_CLIENT_RETRY) {
    ret = OB_HMS_ERROR;
    LOG_WARN("hive metastore expired", K(ret), K(err_msg.c_str()));
  }
  return ret;
}

template <typename Operation>
int ObHiveMetastoreClient::try_call_hive_client_with_lock(Operation &&op)
{
  int ret = OB_SUCCESS;
  int operation_ret = OB_SUCCESS;
  ApacheHive::LockResponse lock_response;
  int64_t lock_id = 0;
  bool lock_acquired = false;

  // Step 1: Get lock information from operation and build lock request
  ApacheHive::LockRequest lock_request;
  if (OB_FAIL(build_lock_request_for_operation(op, lock_request))) {
    LOG_WARN("failed to build lock request for operation", K(ret));
  }

  // Step 2: Try to acquire lock
  if (OB_SUCC(ret)) {
    if (OB_FAIL(lock(lock_request, lock_response))) {
      LOG_WARN("failed to acquire lock", K(ret));
    } else {
      lock_acquired = true;
      lock_id = lock_response.lockid;
      LOG_TRACE("successfully acquired lock", K(lock_id));
    }
  }

  // Step 3: Execute operation if lock is acquired successfully
  if (OB_SUCC(ret) && lock_acquired) {
    operation_ret = try_call_hive_client(op);
    if (OB_SUCC(operation_ret)) {
      LOG_TRACE("operation completed successfully with lock", K(lock_id));
    } else {
      LOG_WARN("operation failed with lock", K(operation_ret), K(lock_id));
    }
  }

  // Step 4: Always release lock if acquired
  if (lock_acquired) {
    ApacheHive::UnlockRequest unlock_request;
    unlock_request.lockid = lock_id;

    int unlock_ret = unlock(unlock_request);
    if (OB_FAIL(unlock_ret)) {
      LOG_ERROR("failed to release lock", K(unlock_ret), K(lock_id));
      // If operation succeeded but unlock failed, return unlock error as priority
      if (OB_SUCC(ret) && OB_SUCC(operation_ret)) {
        ret = unlock_ret;
      }
    } else {
      LOG_TRACE("successfully released lock", K(ret), K(unlock_ret), K(lock_id));
    }
  }

  // Step 5: Return unified result
  if (OB_SUCC(ret)) {
    // If lock operations all succeed, return business operation result.
    ret = operation_ret;
  }

  return ret;
}

int ObHiveMetastoreClient::init_kerberos(ObString &keytab,
                                         ObString &principal,
                                         ObString &krb5conf,
                                         ObString &cache_name,
                                         int64_t kinit_timeout)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY((!keytab.empty() && !principal.empty() || !cache_name.empty()))) {
    const int64_t current_time = ObTimeUtility::current_time();
    if (OB_LIKELY(current_time - kinit_timeout > last_kinit_ts_)) {
      // Prevent cache file corruptions
      SpinWLockGuard guard(kerberos_lock_);
      // Double-check after acquiring lock to prevent race condition
      if (OB_LIKELY(current_time - kinit_timeout > last_kinit_ts_)) {
        ObKerberos kerb;
        if (OB_FAIL(kerb.init(keytab, principal, krb5conf, cache_name))) {
          LOG_WARN("failed to init kerberos", K(ret));
        } else {
          last_kinit_ts_ = ObTimeUtility::current_time();
          LOG_TRACE("latest kerberos kinit timestamp", K(ret), K(last_kinit_ts_));
        }
      }
    }
  }
  return ret;
}

int ObHiveMetastoreClient::list_db_names(ObIAllocator &allocator, ObIArray<ObString> &ns_names)
{
  int ret = OB_SUCCESS;
  Strings databases;
  ListDBNamesOperation op(databases);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to call get all databases operation", K(ret));
  } else {
    for (Strings::iterator iter = databases.begin(); OB_SUCC(ret) && iter != databases.end();
         iter++) {
      ObString database_name(iter->c_str());
      ObString db_name;
      if (OB_FAIL(ob_write_string(allocator, database_name, db_name))) {
        LOG_WARN("failed to write db name", K(ret), K(database_name), K(db_name));
      } else if (OB_FAIL(ns_names.push_back(db_name))) {
        LOG_WARN("failed to push database name to list", K(ret), K(database_name), K(db_name));
      }
    }
  }
  return ret;
}

int ObHiveMetastoreClient::get_database(const ObString &ns_name,
                                        const ObNameCaseMode case_mode,
                                        share::schema::ObDatabaseSchema &database_schema)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<ApacheHive::Database> database = std::make_shared<ApacheHive::Database>();
  String db_name = String(ns_name.ptr(), ns_name.length());
  bool found = false;
  GetDatabaseOperation op(*database, db_name, found);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to call get database operation", K(ret));
  } else if (OB_UNLIKELY(!found)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("failed to get database", K(ret), K(ns_name));
  } else {
    if (OB_LIKELY(database->name.empty())) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("failed to get database", K(ret), K(ns_name));
    } else {
      database_schema.set_database_name(database->name.c_str());
    }
  }

  return ret;
}

int ObHiveMetastoreClient::list_table_names(const ObString &db_name,
                                            ObIAllocator &alloator,
                                            ObIArray<ObString> &tb_names)
{
  int ret = OB_SUCCESS;

  Strings table_names;
  String d_name = String(db_name.ptr(), db_name.length());
  GetAllTablesOperation op(table_names, d_name);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to call get database operation", K(ret));
  } else {
    for (Strings::iterator iter = table_names.begin(); OB_SUCC(ret) && iter != table_names.end();
         iter++) {
      ObString tb_name = ObString(iter->c_str());
      ObString t_name;
      if (OB_FAIL(ob_write_string(alloator, tb_name, t_name))) {
        LOG_WARN("failed to write table name", K(ret), K(tb_name), K(t_name));
      } else if (OB_FAIL(tb_names.push_back(t_name))) {
        LOG_WARN("failed to push table name to list", K(ret), K(tb_name), K(t_name));
      }
    }
  }
  return ret;
}

int ObHiveMetastoreClient::get_latest_schema_version(const ObString &ns_name,
                                                     const ObString &tb_name,
                                                     const ObNameCaseMode case_mode,
                                                     int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  bool found = false;
  std::shared_ptr<ApacheHive::Table> table = std::make_shared<ApacheHive::Table>();
  GetTableOperation table_op(*table, d_name, t_name, found);
  if (OB_FAIL(try_call_hive_client(table_op))) {
    LOG_WARN("failed to call to get table operation", K(ret), K(ns_name), K(tb_name));
  } else if (OB_UNLIKELY(!found)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("failed to find table", K(ret), K(ns_name), K(tb_name));
  } else {
    schema_version = table->createTime;
    if (OB_LIKELY(table->partitionKeys.empty())) {
      // Indicate the table is not with partitions
      std::map<String, String>::iterator iter;
      iter = table->parameters.find(LAST_DDL_TIME);
      if (OB_LIKELY(iter != table->parameters.end())) {
        String version_str = iter->second;
        int64_t ddl_time = handle_ddl_time(version_str);
        if (schema_version < ddl_time) {
          schema_version = ddl_time;
        }
      }
    } else {
      // Fetch the latest partition ddl time.
      int64_t latest_ddl_time = -1;
      GetLatestPartitionDdlTimeOperation ddl_time_op(latest_ddl_time, d_name, t_name);
      if (OB_FAIL(try_call_hive_client(ddl_time_op))) {
        LOG_WARN("failed to call to get partition latest ddl time", K(ret), K(ns_name), K(tb_name));
      } else if (OB_UNLIKELY(-1 == latest_ddl_time)) {
        // DO NOTHING, use table create time as the schema version
      } else if (schema_version < latest_ddl_time) {
        schema_version = latest_ddl_time;
        LOG_TRACE("get the latest partition ddl time as latest version",
                  K(ret),
                  K(latest_ddl_time));
      }
    }
  }
  return ret;
}

int ObHiveMetastoreClient::get_table(const ObString &ns_name,
                                     const ObString &tb_name,
                                     const ObNameCaseMode case_mode,
                                     ApacheHive::Table &original_table)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  bool found = false;
  GetTableOperation table_op(original_table, d_name, t_name, found);
  if (OB_FAIL(try_call_hive_client(table_op))) {
    LOG_WARN("failed to call to get table operation", K(ret), K(ns_name), K(tb_name));
  } else if (OB_UNLIKELY(!found)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("failed to find out table", K(ret), K(ns_name), K(tb_name));
  } else {
    LOG_TRACE("found original table", K(ret), K(ns_name), K(tb_name));
  }
  return ret;
}

int ObHiveMetastoreClient::list_partitions(const ObString &ns_name,
                                           const ObString &tb_name,
                                           Partitions &partitions)
{
  int ret = OB_SUCCESS;

  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  ListPartitionsOperation partitions_op(partitions, d_name, t_name);
  if (OB_FAIL(try_call_hive_client(partitions_op))) {
    LOG_WARN("failed to call get table partition name operation", K(ret), K(ns_name), K(tb_name));
  }
  return ret;
}


int ObHiveMetastoreClient::list_partition_names(const ObString &ns_name,
                                                const ObString &tb_name,
                                                const ObNameCaseMode case_mode,
                                                std::vector<std::string> &partition_names)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  // TODO(bitao): add dynamic max_parts variable
  // Note: partition_names is partition spec collections.
  // examples:
  //  p1: year=2025/month=1/day=1
  //  p2: year=2025/month=2/day=1
  //  p3: year=2025/month=3/day=2
  ListPartitionNamesOperation partition_names_op(partition_names, d_name, t_name);
  if (OB_FAIL(try_call_hive_client(partition_names_op))) {
    LOG_WARN("failed to call get table partition name operation", K(ret), K(ns_name), K(tb_name));
  }
  return ret;
}

int ObHiveMetastoreClient::get_part_values_rows(const ObString &ns_name,
                                                const ObString &tb_name,
                                                const ObNameCaseMode case_mode,
                                                const FieldSchemas &part_keys,
                                                PartitionValuesRows &partition_values_rows)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  if (OB_UNLIKELY(part_keys.empty())) {
    ret = OB_HMS_PARTITION_ERROR;
    LOG_WARN("invalid partition keys to get partition value", K(ret));
  } else {
    GetPartitionValuesOperation partition_values_op(partition_values_rows,
                                                    d_name,
                                                    t_name,
                                                    part_keys);
    if (OB_FAIL(try_call_hive_client(partition_values_op))) {
      LOG_WARN("failed to execute to get partition values operation",
               K(ret),
               K(ns_name),
               K(tb_name));
    }
  }
  return ret;
}

int ObHiveMetastoreClient::get_table_statistics(const ObString &ns_name,
                                                const ObString &tb_name,
                                                const ObNameCaseMode case_mode,
                                                const std::vector<std::string> &column_names,
                                                bool &found,
                                                ApacheHive::TableStatsResult &table_stats_result)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  LOG_TRACE("get table statistics", K(ret), K(ns_name), K(tb_name),
            "column_count", column_names.size());

  if (OB_UNLIKELY(column_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column names should not be empty for table statistics request",
             K(ret), K(ns_name), K(tb_name));
  } else {
    GetTableStatisticsOperation table_stats_op(table_stats_result, d_name, t_name, column_names, found);
    if (OB_FAIL(try_call_hive_client(table_stats_op))) {
      LOG_WARN("failed to execute get table statistics operation", K(ret), K(ns_name), K(tb_name));
    } else if (OB_UNLIKELY(!found)) {
      LOG_TRACE("table statistics not found", K(ret), K(ns_name), K(tb_name));
    } else {
      LOG_TRACE("table statistics operation completed successfully", K(ret), K(ns_name), K(tb_name));
    }
  }

  return ret;
}

int ObHiveMetastoreClient::get_partition_statistics(const ObString &ns_name,
                                                    const ObString &tb_name,
                                                    const ObNameCaseMode case_mode,
                                                    const std::vector<std::string> &column_names,
                                                    const std::vector<std::string> &partition_names,
                                                    bool &found,
                                                    ApacheHive::PartitionsStatsResult &partition_stats_result)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  LOG_TRACE("get partition statistics", K(ret), K(ns_name), K(tb_name),
            "column_count", column_names.size(), "partition_count", partition_names.size());

  if (OB_UNLIKELY(column_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column names should not be empty for partition statistics request",
             K(ret), K(ns_name), K(tb_name));
  } else if (OB_UNLIKELY(partition_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition names should not be empty for partition statistics request",
             K(ret), K(ns_name), K(tb_name));
  } else {
    GetPartitionsStatisticsOperation partition_stats_op(partition_stats_result, d_name, t_name,
                                                        column_names, partition_names, found);
    if (OB_FAIL(try_call_hive_client(partition_stats_op))) {
      LOG_WARN("failed to execute get partitions statistics operation", K(ret), K(ns_name), K(tb_name));
    } else if (OB_UNLIKELY(!found)) {
      LOG_TRACE("partition statistics not found", K(ret), K(ns_name), K(tb_name));
    } else {
      LOG_TRACE("partition statistics operation completed successfully", K(ret), K(ns_name), K(tb_name));
    }
  }

  return ret;
}

int ObHiveMetastoreClient::get_table_basic_stats(const ObString &ns_name,
                                                 const ObString &tb_name,
                                                 const ObNameCaseMode case_mode,
                                                 ObHiveBasicStats &basic_stats)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  LOG_TRACE("get table basic stats", K(ret), K(ns_name), K(tb_name));

  basic_stats.reset();

  ApacheHive::Table table;
  bool found = false;
  GetTableOperation table_op(table, d_name, t_name, found);
  if (OB_FAIL(try_call_hive_client(table_op))) {
    LOG_WARN("failed to execute get table operation", K(ret), K(ns_name), K(tb_name));
  } else if (OB_UNLIKELY(!found)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not found", K(ret), K(ns_name), K(tb_name));
  } else {
    // Extract basic stats from table parameters
    extract_basic_stats_from_parameters(table.parameters, basic_stats);
    LOG_TRACE("table basic stats extracted", K(ret), K(ns_name), K(tb_name), K(basic_stats));
  }

  return ret;
}

int ObHiveMetastoreClient::get_partition_basic_stats(const ObString &ns_name,
                                                    const ObString &tb_name,
                                                    const ObNameCaseMode case_mode,
                                                    const std::vector<std::string> &partition_names,
                                                    std::vector<ObHiveBasicStats> &partition_basic_stats)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  LOG_TRACE("get partition basic stats", K(ret), K(ns_name), K(tb_name),
            "partition_count", partition_names.size());

  partition_basic_stats.clear();

  if (OB_UNLIKELY(partition_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition names should not be empty for partition basic stats request",
             K(ret), K(ns_name), K(tb_name));
  } else {
    // 获取指定分区的信息
    Partitions partitions;
    GetPartitionsByNamesOperation partitions_op(partitions, d_name, t_name, partition_names);

    if (OB_FAIL(try_call_hive_client(partitions_op))) {
      LOG_WARN("failed to call get partitions by names operation", K(ret), K(ns_name), K(tb_name),
               "partition_count", partition_names.size());
    } else {
      partition_basic_stats.reserve(partition_names.size());

      if (partitions.size() == partition_names.size()) {
        for (size_t i = 0; i < partition_names.size(); ++i) {
          ObHiveBasicStats stats;
          extract_basic_stats_from_parameters(partitions[i].parameters, stats);
          partition_basic_stats.push_back(stats);
          LOG_TRACE("partition basic stats extracted by index", K(ret), K(ns_name), K(tb_name),
                   "partition_name", partition_names[i].c_str(), "index", i, K(stats));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition count mismatch", K(ret), K(ns_name), K(tb_name),
                 "requested", partition_names.size(), "returned", partitions.size());
      }
    }
  }

  return ret;
}

int ObHiveMetastoreClient::lock(const ApacheHive::LockRequest &lock_request,
                                ApacheHive::LockResponse &lock_response)
{
  int ret = OB_SUCCESS;
  bool success = false;
  LockOperation lock_op(lock_response, success, lock_request);
  if (OB_FAIL(try_call_hive_client(lock_op))) {
    LOG_WARN("failed to execute lock operation", K(ret));
  } else if (!success) {
    ret = OB_HMS_ERROR;
    LOG_WARN("lock operation completed but failed to acquire lock",
             K(ret), "lock_state", lock_response.state, "error_msg", lock_response.errorMessage.c_str());
  } else {
    LOG_TRACE("lock operation succeeded", K(ret), "lock_id", lock_response.lockid);
  }
  return ret;
}

int ObHiveMetastoreClient::unlock(const ApacheHive::UnlockRequest &unlock_request)
{
  int ret = OB_SUCCESS;
  UnlockOperation unlock_op(unlock_request);
  if (OB_FAIL(try_call_hive_client(unlock_op))) {
    LOG_WARN("failed to execute unlock operation", K(ret), "lock_id", unlock_request.lockid);
  } else {
    LOG_TRACE("unlock operation succeeded", K(ret), "lock_id", unlock_request.lockid);
  }
  return ret;
}

int ObHiveMetastoreClient::alter_table(const ObString &db_name,
                                       const ObString &tb_name,
                                       const ApacheHive::Table &new_table,
                                       const ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String d_name = String(db_name.ptr(), db_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  AlterTableOperation alter_op(d_name, t_name, new_table);
  if (OB_FAIL(try_call_hive_client(alter_op))) {
    LOG_WARN("failed to execute alter table operation", K(ret), K(db_name), K(tb_name));
  } else {
    LOG_TRACE("alter table operation succeeded", K(ret), K(db_name), K(tb_name));
  }
  return ret;
}

int ObHiveMetastoreClient::alter_table_with_lock(const ObString &db_name,
                                                 const ObString &tb_name,
                                                 const ApacheHive::Table &new_table,
                                                 const ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String d_name = String(db_name.ptr(), db_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  // Use client id as the user name for lock identification
  String u_name = std::to_string(client_id_);
  const common::ObAddr &addr = GCTX.self_addr();
  char svr_ip[common::OB_IP_STR_BUFF];
  if (OB_UNLIKELY(!addr.ip_to_string(svr_ip, sizeof(svr_ip)))) {
    LOG_WARN("failed to convert server ip to string", K(ret), K(addr));
  } else {
    String h_name = String(svr_ip);
    LOG_TRACE("server ip", K(ret), K(addr));
    // Create alter table operation with user and hostname
    AlterTableOperation alter_op(d_name, t_name, new_table, u_name, h_name);
    // Use lock-aware call method
    if (OB_FAIL(try_call_hive_client_with_lock(alter_op))) {
      LOG_WARN("failed to execute alter table operation with lock",
               K(ret),
               K(db_name),
               K(tb_name),
               K_(client_id),
               K(svr_ip));
    } else {
      LOG_INFO("alter table operation with lock completed successfully",
               K(ret),
               K(db_name),
               K(tb_name),
               K_(client_id),
               K(svr_ip));
    }
  }
  return ret;
}

/* --------------------- end of ObHiveMetastoreClient ---------------------*/
} // namespace share
} // namespace oceanbase
