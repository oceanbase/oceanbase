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
#ifndef _SHARE_CATALOG_HIVE_OB_HIVE_METASTORE_H
#define _SHARE_CATALOG_HIVE_OB_HIVE_METASTORE_H

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "share/schema/ob_schema_struct.h"
#include "share/catalog/hive/thrift/gen_cpp/ThriftHiveMetastore.h"

#include <memory>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

namespace oceanbase
{
namespace share
{
// Forward declaration to avoid circular dependency
class ObHMSClientPool;

using namespace oceanbase::common;
using namespace oceanbase::sql;

// Setup using useful namespace.
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

// Create short name alias for Apache::Hadoop::Hive namespace
namespace ApacheHive = Apache::Hadoop::Hive;
using namespace ApacheHive;

// Many hive thrift methods use std::string as parameters type.
using String = std::string;
using Strings = std::vector<String>;
using FieldSchemas = std::vector<ApacheHive::FieldSchema>;
using Partitions = std::vector<ApacheHive::Partition>;
using PartitionValuesRows = std::vector<ApacheHive::PartitionValuesRow>;

// HiveMetastore related configs.
static const int MAX_HIVE_METASTORE_CLIENT_RETRY = 3;

const char *const LAST_DDL_TIME = "transient_lastDdlTime";

// Kerberos related.
static constexpr int64_t DEFAULT_KINIT_TIMEOUT_US = 10LL * 60LL * 1000LL * 1000LL; // 10 min
static constexpr int64_t OB_MAX_ACCESS_INFO_LENGTH = 1600;
const char *const HMS_KEYTAB = "hms_keytab=";
const char *const HMS_PRINCIPAL = "hms_principal=";
const char *const HMS_KRB5CONF = "hms_krb5conf=";

struct ObHiveBasicStats
{
  ObHiveBasicStats() : num_files_(0), num_rows_(0), total_size_(0) {}

  void reset() {
    num_files_ = 0;
    num_rows_ = 0;
    total_size_ = 0;
  }

  bool is_valid() const {
    return num_files_ >= 0 && num_rows_ >= 0 && total_size_ >= 0;
  }

  TO_STRING_KV(K(num_files_), K(num_rows_), K(total_size_));
  int64_t num_files_;
  int64_t num_rows_;
  int64_t total_size_;
};

/*-----------------start of Hive Client Operations----------------------------*/

// Abstract base class for operations that require locking
// All lockable operations must implement these methods to provide lock information
class LockableOperation
{
public:
  virtual ~LockableOperation() = default;

  // Basic information required for locking
  virtual const String& get_db_name() const = 0;
  virtual const String& get_table_name() const = 0;

  // Lock attributes - each operation decides what kind of lock it needs
  virtual ApacheHive::LockType::type get_lock_type() const = 0;
  virtual ApacheHive::LockLevel::type get_lock_level() const = 0;
  virtual ApacheHive::DataOperationType::type get_operation_type() const = 0;
  virtual bool is_transactional() const = 0;
  virtual bool is_dynamic_partition_write() const = 0;

  // Metadata for lock requests
  virtual String get_user() const = 0;
  virtual String get_hostname() const = 0;
  virtual String get_agent_info() const {
    return "OceanBase-HMS-Client";  // Default agent info, can be overridden
  }
};

template <typename Derived>
class ObHiveClientOperation
{
public:
  // Static polymorphic method: derived classes must implement execute_impl
  void execute(ThriftHiveMetastoreClient *client)
  {
    static_cast<Derived *>(this)->execute_impl(client);
  }
};

class ListDBNamesOperation : public ObHiveClientOperation<ListDBNamesOperation>
{
public:
  ListDBNamesOperation(Strings &databases) : databases_(databases)
  {
  }

  ListDBNamesOperation(const ListDBNamesOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  Strings &databases_;
};

class GetDatabaseOperation : public ObHiveClientOperation<GetDatabaseOperation>
{
public:
  GetDatabaseOperation(ApacheHive::Database &db, const String &db_name, bool &found)
      : db_(db), db_name_(db_name), found_(found)
  {
  }

  GetDatabaseOperation(const GetDatabaseOperation &) = delete;

  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  ApacheHive::Database &db_;
  const String &db_name_;
  bool &found_;
};

class GetAllTablesOperation : public ObHiveClientOperation<GetAllTablesOperation>
{
public:
  GetAllTablesOperation(Strings &tb_names, const String &db_name)
      : tb_names_(tb_names), db_name_(db_name)
  {
  }

  GetAllTablesOperation(const GetAllTablesOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  Strings &tb_names_;
  const String &db_name_;
};

class ListPartitionsOperation : public ObHiveClientOperation<ListPartitionsOperation>
{
public:
  ListPartitionsOperation(Partitions &partitions,
                          const String &db_name,
                          const String &tb_name,
                          const int16_t max_parts)
      : partitions_(partitions), db_name_(db_name), tb_name_(tb_name), max_parts_(max_parts)
  {
  }

  ListPartitionsOperation(Partitions &partitions,
                          const String &db_name,
                          const String &tb_name)
      : partitions_(partitions), db_name_(db_name), tb_name_(tb_name), max_parts_(-1)
  {
  }

  ListPartitionsOperation(const ListPartitionsOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  Partitions &partitions_;
  const String &db_name_;
  const String &tb_name_;
  const int16_t max_parts_;
};

class ListPartitionNamesOperation : public ObHiveClientOperation<ListPartitionNamesOperation>
{
public:
  ListPartitionNamesOperation(Strings &partition_names,
                              const String &db_name,
                              const String &tb_name,
                              const int16_t max_parts)
      : partition_names_(partition_names), db_name_(db_name), tb_name_(tb_name),
        max_parts_(max_parts)
  {
  }

  ListPartitionNamesOperation(Strings &partition_names,
                              const String &db_name,
                              const String &tb_name)
      : partition_names_(partition_names), db_name_(db_name), tb_name_(tb_name), max_parts_(-1)
  {
  }

  ListPartitionNamesOperation(const ListPartitionNamesOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  Strings &partition_names_;
  const String &db_name_;
  const String &tb_name_;
  const int16_t max_parts_;
};

class GetPartitionValuesOperation : public ObHiveClientOperation<GetPartitionValuesOperation>
{
public:
  GetPartitionValuesOperation(PartitionValuesRows &partition_values_rows,
                              const String &db_name,
                              const String &tb_name,
                              const FieldSchemas &partition_keys)
      : partition_values_rows_(partition_values_rows), db_name_(db_name), tb_name_(tb_name),
        partition_keys_(partition_keys)
  {
  }

  GetPartitionValuesOperation(const GetPartitionValuesOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  PartitionValuesRows &partition_values_rows_;
  const String &db_name_;
  const String &tb_name_;
  const FieldSchemas &partition_keys_;
};

class GetTableOperation : public ObHiveClientOperation<GetTableOperation>
{
public:
  GetTableOperation(ApacheHive::Table &table,
                    const String &db_name,
                    const String &tb_name,
                    bool &found)
      : table_(table), db_name_(db_name), tb_name_(tb_name), found_(found),
        is_newest_version_(false)
  {
  }

  GetTableOperation(const GetTableOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  ApacheHive::Table &table_;
  const String &db_name_;
  const String &tb_name_;
  bool &found_;
  bool is_newest_version_;
};

class GetLatestPartitionDdlTimeOperation
    : public ObHiveClientOperation<GetLatestPartitionDdlTimeOperation>
{
public:
  GetLatestPartitionDdlTimeOperation(int64_t &latest_ddl_time,
                                     const String &db_name,
                                     const String &tb_name,
                                     const int16_t max_parts)
      : latest_ddl_time_(latest_ddl_time), db_name_(db_name), tb_name_(tb_name),
        max_parts_(max_parts)
  {
  }

  GetLatestPartitionDdlTimeOperation(int64_t &latest_ddl_time,
                                     const String &db_name,
                                     const String &tb_name)
      : latest_ddl_time_(latest_ddl_time), db_name_(db_name), tb_name_(tb_name), max_parts_(-1)
  {
  }

  GetLatestPartitionDdlTimeOperation(const GetLatestPartitionDdlTimeOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  int64_t &latest_ddl_time_;
  const String &db_name_;
  const String &tb_name_;
  const int16_t max_parts_;
};

class LockOperation : public ObHiveClientOperation<LockOperation>
{
public:
  LockOperation(ApacheHive::LockResponse &lock_response,
                bool &success,
                const ApacheHive::LockRequest &lock_request)
      : lock_response_(lock_response), success_(success), lock_request_(lock_request)
  {
  }

  LockOperation(const LockOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  ApacheHive::LockResponse &lock_response_;
  bool &success_;
  const ApacheHive::LockRequest &lock_request_;
};

class UnlockOperation : public ObHiveClientOperation<UnlockOperation>
{
public:
  UnlockOperation(const ApacheHive::UnlockRequest &unlock_request)
      : unlock_request_(unlock_request)
  {
  }

  UnlockOperation(const UnlockOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  const ApacheHive::UnlockRequest &unlock_request_;
};

class AlterTableOperation : public ObHiveClientOperation<AlterTableOperation>,
                            public LockableOperation
{
public:
  AlterTableOperation(const String &db_name,
                      const String &tb_name,
                      const ApacheHive::Table &new_table,
                      const String &user = "oceanbase",
                      const String &hostname = "localhost")
      : db_name_(db_name), tb_name_(tb_name), new_table_(new_table), user_(user),
        hostname_(hostname)
  {
  }

  AlterTableOperation(const AlterTableOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

  // Implement LockableOperation interfaces.
  const String &get_db_name() const override
  {
    return db_name_;
  }
  const String &get_table_name() const override
  {
    return tb_name_;
  }

  ApacheHive::LockType::type get_lock_type() const override
  {
    return ApacheHive::LockType::EXCLUSIVE;
  }
  ApacheHive::LockLevel::type get_lock_level() const override
  {
    return ApacheHive::LockLevel::TABLE;
  }
  ApacheHive::DataOperationType::type get_operation_type() const override
  {
    return ApacheHive::DataOperationType::UNSET;
  }
  bool is_transactional() const override
  {
    return true;
  }
  bool is_dynamic_partition_write() const override
  {
    return false;
  }
  String get_user() const override
  {
    return user_;
  }
  String get_hostname() const override
  {
    return hostname_;
  }

private:
  const String &db_name_;
  const String &tb_name_;
  const ApacheHive::Table &new_table_;
  String user_;
  String hostname_;
};
class GetTableStatisticsOperation : public ObHiveClientOperation<GetTableStatisticsOperation>
{
public:
  GetTableStatisticsOperation(ApacheHive::TableStatsResult &table_stats_result,
                              const String &db_name,
                              const String &tb_name,
                              const std::vector<std::string> &column_names,
                              bool &found)
      : table_stats_result_(table_stats_result), db_name_(db_name), tb_name_(tb_name),
        column_names_(column_names), found_(found)
  {
  }

  GetTableStatisticsOperation(const GetTableStatisticsOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  ApacheHive::TableStatsResult &table_stats_result_;
  const String &db_name_;
  const String &tb_name_;
  const std::vector<std::string> &column_names_;
  bool &found_;
};

class GetPartitionsStatisticsOperation : public ObHiveClientOperation<GetPartitionsStatisticsOperation>
{
public:
  GetPartitionsStatisticsOperation(ApacheHive::PartitionsStatsResult &partition_stats_result,
                                   const String &db_name,
                                   const String &tb_name,
                                   const std::vector<std::string> &column_names,
                                   const std::vector<std::string> &partition_names,
                                   bool &found)
      : partition_stats_result_(partition_stats_result), db_name_(db_name), tb_name_(tb_name),
        column_names_(column_names), partition_names_(partition_names), found_(found)
  {
  }

  GetPartitionsStatisticsOperation(const GetPartitionsStatisticsOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  ApacheHive::PartitionsStatsResult &partition_stats_result_;
  const String &db_name_;
  const String &tb_name_;
  const std::vector<std::string> &column_names_;
  const std::vector<std::string> &partition_names_;
  bool &found_;
};

class GetAllPartitionsOperation : public ObHiveClientOperation<GetAllPartitionsOperation>
{
public:
  GetAllPartitionsOperation(Partitions &partitions,
                           const String &db_name,
                           const String &tb_name)
      : partitions_(partitions), db_name_(db_name), tb_name_(tb_name)
  {
  }

  GetAllPartitionsOperation(const GetAllPartitionsOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  Partitions &partitions_;
  const String &db_name_;
  const String &tb_name_;
};

class GetPartitionsByNamesOperation : public ObHiveClientOperation<GetPartitionsByNamesOperation>
{
public:
  GetPartitionsByNamesOperation(Partitions &partitions,
                               const String &db_name,
                               const String &tb_name,
                               const std::vector<std::string> &partition_names)
      : partitions_(partitions), db_name_(db_name), tb_name_(tb_name), partition_names_(partition_names)
  {
  }

  GetPartitionsByNamesOperation(const GetPartitionsByNamesOperation &) = delete;
  void execute_impl(ThriftHiveMetastoreClient *client);

private:
  Partitions &partitions_;
  const String &db_name_;
  const String &tb_name_;
  const std::vector<std::string> &partition_names_;
};

/*-----------------end of Hive Client Operations----------------------------*/

class ObHiveMetastoreClient
{
public:
  explicit ObHiveMetastoreClient();
  virtual ~ObHiveMetastoreClient();

  int init(const int64_t &socket_timeout, ObIAllocator *allocator);
  int setup_hive_metastore_client(const ObString &uri, const ObString &properties);
  // Open transport to connect to hive.
  int open();
  // Close transport to disconnect to hive.
  int close();
  // Release client back to pool (user should call this when done with client)
  int release();
  bool is_valid() const
  {
    return nullptr != hive_metastore_client_;
  }

public:
  int list_db_names(ObIAllocator &allocator, ObIArray<ObString> &ns_names);
  int get_database(const ObString &ns_name,
                   const ObNameCaseMode case_mode,
                   share::schema::ObDatabaseSchema &database_schema);
  int list_table_names(const ObString &db_name,
                       ObIAllocator &allocator,
                       ObIArray<ObString> &tb_names);
  int get_latest_schema_version(const ObString &ns_name,
                                const ObString &tb_name,
                                const ObNameCaseMode case_mode,
                                int64_t &schema_version);
  int get_table(const ObString &ns_name,
                const ObString &tb_name,
                const ObNameCaseMode case_mode,
                ApacheHive::Table &original_table);
  int list_partitions(const ObString &ns_name,
                      const ObString &tb_name,
                      Partitions &partitions);
  int list_partition_names(const ObString &ns_name,
                           const ObString &tb_name,
                           const ObNameCaseMode case_mode,
                           Strings &partition_names);
  int get_part_values_rows(const ObString &ns_name,
                           const ObString &tb_name,
                           const ObNameCaseMode case_mode,
                           const FieldSchemas &part_keys,
                           PartitionValuesRows &partition_values_rows);

  int lock(const ApacheHive::LockRequest &lock_request, ApacheHive::LockResponse &lock_response);
  int unlock(const ApacheHive::UnlockRequest &unlock_request);

  int alter_table(const ObString &db_name,
                  const ObString &tb_name,
                  const ApacheHive::Table &new_table,
                  const ObNameCaseMode case_mode = OB_NAME_CASE_INVALID);

  // Lock-aware alter table operation, automatically manage lock lifecycle
  int alter_table_with_lock(const ObString &db_name,
                            const ObString &tb_name,
                            const ApacheHive::Table &new_table,
                            const ObNameCaseMode case_mode = OB_NAME_CASE_INVALID);

  int64_t get_client_id() const
  {
    return client_id_;
  }

  // Pool management methods
  void set_client_pool(ObHMSClientPool *pool)
  {
    client_pool_ = pool;
  }
  void set_in_use_state(bool in_use)
  {
    is_in_use_ = in_use;
  }

  int get_table_statistics(const ObString &ns_name,
                          const ObString &tb_name,
                          const ObNameCaseMode case_mode,
                          const std::vector<std::string> &column_names,
                          bool &found,
                          ApacheHive::TableStatsResult &table_stats_result);
  int get_partition_statistics(const ObString &ns_name,
                              const ObString &tb_name,
                              const ObNameCaseMode case_mode,
                              const std::vector<std::string> &column_names,
                              const std::vector<std::string> &partition_names,
                              bool &found,
                              ApacheHive::PartitionsStatsResult &partition_stats_result);
  int get_table_basic_stats(const ObString &ns_name,
                           const ObString &tb_name,
                           const ObNameCaseMode case_mode,
                           ObHiveBasicStats &basic_stats);
  int get_partition_basic_stats(const ObString &ns_name,
                               const ObString &tb_name,
                               const ObNameCaseMode case_mode,
                               const std::vector<std::string> &partition_names,
                               std::vector<ObHiveBasicStats> &partition_basic_stats);

private:
  static int extract_host_and_port(const ObString &uri, char *host, int &port);
  static int64_t handle_ddl_time(String &time_str);
  static void extract_basic_stats_from_parameters(const std::map<std::string, std::string> &parameters,
                                                  ObHiveBasicStats &basic_stats);

private:
  template <typename Operation> int try_call_hive_client(Operation &&op);

  // Lock-aware operation wrapper: automatically acquire lock, execute operation, release lock
  template <typename Operation>
  int try_call_hive_client_with_lock(Operation &&op);

  // Helper method to build lock request for different operations
  int build_lock_request_for_operation(const LockableOperation &op,
                                       ApacheHive::LockRequest &lock_request);

  int init_kerberos(ObString &keytab,
                    ObString &principal,
                    ObString &krb5conf,
                    ObString &cache_name,
                    int64_t kinit_timeout = DEFAULT_KINIT_TIMEOUT_US);
  int extract_access_info(const char *access_info);

private:
  ObIAllocator *allocator_;
  // Execution assemble dependency path:
  // socket -> transport -> protocol -> hive_metastore_client
  std::shared_ptr<TSocket> socket_;
  std::shared_ptr<TTransport> transport_;
  std::shared_ptr<TProtocol> protocol_;
  std::shared_ptr<ThriftHiveMetastoreClient> hive_metastore_client_;
  ObString uri_;
  ObString properties_;
  ObString hms_keytab_;
  ObString hms_principal_;
  ObString hms_krb5conf_;
  ObString service_;
  ObString server_FQDN_;

  // minor lock for state, ref, conn, kerberos
  mutable SpinRWLock state_lock_;
  mutable SpinRWLock conn_lock_;
  mutable SpinRWLock kerberos_lock_;

  bool is_inited_;
  bool is_opened_;
  // Used to store the last kinit timestamp if using kerberos.
  int64_t last_kinit_ts_;
  int64_t client_id_;
  ObHMSClientPool *client_pool_; // Pool pointer for auto return
  bool is_in_use_;               // Flag to indicate if client is currently in use
  int64_t socket_timeout_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHiveMetastoreClient);
};
} // namespace share
} // namespace oceanbase
#endif /* _SHARE_CATALOG_HIVE_OB_HIVE_METASTORE_H */