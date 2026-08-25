/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "common/object/ob_object.h"
#include "lib/json/ob_json.h"
#include "lib/string/ob_string.h"
#include "sql/parser/parse_node.h"
#include "sql/engine/cmd/ob_load_data_parser.h"

#ifndef _OB_CATALOG_PROPERTIES_H_
#define _OB_CATALOG_PROPERTIES_H_

namespace oceanbase
{
namespace share
{
// Keep the four-byte enum representation used by existing schema/plan fields while
// allowing dynamically encoded plugin values in the range [-256, -1].
enum class ObLakeTableFormat : int32_t
{
  // C++ external-table plugin. The concrete plugin identity is the enum value
  // itself: the inclusive range [LAKE_PLUGIN_PLACEHOLDER_BEGIN,
  // LAKE_PLUGIN_PLACEHOLDER_END] maps to resident plugin slots [255, 0] in
  // ObExtFormatRegistry. So lake_table_format_ (already carried in every plan
  // struct, serialized via OB_UNIS) doubles as the plugin identity — no separate
  // plugin name/slot field is carried, and NO OB_UNIS_VERSION bump is needed.
  // The plugin order in `ext_plugin_config` maps to slot 0..255.
  LAKE_PLUGIN_PLACEHOLDER_BEGIN = -256,
  LAKE_PLUGIN_PLACEHOLDER_END = -1,
  INVALID = 0,
  ICEBERG,
  HIVE,
  ODPS,
};

/// Raw plugin slot index ([0, 256)) used internally by ObExtFormatRegistry (and the
/// loader/virtual-table diagnostic). It is NOT a field in any plan struct — the
/// plugin identity in the runtime flow is carried by the ObLakeTableFormat enum
/// value itself (within the plugin-placeholder range). Convert with lake_plugin_slot_of() /
/// lake_plugin_format_of().
using ObPluginSlot = int16_t;
static constexpr ObPluginSlot INVALID_LAKE_PLUGIN_SLOT = -1;

/// Enum value for the first plugin slot (slot 0).
static constexpr int64_t LAKE_PLUGIN_PLACEHOLDER_BASE
    = static_cast<int64_t>(ObLakeTableFormat::LAKE_PLUGIN_PLACEHOLDER_END);
/// Max number of plugin slots (matches ObExtFormatRegistry::MAX_PLUGINS).
static constexpr int64_t LAKE_PLUGIN_PLACEHOLDER_COUNT = 256;

/// True iff `slot` can be represented by the plugin-placeholder enum range.
inline bool is_valid_lake_plugin_slot(const ObPluginSlot slot)
{
  return slot >= 0 && static_cast<int64_t>(slot) < LAKE_PLUGIN_PLACEHOLDER_COUNT;
}

/// True iff `format` is one of the plugin placeholder slots.
inline bool is_lake_plugin_table(const ObLakeTableFormat format)
{
  const int64_t v = static_cast<int64_t>(format);
  return v <= LAKE_PLUGIN_PLACEHOLDER_BASE
         && v > LAKE_PLUGIN_PLACEHOLDER_BASE - LAKE_PLUGIN_PLACEHOLDER_COUNT;
}

/// True iff `format` identifies an Iceberg lake table.
inline bool is_iceberg_lake_table(const ObLakeTableFormat format)
{
  return ObLakeTableFormat::ICEBERG == format;
}

/// True iff `format` identifies a Hive lake table.
inline bool is_hive_lake_table(const ObLakeTableFormat format)
{
  return ObLakeTableFormat::HIVE == format;
}

/// Extract the plugin slot ([0, 256)) from a plugin-placeholder enum value, or
/// INVALID_LAKE_PLUGIN_SLOT when `format` is not in the plugin range.
inline ObPluginSlot lake_plugin_slot_of(const ObLakeTableFormat format)
{
  return is_lake_plugin_table(format)
             ? static_cast<ObPluginSlot>(
                   LAKE_PLUGIN_PLACEHOLDER_BASE - static_cast<int64_t>(format))
             : INVALID_LAKE_PLUGIN_SLOT;
}

/// Build the plugin-placeholder enum value for slot `slot` ([0, 256)), or
/// INVALID when the supplied slot is out of range.
inline ObLakeTableFormat lake_plugin_format_of(const ObPluginSlot slot)
{
  return is_valid_lake_plugin_slot(slot)
             ? static_cast<ObLakeTableFormat>(LAKE_PLUGIN_PLACEHOLDER_BASE - slot)
             : ObLakeTableFormat::INVALID;
}

/// Catalog-backed lake formats (Iceberg / Hive / C++ plugin). ODPS is not included.
inline bool is_lake_external_table(const ObLakeTableFormat format)
{
  return is_iceberg_lake_table(format)
         || is_hive_lake_table(format)
         || is_lake_plugin_table(format);
}

/// ODPS materializes catalog metadata into OB schema (unlike Iceberg/Hive/plugin).
inline bool is_ob_external_table(const ObLakeTableFormat format)
{
  return (ObLakeTableFormat::ODPS == format || ObLakeTableFormat::INVALID == format);
}

enum class ObURISelectionMode
{
  SEQUENTIAL = 0,
  MAX_MODE
};

class ObCatalogProperties
{
public:
  enum class CatalogType
  {
    INVALID_TYPE = -1,
    ODPS_TYPE,
    FILESYSTEM_TYPE,
    HMS_TYPE,
    REST_TYPE,
    MAX_TYPE
  };
  ObCatalogProperties() : type_(CatalogType::INVALID_TYPE) {}
  ObCatalogProperties(CatalogType type) : type_(type) {}
  virtual ~ObCatalogProperties() {}
  int to_string_with_alloc(ObString &str, ObIAllocator &allocator) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int to_string(char *buf, const int64_t buf_len, int64_t &pos) const;
  virtual int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const = 0;
  virtual int load_from_string(const common::ObString &str, common::ObIAllocator &allocator) = 0;
  static int parse_catalog_type(const common::ObString &str, CatalogType &type);
  static int resolve_catalog_type(const ParseNode &node, CatalogType &type);
  virtual int resolve_catalog_properties(const ParseNode &node) = 0;
  int encrypt_str(common::ObString &src, common::ObString &dst, ObIAllocator &allocator);
  int decrypt_str(common::ObString &src, common::ObString &dst, ObIAllocator &allocator);
  virtual int encrypt(ObIAllocator &allocator) = 0;
  virtual int decrypt(ObIAllocator &allocator) = 0;

public:
  CatalogType type_;
  static const char *CATALOG_TYPE_STR[];
};

class ObODPSCatalogProperties : public ObCatalogProperties
{
public:
  enum class ObOdpsCatalogOptions
  {
    ACCESSTYPE = 0,
    ACCESSID,
    ACCESSKEY,
    STSTOKEN,
    ENDPOINT,
    TUNNEL_ENDPOINT,
    PROJECT_NAME,
    QUOTA_NAME,
    COMPRESSION_CODE,
    REGION,
    API_MODE,
    MAX_OPTIONS
  };
  ObODPSCatalogProperties() : ObCatalogProperties(CatalogType::ODPS_TYPE),
              api_mode_(sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API) {}
  virtual ~ObODPSCatalogProperties() {}
  virtual int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int load_from_string(const common::ObString &str,
                               common::ObIAllocator &allocator) override;
  virtual int resolve_catalog_properties(const ParseNode &node) override;
  virtual int encrypt(ObIAllocator &allocator) override;
  virtual int decrypt(ObIAllocator &allocator) override;

public:
  static constexpr const char *OPTION_NAMES[] = {
      "ACCESSTYPE",
      "ACCESSID",
      "ACCESSKEY",
      "STSTOKEN",
      "ENDPOINT",
      "TUNNEL_ENDPOINT",
      "PROJECT_NAME",
      "QUOTA_NAME",
      "COMPRESSION_CODE",
      "REGION",
      "API_MODE"
  };
  common::ObString access_type_;
  common::ObString access_id_;
  common::ObString access_key_;
  common::ObString sts_token_;
  common::ObString endpoint_;
  common::ObString tunnel_endpoint_;
  common::ObString project_;
  common::ObString quota_;
  common::ObString compression_code_;
  common::ObString region_;
  sql::ObODPSGeneralFormat::ApiMode api_mode_;
};

class ObFilesystemCatalogProperties : public ObCatalogProperties
{
public:
  enum class ObFilesystemCatalogOptions
  {
    WAREHOUSE = 0,
    MAX_OPTIONS
  };
  static constexpr const char *OPTION_NAMES[] = {
      "WAREHOUSE",
  };
  ObFilesystemCatalogProperties() : ObCatalogProperties(CatalogType::FILESYSTEM_TYPE) {}
  virtual ~ObFilesystemCatalogProperties() = default;
  virtual int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int load_from_string(const common::ObString &str,
                               common::ObIAllocator &allocator) override;
  virtual int resolve_catalog_properties(const ParseNode &node) override;
  virtual int encrypt(ObIAllocator &allocator) override;
  virtual int decrypt(ObIAllocator &allocator) override;

  common::ObString warehouse_;
};

class ObHMSCatalogProperties : public ObCatalogProperties
{
private:
  static constexpr int64_t DEFAULT_HMS_CLIENT_POOL_SIZE = 20;
  static constexpr int64_t DEFAULT_HMS_CLIENT_SOCKET_TIMEOUT_US = 10LL * 1000LL * 1000LL; // 10 seconds
  static constexpr int64_t DEFAULT_CACHE_REFRESH_INTERVAL_SEC = 10 * 60L; // 10 min
  static constexpr int64_t INVALID_CACHE_REFRESH_INTERVAL_SEC = -1;

public:
  enum ObHiveCatalogOptions {
    URI = 0,
    PRINCIPAL,
    KEYTAB,
    KRB5CONF,
    MAX_CLIENT_POOL_SIZE,
    SOCKET_TIMEOUT,
    CACHE_REFRESH_INTERVAL_SEC,
    HMS_CATALOG_NAME, // FARM COMPAT WHITELIST
    HMS_PRINCIPAL,
    MAX_OPTIONS
  };
  ObHMSCatalogProperties() :
    ObCatalogProperties(CatalogType::HMS_TYPE)
  {
    max_client_pool_size_ = DEFAULT_HMS_CLIENT_POOL_SIZE;
    socket_timeout_ = DEFAULT_HMS_CLIENT_SOCKET_TIMEOUT_US;
    cache_refresh_interval_sec_ = INVALID_CACHE_REFRESH_INTERVAL_SEC;
  }
  virtual ~ObHMSCatalogProperties() {}
  virtual int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int load_from_string(const common::ObString &str, common::ObIAllocator &allocator) override;
  virtual int resolve_catalog_properties(const ParseNode &node) override;
  virtual int encrypt(ObIAllocator &allocator) override;
  virtual int decrypt(ObIAllocator &allocator) override;

  int64_t get_cache_refresh_interval_sec() const;
  bool is_set_cache_refresh_interval_sec() const;
public:
  static constexpr const char *OPTION_NAMES[] = {
    "URI",
    "PRINCIPAL",
    "KEYTAB",
    "KRB5CONF",
    "MAX_CLIENT_POOL_SIZE",
    "SOCKET_TIMEOUT",
    "CACHE_REFRESH_INTERVAL_SEC",
    "HMS_CATALOG_NAME",
    "HMS_PRINCIPAL",
  };
  common::ObString uri_;
  common::ObString principal_;
  common::ObString keytab_;
  common::ObString krb5conf_;
  common::ObString hms_catalog_name_;
  common::ObString service_principal_;
  int64_t max_client_pool_size_;
  int64_t socket_timeout_;   // us
  int64_t cache_refresh_interval_sec_;
};

class ObRestCatalogProperties : public ObCatalogProperties
{
public:
  enum ObRestCatalogOptions {
    URI = 0,
    PREFIX,
    AUTH_TYPE,
    ACCESSID,
    ACCESSKEY,
    SCOPE,
    OAUTH2_SVR_URI,
    SIGN_NAME,
    SIGN_REGION,
    TOKEN,
    VENDED_CREDENTAIL_ENABLED,
    MAX_CLIENT_POOL_SIZE,
    HTTP_TIMEOUT,
    HTTP_KEEPALIVE_TIME,
    MAX_OPTIONS
  };
  enum class ObRestAuthType
  {
    INVALID_TYPE = -1,
    NONE_TYPE,
    OAUTH2_TYPE,
    SIGV4_TYPE,
    MAX_TYPE
  };
  ObRestCatalogProperties()
  : ObCatalogProperties(CatalogType::REST_TYPE),
    uri_(), prefix_(), auth_type_(ObRestAuthType::NONE_TYPE),
    accessid_(), accesskey_(),
    scope_(), oauth2_svr_uri_(),
    sign_name_(), sign_region_(),
    token_(), vended_credential_enabled_(false),
    max_client_pool_size_(DEFAULT_MAX_CLIENT_POOL_SIZE),
    http_timeout_(DEFAULT_HTTP_TIMEOUT),
    http_keep_alive_time_(DEFAULT_HTTP_KEEP_ALIVE_TIME)
  {}
  virtual ~ObRestCatalogProperties() {}
  virtual int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int load_from_string(const common::ObString &str, common::ObIAllocator &allocator) override;
  virtual int resolve_catalog_properties(const ParseNode &node) override;
  virtual int encrypt(ObIAllocator &allocator) override;
  virtual int decrypt(ObIAllocator &allocator) override;

  int get_auth_type_str(common::ObString &auth_type_str) const;

  static constexpr const char *OPTION_NAMES[] = {
    "URI",
    "PREFIX",
    "AUTH_TYPE",
    "ACCESSID",
    "ACCESSKEY",
    "SCOPE",
    "OAUTH2_SVR_URI",
    "SIGN_NAME",
    "SIGN_REGION",
    "TOKEN",
    "VENDED_CREDENTIAL_ENABLED",
    "MAX_CLIENT_POOL_SIZE",
    "HTTP_TIMEOUT",
    "HTTP_KEEPALIVE_TIME"
  };
  static constexpr const char *HTTP_PREFIX = "http://";
  static constexpr const char *HTTPS_PREFIX = "https://";
  static constexpr const char *AUTH_TYPE_NAMES[] = {
    "none",
    "oauth2",
    "sigv4"
  };
  static_assert(sizeof(AUTH_TYPE_NAMES) / sizeof(AUTH_TYPE_NAMES[0]) == static_cast<int>(ObRestAuthType::MAX_TYPE),
                "AUTH_TYPE_NAMES size mismatch with ObRestAuthType");
  static constexpr int64_t DEFAULT_MAX_CLIENT_POOL_SIZE = 20;
  static constexpr int64_t DEFAULT_HTTP_TIMEOUT = 10 * 1000 * 1000; // 10 seconds
  static constexpr int64_t DEFAULT_HTTP_KEEP_ALIVE_TIME = 60 * 1000 * 1000; // 1 minute
  static constexpr int64_t OB_MAX_ACCESSID_LENGTH = 256;
  static constexpr int64_t OB_MAX_ACCESSKEY_LENGTH = 256;
  static constexpr int64_t OB_MAX_SCOPE_LENGTH = 256;
  common::ObString uri_;
  common::ObString prefix_;
  ObRestAuthType auth_type_;
  common::ObString accessid_;
  common::ObString accesskey_;
  common::ObString scope_;
  common::ObString oauth2_svr_uri_;
  common::ObString sign_name_;
  common::ObString sign_region_;
  common::ObString token_;
  bool vended_credential_enabled_;
  int64_t max_client_pool_size_;
  int64_t http_timeout_;
  int64_t http_keep_alive_time_;
};

} // namespace share
} // namespace oceanbase

#endif //_OB_CATALOG_PROPERTIES_H_
