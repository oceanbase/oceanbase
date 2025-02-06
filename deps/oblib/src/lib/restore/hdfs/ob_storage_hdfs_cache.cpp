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

#include "ob_storage_hdfs_cache.h"
#include "sql/engine/connector/ob_java_env.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
namespace common
{
// --------------------- ObHdfsFsClient ---------------------
ObHdfsFsClient::ObHdfsFsClient()
    : hdfs_fs_(nullptr), lock_(ObLatchIds::OBJECT_DEVICE_LOCK),
      is_inited_(false), stopped_(false), ref_cnt_(0), last_modified_ts_(0)
{
}

int ObHdfsFsClient::init()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "ObHdfsFsClient init twice", K(ret));
  } else {
    last_modified_ts_ = ObTimeUtility::current_time();
    is_inited_ = true;
  }
  return ret;
}

bool ObHdfsFsClient::is_stopped() const
{
  SpinRLockGuard guard(lock_);
  return stopped_;
}

void ObHdfsFsClient::increase()
{
  SpinWLockGuard guard(lock_);
  ref_cnt_++;
  int ret = OB_SUCCESS;
  OB_LOG(TRACE, "increase client ref", K(ret), KP(this), K(ref_cnt_));
  last_modified_ts_ = ObTimeUtility::current_time();
}

void ObHdfsFsClient::release()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_LIKELY(ref_cnt_ <= 0)) {
    OB_LOG(TRACE, "client ref is zero", K(ret), KP(this), K(ref_cnt_), K(lbt()));
  } else {
    ref_cnt_--;
  }
  OB_LOG(TRACE, "decrease client ref", K(ret), KP(this), K(ref_cnt_), K(lbt()));
  last_modified_ts_ = ObTimeUtility::current_time();
}

bool ObHdfsFsClient::try_stop(const int64_t timeout)
{
  bool is_stopped = true;
  const int64_t abs_timeout_us = ObTimeUtility::current_time() + timeout;
  if (OB_SUCCESS == lock_.wrlock(abs_timeout_us)) {
    if (is_inited_) {
      const int64_t cur_time_us = ObTimeUtility::current_time();
      if (ref_cnt_ <= 0
          && cur_time_us - last_modified_ts_ >= MAX_HDFS_CLIENT_IDLE_DURATION_US) {
        stopped_ = true;
      } else {
        is_stopped = false;
      }
    }
    lock_.unlock();
  } else {
    is_stopped = false;
  }
  return is_stopped;
}

ObHdfsFsClient::~ObHdfsFsClient()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  is_inited_ = false;
  stopped_ = false;
  ref_cnt_ = 0;
  last_modified_ts_ = 0;
  if (OB_NOT_NULL(hdfs_fs_)) {
    // hdfs_fs maybe a nullptr, if it create failed.
    obHdfsDisconnect(hdfs_fs_);
    hdfs_fs_ = nullptr;
  }
}

// --------------------- ObHdfsCacheUtils ---------------------
int ObHdfsCacheUtils::get_namenode_and_path_from_uri(char *namenode,
                                                     const int64_t namenode_len,
                                                     char *path,
                                                     const int64_t path_len,
                                                     const ObString &uri)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(namenode) || OB_ISNULL(path)) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to handle namenode or path is null", K(ret), K(uri));
  } else if (OB_UNLIKELY(!uri.prefix_match(OB_HDFS_PREFIX))) {
    ret = OB_HDFS_MALFORMED_URI;
    OB_LOG(WARN, "failed to handle namenode or path with prefix", K(ret), K(uri));
  } else {
    // Full path is qualified, i.e. "scheme://authority/path/to/file".
    // Extract "scheme://authority".
    const char *ptr = uri.ptr();
    // Check last slash after "scheme://authority"
    const char *needed_last_slash = strchr(ptr + strlen(OB_HDFS_PREFIX), '/');
    if (OB_ISNULL(needed_last_slash)) {
      ret = OB_HDFS_MALFORMED_URI;
      OB_LOG(WARN, "failed to handle namenode and path", K(ret), K(uri),
             K(needed_last_slash));
    }

    if (OB_FAIL(ret)) {
    } else {
      // Handle namenode.
      const int64_t len = needed_last_slash - ptr - 1;
      if (OB_UNLIKELY(len > namenode_len)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "expected namenode len execeed malloc", K(ret), K(len), K(namenode_len));
      } else {
        strncpy(namenode, ptr, len + 1);
        namenode[len + 1] = '\0';
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      // Handle path.
      const int64_t len = strlen(needed_last_slash);
      if (OB_UNLIKELY(len > path_len)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "expected path len exceed malloc", K(ret), K(len), K(path_len));
      } else {
        strncpy(path, needed_last_slash, len + 1);
        path[len] = '\0';
      }
    }
  }
  OB_LOG(TRACE, "parse namenode and path detail", K(ret), K(namenode), K(path));
  return ret;
}

int ObHdfsCacheUtils::parse_hdfs_auth_info_(ObObjectStorageInfo *storage_info,
                                            char *krb5conf_path, const int64_t kconf_len,
                                            char *principal, const int64_t principal_len,
                                            char *keytab_path, const int64_t keytab_len,
                                            char *ticiket_path, const int64_t ticket_len,
                                            char *configs, const int64_t configs_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_info) || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "storage info is invalid", K(ret), KPC(storage_info));
  } else {
    char tmp[OB_MAX_HDFS_BACKUP_EXTENSION_LENGTH] = {0};
    char *token = nullptr;
    char *saved_ptr = nullptr;

    char *hdfs_extension = storage_info->hdfs_extension_;
    const int64_t extension_len = STRLEN(hdfs_extension);

    MEMCPY(tmp, hdfs_extension, extension_len);
    tmp[extension_len] = '\0';
    token = tmp;

    int64_t info_len = 0;
    int64_t str_len = 0;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (NULL == token) {
        break;
      }
      info_len = strlen(token);
      if (0 == strncmp(KRB5CONF, token, strlen(KRB5CONF))) {
        if (OB_FAIL(ob_set_field(token + strlen(KRB5CONF), krb5conf_path,
                                 kconf_len))) {
          OB_LOG(WARN, "failed to setup krb5conf path", K(ret), K(token),
                 K(krb5conf_path), K(kconf_len));
        }
      } else if (0 == strncmp(PRINCIPAL, token, strlen(PRINCIPAL))) {
        if (OB_FAIL(ob_set_field(token + strlen(PRINCIPAL), principal,
                                 principal_len))) {
          OB_LOG(WARN, "failed to setup principal", K(ret), K(token),
                 K(principal), K(principal_len));
        }
      } else if (0 == strncmp(KEYTAB, token, strlen(KEYTAB))) {
        if (OB_FAIL(ob_set_field(token + strlen(KEYTAB), keytab_path,
                                 keytab_len))) {
          OB_LOG(WARN, "failed to setup keytab path", K(ret), K(token),
                 K(keytab_path), K(keytab_len));
        }
      } else if (0 ==
                 strncmp(TICKET_CACHE_PATH, token, strlen(TICKET_CACHE_PATH))) {
        if (OB_FAIL(ob_set_field(token + strlen(TICKET_CACHE_PATH),
                                 ticiket_path, ticket_len))) {
          OB_LOG(WARN, "failed to setup ticket cache path", K(ret), K(token),
                 K(ticiket_path), K(ticket_len));
        }
      } else if (0 == strncmp(HDFS_CONFIGS, token, strlen(HDFS_CONFIGS))) {
        if (OB_FAIL(ob_set_field(token + strlen(HDFS_CONFIGS), configs,
                                 configs_len))) {
          OB_LOG(WARN, "failed to setup hdfs configs", K(ret), K(token),
                 K(configs), K(configs_len));
        }
      }
    }
  }
  return ret;
}

int ObHdfsCacheUtils::check_kerberized_(const char *krb5conf_path, const char *principal,
                                        const char *keytab_path, const char *ticiket_path,
                                        const char *configs, bool &is_kerberized,
                                        bool &using_ticket_cache,
                                        bool &using_keytab,
                                        bool &using_principal)
{
  int ret = OB_SUCCESS;
  // Init bool values
  is_kerberized = false;
  using_ticket_cache = true;
  using_keytab = true;
  using_principal = true;

  if (OB_NOT_NULL(krb5conf_path) && 0 != STRLEN(krb5conf_path)) {
    is_kerberized = true;
  }

  // Kerberos auth should check by ticket cache or the combination of
  // keytab and principal.
  bool is_valid_auth = false;

  if (is_kerberized) {
    if (OB_ISNULL(ticiket_path) || 0 == STRLEN(ticiket_path)) {
      using_ticket_cache = false;
      OB_LOG(TRACE, "use ticket cache path", K(ret), K(ticiket_path));
    }

    if (OB_ISNULL(keytab_path) || 0 == STRLEN(keytab_path)) {
      using_keytab = false;
      OB_LOG(TRACE, "use keytab", K(ret), K(keytab_path));
    }

    if (OB_ISNULL(principal) || 0 == STRLEN(principal)) {
      using_principal = false;
      OB_LOG(TRACE, "use principal", K(ret), K(principal));
    }

    is_valid_auth = using_ticket_cache || (using_keytab && using_principal);

    if (!is_valid_auth) {
      ret = OB_HDFS_PERMISSION_DENIED;
      OB_LOG(WARN, "insufficent permission to access kerberized hdfs", K(ret),
             K(is_kerberized), K(is_valid_auth), K(using_ticket_cache), K(using_keytab),
             K(using_principal), K(ticiket_path), K(keytab_path), K(principal));
    }
  }
  return ret;
}

int ObHdfsCacheUtils::create_fs_(ObHdfsFsClient *hdfs_client,
                                 const ObString &namenode,
                                 ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;

  char krb5conf_path[OB_MAX_HDFS_SINGLE_CONF_LENGTH] = { 0 };
  char principal[OB_MAX_HDFS_SINGLE_CONF_LENGTH] = { 0 };
  char keytab_path[OB_MAX_HDFS_SINGLE_CONF_LENGTH] = { 0 };
  char ticiket_path[OB_MAX_HDFS_SINGLE_CONF_LENGTH] = { 0 };
  char hdfs_configs[OB_MAX_HDFS_CONFS_LENGTH] = { 0 };

  bool is_kerberized = false;
  // Single check by ticket cache
  bool using_ticket_cache = true;
  // Check by combination of keytab and principal
  bool using_keytab = true;
  bool using_principal = true;

  hdfsBuilder *hdfs_builder = obHdfsNewBuilder();
  if (OB_ISNULL(hdfs_client)) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to get hdfs client", K(ret));
  } else if (OB_ISNULL(storage_info)) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to get storage info", K(ret));
  } else if (OB_ISNULL(hdfs_builder)) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to init hdfs_builder", K(ret));
  } else if (OB_FAIL(parse_hdfs_auth_info_(
          storage_info, krb5conf_path, sizeof(krb5conf_path), principal,
          sizeof(principal), keytab_path, sizeof(keytab_path), ticiket_path,
          sizeof(ticiket_path), hdfs_configs, sizeof(hdfs_configs)))) {
    OB_LOG(WARN, "failed to parse hdfs auth info", K(ret),
           K(storage_info->hdfs_extension_));
  } else if (OB_FAIL(check_kerberized_(krb5conf_path, principal, keytab_path,
                                       ticiket_path, hdfs_configs,
                                       is_kerberized, using_ticket_cache,
                                       using_keytab, using_principal))) {
    OB_LOG(WARN, "failed to check kerberized status", K(ret));
  } else {
    obHdfsBuilderSetNameNode(hdfs_builder, namenode.ptr());
    if (is_kerberized) {
      // Setup kerberized client can fallback to access with simple auth
      const int rv = obHdfsBuilderConfSetStr(
          hdfs_builder, "ipc.client.fallback-to-simple-auth-allowed", "true");
      if (0 != rv) {
        ret = OB_HDFS_INVALID_ARGUMENT;
        OB_LOG(WARN, "failed to set conf for fall back simple auth", K(ret), K(rv));
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        OB_LOG(TRACE, "storage info krb 5conf", K(ret), K(krb5conf_path));
        const char *krb5conf = krb5conf_path;
        if (OB_ISNULL(krb5conf)) {
          ret = OB_HDFS_INVALID_ARGUMENT;
          OB_LOG(WARN, "failed to get krb5conf", K(ret), K(krb5conf_path));
        } else {
          // Setup kerberos conf
          obHdfsBuilderSetKerb5Conf(hdfs_builder, krb5conf);
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        if (using_keytab && using_principal) {
          const char *tmp_principal = principal;
          const char *keytab = keytab_path;
          if (OB_ISNULL(tmp_principal)) {
            ret = OB_HDFS_INVALID_ARGUMENT;
            OB_LOG(WARN, "failed to get principal", K(ret), K(tmp_principal));
          } else if (OB_ISNULL(keytab)) {
            ret = OB_HDFS_INVALID_ARGUMENT;
            OB_LOG(WARN, "failed to get keytab", K(ret), K(keytab_path));
          } else {
            OB_LOG(TRACE, "get keytab and principal", K(ret), K(tmp_principal),
                   K(keytab));
            obHdfsBuilderSetPrincipal(hdfs_builder, tmp_principal);
            obHdfsBuilderSetKeyTabFile(hdfs_builder, keytab);
          }
          // Default setup the keytab login with auto renew
          const int rv = obHdfsBuilderConfSetStr(
              hdfs_builder, "hadoop.kerberos.keytab.login.autorenewal.enabled",
              "true");
          if (0 != rv) {
            ret = OB_HDFS_INVALID_ARGUMENT;
            OB_LOG(WARN, "failed to set conf for auto renewal", K(ret), K(rv));
          }
        } else if (using_ticket_cache) {
          const char *ticket_cache_path = ticiket_path;
          if (OB_ISNULL(ticket_cache_path)) {
            ret = OB_HDFS_INVALID_ARGUMENT;
            OB_LOG(WARN, "failed to get ticket cache path", K(ret),
                   K(ticiket_path));
          } else {
            OB_LOG(TRACE, "get ticket cache path", K(ret), K(ticket_cache_path));
            obHdfsBuilderSetKerbTicketCachePath(hdfs_builder, ticket_cache_path);
          }
        } else {
          ret = OB_HDFS_INVALID_ARGUMENT;
          OB_LOG(WARN, "failed to setup hdfs builder", K(ret), K(using_keytab),
                 K(using_principal), K(using_ticket_cache));
        }
      }
    }
  }

  // Handle other hadoop configs
  if (OB_ISNULL(hdfs_configs) || 0 == STRLEN(hdfs_configs)) {
    // do nothing
  } else {
    char tmp[1024] = {0};
    char *token = nullptr;
    char *saved_ptr = nullptr;
    char *inner_save_ptr = nullptr;

    char *key_value_token = nullptr;
    OB_LOG(TRACE, "storage info configs", K(ret), K(hdfs_configs));

    const char *configs = hdfs_configs;
    if (OB_ISNULL(configs)) {
      ret = OB_HDFS_INVALID_ARGUMENT;
      OB_LOG(WARN, "failed to get configs", K(ret), K(hdfs_configs));
    } else {
      const int64_t configs_len = strlen(configs);
      MEMCPY(tmp, configs, configs_len);
      tmp[configs_len] = '\0';
      token = tmp;
    }

    // Configs will split by "#", examples: key=value#key=value#...
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "#", &saved_ptr);
      if (token == NULL) {
        break;
      } else {
        char *inner_token = ::strtok_r(token, "=", &inner_save_ptr);
        char *key = inner_token;
        char *value = ::strtok_r(NULL, "=", &inner_save_ptr);
        if (OB_ISNULL(key)) {
          ret = OB_HDFS_INVALID_ARGUMENT;
          OB_LOG(WARN, "failed to get key of config", K(ret), K(inner_token));
        } else if (OB_ISNULL(value)) {
          ret = OB_HDFS_INVALID_ARGUMENT;
          OB_LOG(WARN, "failed to get value of config", K(ret), K(inner_token));
        } else {
          OB_LOG(INFO, "setup config with key and value", K(ret), K(key), K(value));
          const int rv = obHdfsBuilderConfSetStr(hdfs_builder, key, value);
          if (0 != rv) {
            ret = OB_HDFS_INVALID_ARGUMENT;
            OB_LOG(WARN, "failed to set conf key value", K(ret), K(rv), K(key), K(value));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // NOTE: free the builder to avoid memory leak.
    // Don't load this free after obHdfsBuilderConnect, because
    // obHdfsBuilderConnect call hdfsFreeBuilder method inner.
    OB_LOG(WARN, "failed to setup hdfs builder, start to free", K(ret));
    obHdfsFreeBuilder(hdfs_builder);
  }

  if (OB_SUCC(ret)) {
    obHdfsBuilderSetForceNewInstance(hdfs_builder);
    // It is normally not necessary to call hdfsFreeBuilder since
    // hdfsBuilderConnect frees the builder.
    hdfsFS hdfs_fs = obHdfsBuilderConnect(hdfs_builder);
    hdfs_client->set_hdfs_fs(hdfs_fs);
    // hdfs_client->hdfs_fs_ = obHdfsBuilderConnect(hdfs_builder);
    if (OB_LIKELY(!hdfs_client->is_valid_client())) {
      // NOTE: free the builder to avoid memory leak
      ret = OB_HDFS_INVALID_ARGUMENT;
      OB_LOG(WARN, "failed to create hdfs client connect", K(ret));
    }
  }

  return ret;
}

int ObHdfsCacheUtils::create_hdfs_fs_handle(const ObString &namenode,
                                            ObHdfsFsClient *hdfs_client,
                                            ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  sql::ObJavaEnv &java_env = sql::ObJavaEnv::getInstance();
  // This entry is first time to setup java env
  if (!java_env.is_env_inited()) {
    if (OB_FAIL(java_env.setup_java_env())) {
      OB_LOG(WARN, "failed to setup java env", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    sql::JVMFunctionHelper &helper = sql::JVMFunctionHelper::getInstance();
    if (OB_UNLIKELY(!helper.is_inited())) {
      if (OB_FAIL(helper.init_jni_env())) {
        OB_LOG(WARN, "failed to re-init jni env from helper", K(ret));
      } else if (OB_UNLIKELY(!helper.is_inited())) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "failed to re-init jni env from helper", K(ret));
      }
      OB_LOG(TRACE, "get helper init status", K(ret), K(helper.is_inited()));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(create_fs_(hdfs_client, namenode, storage_info))) {
    OB_LOG(WARN, "failed to create hdfs fs", K(ret));
  }
  return ret;
}

// --------------------- ObHdfsFsCache ---------------------

ObHdfsFsCache::ObHdfsFsCache()
    : lock_(ObLatchIds::OBJECT_DEVICE_LOCK), hdfs_client_map_(),
      is_inited_map_(false)
{
}

int ObHdfsFsCache::clean_hdfs_client_map_()
{
  int ret = OB_SUCCESS;
  OB_LOG(TRACE, "clean hdfs client map", K(ret), K(hdfs_client_map_.size()));
  hash::ObHashMap<int64_t, ObHdfsFsClient *>::iterator iter = hdfs_client_map_.begin();
  ObArray<int64_t> hdfs_clients_to_clean;
  while (OB_SUCC(ret) && iter != hdfs_client_map_.end()) {
    if (OB_NOT_NULL(iter->second)) {
      ObHdfsFsClient *hdfs_client = iter->second;
      if (hdfs_client->try_stop()) {
        if (OB_FAIL(hdfs_clients_to_clean.push_back(iter->first))) {
          OB_LOG(WARN, "failed to push back into hdfs_clients_to_clean", K(ret),
                 K(iter->first), KP(hdfs_client));
        } else {
          hdfs_client->~ObHdfsFsClient();
          ob_free(iter->second);
          iter->second = nullptr;
        }
      }
    } else if (OB_FAIL(hdfs_clients_to_clean.push_back(iter->first))) {
      OB_LOG(WARN, "failed to push back into hdfs_clients_to_clean",
              K(ret), K(iter->first), KP(iter->second));
    }
    iter++;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < hdfs_clients_to_clean.count(); i++) {
    if (OB_FAIL(hdfs_client_map_.erase_refactored(hdfs_clients_to_clean[i]))) {
      OB_LOG(WARN, "failed to clean hdfs client map", K(ret));
    }
  }

  return ret;
}

int ObHdfsFsCache::get_connection(const ObString &namenode,
                                  ObHdfsFsClient *&hdfs_client,
                                  ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  hdfs_client = nullptr;
  // Sometimes client is accessing hdfs with simple auth.
  // So the key should hash by namenode too.
  int64_t key = 0;
  char info_str[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  if (OB_ISNULL(storage_info)) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to get storage info", K(ret), KPC(storage_info));
  } else if (OB_FAIL(storage_info->get_storage_info_str(info_str, sizeof(info_str)))) {
    OB_LOG(WARN, "failed to get storage info str", K(ret), KPC(storage_info));
  } else {
    key = murmurhash(info_str, static_cast<int32_t>(strlen(info_str)), key);
    const char *tmp_namenode = namenode.ptr();
    key = murmurhash(tmp_namenode, static_cast<int32_t>(strlen(tmp_namenode)), key);
  }
  OB_LOG(TRACE, "check generated new key", K(ret), K(key), K(namenode), K(info_str), K(lbt()));

  // Delay to create the client map to avoid static instance construct for
  // creating failed.
  if (OB_UNLIKELY(!is_inited_map_)) {
    if (OB_FAIL(hdfs_client_map_.create(MAX_HDFS_CLIENT_NUM,
                                        OB_STORAGE_HDFS_ALLOCATOR))) {
      is_inited_map_ = false;
      OB_LOG(WARN, "failed to create hdfs client map", K(ret));
    } else {
      is_inited_map_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!is_inited_map_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to inited the ObHdfsFsCache", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (REACH_TIME_INTERVAL(MAX_HDFS_CLIENT_IDLE_DURATION_US)) {
    int tmp_ret = OB_SUCCESS;
    OB_LOG(TRACE, "start to clean hdfs client map", K(tmp_ret),
           K(hdfs_client_map_.size()));
    if (OB_TMP_FAIL(clean_hdfs_client_map_())) {
      OB_LOG(WARN, "failed to clean hdfs client map", K(tmp_ret),
             K(hdfs_client_map_.size()));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (FAILEDx(hdfs_client_map_.get_refactored(key, hdfs_client))) {
    if (ret == OB_HASH_NOT_EXIST) {
      OB_LOG(TRACE, "create new client by key", K(ret), K(hdfs_client_map_.size()), K(key));
      ret = OB_SUCCESS;
      void *hdfs_client_buf = nullptr;
      if (OB_ISNULL(hdfs_client_buf = ob_malloc(sizeof(ObHdfsFsClient),
                                                OB_STORAGE_HDFS_ALLOCATOR))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed to alloc buf for ob hdfs fs client", K(ret));
      } else {
        hdfs_client = new(hdfs_client_buf) ObHdfsFsClient();
        if (OB_FAIL(hdfs_client->init())) {
          OB_LOG(WARN, "failed to init hdfs fs client", K(ret));
        } else if (OB_FAIL(ObHdfsCacheUtils::create_hdfs_fs_handle(
                       namenode, hdfs_client, storage_info))) {
          OB_LOG(WARN, "failed to create hdfs fs handle", K(ret));
        } else if (OB_FAIL(hdfs_client_map_.set_refactored(key, hdfs_client))) {
          OB_LOG(WARN, "failed to insert into hdfs client map", K(ret), K(key),
                 K(namenode), KPC(storage_info));
        } else {
          OB_LOG(TRACE, "succeed create new hdfs fs client", K(ret),
                 K(namenode), KPC(storage_info), K(hdfs_client_map_.size()), K(key));
        }
      }

      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(hdfs_client)) {
          hdfs_client->~ObHdfsFsClient();
          hdfs_client = nullptr;
        }

        if (OB_NOT_NULL(hdfs_client_buf)) {
          ob_free(hdfs_client_buf);
          hdfs_client_buf = nullptr;
        }
      }
    } else {
      OB_LOG(WARN, "failed to get hdfs client from map", K(ret), K(namenode),
             KP(storage_info));
    }
  } else if (OB_UNLIKELY(hdfs_client->is_stopped())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "a stopped client remained is hdfs client map", K(ret),
           KP(hdfs_client), K(hdfs_client_map_.size()));
  }

  if (OB_SUCC(ret)) {
    hdfs_client->increase();
  }

  return ret;
}

} // namespace common
} // namespace oceanbase