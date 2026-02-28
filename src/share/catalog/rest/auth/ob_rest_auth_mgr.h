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

#ifndef _SHARE_OB_REST_AUTH_MGR_H
#define _SHARE_OB_REST_AUTH_MGR_H

#include "src/share/catalog/ob_catalog_properties.h"
#include "src/share/catalog/rest/requests/ob_rest_http_request.h"
#include "src/share/catalog/rest/responses/ob_rest_http_response.h"
#include "lib/allocator/ob_fifo_allocator.h"

namespace oceanbase
{
namespace share
{

class ObRestOAuth2Key
{
public:
  ObRestOAuth2Key();
  ObRestOAuth2Key(const uint64_t tenant_id, const uint64_t catalog_id,
                  const ObString &access_id, const ObString &access_key,
                  const ObString &scope, const ObString &oauth2_svr_uri);
  ObRestOAuth2Key(const ObRestOAuth2Key &other);

  ObRestOAuth2Key &operator=(const ObRestOAuth2Key &other);
  int assign(const ObRestOAuth2Key &other);
  ~ObRestOAuth2Key() {}
  uint64_t hash() const;

  int hash(uint64_t &hash_val) const;
  bool operator==(const ObRestOAuth2Key &other) const;
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(catalog_id), K_(accessid),
               K_(accesskey), K_(scope), K_(oauth2_svr_uri));
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  ObString accessid_;
  ObString accesskey_;
  ObString scope_;
  ObString oauth2_svr_uri_;
  char accessid_ptr_[ObRestCatalogProperties::OB_MAX_ACCESSID_LENGTH];
  char accesskey_ptr_[ObRestCatalogProperties::OB_MAX_ACCESSKEY_LENGTH];
  char scope_ptr_[ObRestCatalogProperties::OB_MAX_SCOPE_LENGTH];
  char oauth2_svr_uri_ptr_[OB_MAX_URI_LENGTH];
};

class ObRestAuthMgr;
class ObRestAuthMgrRefreshTask : public common::ObTimerTask
{
public:
  ObRestAuthMgrRefreshTask()
    : auth_mgr_(nullptr) {}
  void runTimerTask(void) override;
  ObRestAuthMgr *auth_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRestAuthMgrRefreshTask);
};

struct ObRestOauth2Value
{
public:
ObRestOauth2Value()
    :access_token_(), last_access_time_us_(0) {}
  ~ObRestOauth2Value()
  { reset(); }
  void reset();
  int assign(const ObRestOauth2Value &other);
  ObString access_token_;
  int64_t last_access_time_us_;
};

class ObRestOauth2ValueAccessTimeCallBack
{
public:
  explicit ObRestOauth2ValueAccessTimeCallBack(const bool update_access_time_us)
      : update_access_time_us_(update_access_time_us), original_access_time_us_(0)
  {}
  void operator()(hash::HashMapPair<ObRestOAuth2Key, ObRestOauth2Value> &v)
  {
    original_access_time_us_ = v.second.last_access_time_us_;
    if (update_access_time_us_) {
      v.second.last_access_time_us_ = ObTimeUtility::current_time();
    }
  };

  bool update_access_time_us_;
  int64_t original_access_time_us_;
};

class ObRestAuthMgr
{
using ObRestAuthType = ObRestCatalogProperties::ObRestAuthType;
public:
  ~ObRestAuthMgr();
  int init();

  void stop();
  int authenticate(const uint64_t tenant_id,
                   const uint64_t catalog_id,
                   ObRestHttpRequest &request,
                   const ObRestCatalogProperties &rest_properties,
                   ObIAllocator &allocator,
                   bool force_refresh = false);
  int refresh();
  static ObRestAuthMgr &get_instance();

  static constexpr const char *AuthHeader = "Authorization";

private:
  ObRestAuthMgr()
    :is_inited_(false),
     oauth2_credential_lock_(ObLatchIds::OBJECT_DEVICE_LOCK),
     oauth2_credential_map_(),
     tg_id_(-1),
     refresh_task_(),
     oauth2_credential_allocator_()
  {
  }
  static int extract_host_and_canonical_uri(ObString &full_url,
                                            ObString &host,
                                            ObString &canonical_uri,
                                            ObIAllocator &allocator);
  static int get_sigv4_signature(ObRestHttpRequest &request,
                                 const ObString &url,
                                 const ObRestCatalogProperties &rest_properties,
                                 ObIAllocator &allocator);
  static int generate_date_string(const ObTime &obtime, const ObString &format,
                                  ObString &date_str, ObIAllocator &allocator);
  static int sha256(const unsigned char *data, int data_len, unsigned char *output);
  static int hmac_sha256(const unsigned char *key, int key_len,
                         const unsigned char *data, int data_len,
                         unsigned char *output);

  int curl_oauth2_credential(const ObRestOAuth2Key &oauth2_key,
                             const bool update_access_time);

  int parse_oauth2_response(const ObString &response_body,
                            ObString &access_token,
                            ObString &token_type,
                            ObIAllocator &allocator);

  int get_oauth2_credential_from_map(const ObRestOAuth2Key &oauth2_key,
                                     ObString &access_token,
                                     ObIAllocator &allocator);

  bool is_inited_;
  common::SpinRWLock oauth2_credential_lock_;
  common::hash::ObHashMap<ObRestOAuth2Key, ObRestOauth2Value> oauth2_credential_map_;
  int tg_id_;
  ObRestAuthMgrRefreshTask refresh_task_;
  ObFIFOAllocator oauth2_credential_allocator_;
  static constexpr const char *ACCESS_TOKEN_KEY = "access_token";
  static constexpr const char *TOKEN_TYPE_KEY = "token_type";
  static constexpr const char *DEFAULT_TOKEN_TYPE = "Bearer";
  static constexpr const char *HOST_HEADER = "host";
  static constexpr const char *X_AMZ_DATE_HEADER = "x-amz-date";
  static constexpr const char *SIGNED_HEADERS = "host;x-amz-date";
  static constexpr const char *AWS4_REQUEST = "aws4_request";
  static constexpr const char *EMPTY_HASHED_PAYLOAD = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  DISALLOW_COPY_AND_ASSIGN(ObRestAuthMgr);
};

}
}

#endif /* OB_REST_AUTH_MGR_H */