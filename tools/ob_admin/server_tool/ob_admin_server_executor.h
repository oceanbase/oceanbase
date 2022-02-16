/*
 * ob_admin_server_executor.h
 *
 *  Created on: Aug 29, 2017
 *      Author: yuzhong.zhao
 */

#ifndef OB_ADMIN_SERVER_EXECUTOR_H_
#define OB_ADMIN_SERVER_EXECUTOR_H_
#include "../ob_admin_executor.h"
#include "ob_admin_routine.h"

namespace oceanbase
{
namespace tools
{

class ObAdminServerExecutor : public ObAdminExecutor
{
public:
  enum SSL_MODE {
    SSL_MODE_NONE = 0, 
    SSL_MODE_INTL = 1, 
    SSL_MODE_SM   = 2,
  };
  const char *const OB_CLIENT_SSL_CA_FILE = "wallet/ca.pem";
  const char *const OB_CLIENT_SSL_CERT_FILE = "wallet/client-cert.pem";
  const char *const OB_CLIENT_SSL_KEY_FILE = "wallet/client-key.pem";
public:
  ObAdminServerExecutor();
  virtual ~ObAdminServerExecutor();
  virtual int execute(int argc, char *argv[]);
  int load_ssl_config();
private:
  bool parse_command(int argc, char *argv[]);
  void usage() const;
private:
  bool inited_;
  obrpc::ObNetClient client_;
  obrpc::ObSrvRpcProxy srv_proxy_;

  common::ObAddr dst_server_;
  int64_t timeout_;
  std::string cmd_;
  SSL_MODE ssl_mode_;
  int ssl_cfg_mode_; //0 for local file mode, 1 for bkmi mode

  static const std::string DEFAULT_HOST;
  static const int DEFAULT_PORT;
  static const int64_t DEFAULT_TIMEOUT;
};

}
}

#endif /* OB_ADMIN_SERVER_EXECUTOR_H_ */
