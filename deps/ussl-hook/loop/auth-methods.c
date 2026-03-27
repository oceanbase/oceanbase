/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

int g_ussl_client_auth_methods = USSL_AUTH_NONE;
// all methods are supported by default
int g_ussl_server_auth_methods = USSL_AUTH_NONE |
                                 USSL_AUTH_SSL_HANDSHAKE |
                                 USSL_AUTH_SSL_IO;
int g_ussl_enable_bypass_flag = 0;

void set_server_auth_methods(const int methods)
{
  g_ussl_server_auth_methods = methods;
}

int test_server_auth_methods(const int method)
{
  int ret = 0;
  if (g_ussl_server_auth_methods & method) {
    ret = 1;
  }
  return ret;
}

int get_server_auth_methods()
{
  return ATOMIC_LOAD(&g_ussl_server_auth_methods);
}

void set_client_auth_methods(const int methods)
{
  ATOMIC_STORE(&g_ussl_client_auth_methods, methods);
}

int get_client_auth_methods()
{
  return ATOMIC_LOAD(&g_ussl_client_auth_methods);
}

void ussl_set_auth_bypass_flag(int enable)
{
  g_ussl_enable_bypass_flag = enable;
}

int ussl_get_auth_bypass_flag()
{
  return g_ussl_enable_bypass_flag;
}

