/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef USSL_HOOK_LOOP_AUTH_METHODS_
#define USSL_HOOK_LOOP_AUTH_METHODS_

extern void set_server_auth_methods(const int methods);
extern int test_server_auth_methods(const int method);
extern int get_server_auth_methods();

extern void set_client_auth_methods(const int methods);
extern int get_client_auth_methods();

extern void ussl_set_auth_bypass_flag(int enable);
extern int ussl_get_auth_bypass_flag();


#endif // USSL_HOOK_LOOP_AUTH_METHODS_
