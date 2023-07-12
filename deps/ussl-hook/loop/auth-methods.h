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

#ifndef USSL_HOOK_LOOP_AUTH_METHODS_
#define USSL_HOOK_LOOP_AUTH_METHODS_

extern void set_server_auth_methods(const int methods);
extern int test_server_auth_methods(const int method);
extern int get_server_auth_methods();

extern void set_client_auth_methods(const int methods);
extern int get_client_auth_methods();


#endif // USSL_HOOK_LOOP_AUTH_METHODS_
