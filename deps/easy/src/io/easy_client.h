#ifndef EASY_CLIENT_H_
#define EASY_CLIENT_H_

#include "easy_define.h"
#include "io/easy_io_struct.h"

/**
 * 主动连接管理
 */

EASY_CPP_START

void *easy_client_list_find(easy_hash_t *table, easy_addr_t *addr);
int easy_client_list_add(easy_hash_t *table, easy_addr_t *addr, easy_hash_list_t *list);

EASY_CPP_END

#endif
