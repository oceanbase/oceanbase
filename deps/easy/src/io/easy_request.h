#ifndef EASY_REQUEST_H_
#define EASY_REQUEST_H_

#include "easy_define.h"
#include "io/easy_io_struct.h"

/**
 * 一个request对象
 */

EASY_CPP_START

void easy_request_server_done(easy_request_t *r);
void easy_request_client_done(easy_request_t *r);
void easy_request_set_cleanup(easy_request_t *r, easy_list_t *output);
void check_easy_request_rt(easy_session_t *s);

EASY_CPP_END

#endif

