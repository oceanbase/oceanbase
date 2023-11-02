#ifndef EASY_MESSAGE_H_
#define EASY_MESSAGE_H_

#include "easy_define.h"
#include "io/easy_io_struct.h"

/**
 * 接收message
 */

EASY_CPP_START

easy_message_t *easy_message_create(easy_connection_t *c);
easy_message_t *easy_message_create_nlist(easy_connection_t *c);
int easy_message_destroy(easy_message_t *m, int del);
int easy_session_process(easy_session_t *s, int stop, int err);
int easy_session_process_keep_connection_resilient(easy_session_t* s, int stop, int err);

EASY_CPP_END

#endif
