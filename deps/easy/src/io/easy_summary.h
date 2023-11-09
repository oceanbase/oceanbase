#ifndef  EASY_SUMMARY_H
#define  EASY_SUMMARY_H

#include "easy_define.h"

#include "io/easy_io_struct.h"
#include "io/easy_log.h"

EASY_CPP_START
//////////////////////////////////////////////////////////////////////////////////
//接口函数
extern easy_summary_t          *easy_summary_create();
extern void                     easy_summary_destroy(easy_summary_t *sum);
extern easy_summary_node_t     *easy_summary_locate_node(int fd, easy_summary_t *sum, int hidden);
extern void                     easy_summary_destroy_node(int fd, easy_summary_t *sum);
extern void                     easy_summary_copy(easy_summary_t *src, easy_summary_t *dest);
extern easy_summary_t          *easy_summary_diff(easy_summary_t *ns, easy_summary_t *os);
extern void                     easy_summary_html_output(easy_pool_t *pool,
        easy_list_t *bc, easy_summary_t *sum, easy_summary_t *last);
extern void                     easy_summary_raw_output(easy_pool_t *pool,
        easy_list_t *bc, easy_summary_t *sum, const char *desc);

/////////////////////////////////////////////////////////////////////////////////

EASY_CPP_END

#endif
