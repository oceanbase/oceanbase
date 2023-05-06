#ifndef USSL_HOOK_LOOP_HANDLE_EVENT_
#define USSL_HOOK_LOOP_HANDLE_EVENT_

#define USSL_KEY_PATH "ussl-cfg/key"
#define USSL_AUTH_LIST_PATH "ussl-cfg/auth_list"
#define IP_STRING_MAX_LEN 64

extern int clientfd_sk_handle_event(clientfd_sk_t *s);
extern int acceptfd_sk_handle_event(acceptfd_sk_t *s);
extern void get_src_addr(int fd, char *buf, int len);

#endif // USSL_HOOK_LOOP_HANDLE_EVENT_