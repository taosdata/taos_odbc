#ifndef _macros_h_
#define _macros_h_

#ifdef __cplusplus         /* { */
#define EXTERN_C_BEGIN     extern "C" {
#define EXTERN_C_END       }
#else                      /* }{ */
#define EXTERN_C_BEGIN
#define EXTERN_C_END
#endif                     /* } */

EXTERN_C_BEGIN


#define FA_HIDDEN __attribute__((visibility("hidden")))


EXTERN_C_END

#endif // _macros_h_

