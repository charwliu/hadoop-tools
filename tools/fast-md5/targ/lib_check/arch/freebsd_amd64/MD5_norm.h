#include <jni.h>
#ifndef _Included_com_twmacinta_util_MD5
#define _Included_com_twmacinta_util_MD5
#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT void JNICALL Java_com_twmacinta_util_MD5_Transform_1native
  (JNIEnv *, jobject, jintArray, jbyteArray, jint, jint);
#ifdef __cplusplus
}
#endif
#endif
