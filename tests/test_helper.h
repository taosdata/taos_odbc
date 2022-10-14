#ifndef _test_helper_h_
#define _test_helper_h_

#include "cjson/cJSON.h"

#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <string.h>

static inline int json_get_by_path(cJSON *root, const char *path, cJSON **json)
{
  const char *t = path;
  const char *s = path;
  cJSON *j = root;
  char buf[1024];
  buf[0] = '\0';
  int n;

  while (j) {
    if (!*s) break;
    while (*s == '/') ++s;
    if (!*s) break;
    t = s + 1;
    while (*t != '/' && *t) ++t;

    n = snprintf(buf, sizeof(buf), "%.*s", (int)(t-s), s);
    if (n<0 || (size_t)n>=sizeof(buf)) {
      W("buffer too small(%ld)", sizeof(buf));
      return -1;
    }
    s = t;

    if (!cJSON_IsArray(j) && !cJSON_IsObject(j)) {
      j = NULL;
      break;
    }

    char *end = NULL;
    long int i = strtol(buf, &end, 0);
    if (end && *end) {
      if (!cJSON_IsObject(j)) {
        j = NULL;
      } else {
        j = cJSON_GetObjectItem(j, buf);
      }
    } else if (!cJSON_IsArray(j)) {
      j = NULL;
    } else if (i>=cJSON_GetArraySize(j)) {
      j = NULL;
    } else {
      j = cJSON_GetArrayItem(j, i);
    }
  }

  if (json) *json = j;
  return 0;
}

static inline int json_get_by_item(cJSON *root, int item, cJSON **json)
{
  if (!cJSON_IsArray(root)) return -1;
  cJSON *v = NULL;
  if (item >= 0 && item < cJSON_GetArraySize(root)) {
    v = cJSON_GetArrayItem(root, item);
  }
  if (json) *json = v;
  return 0;
}

static inline const char* json_to_string(cJSON *json)
{
  if (!cJSON_IsString(json)) return NULL;
  return cJSON_GetStringValue(json);
}

// static int json_to_double(cJSON *json, double *v)
// {
//   if (!cJSON_IsNumber(json)) return -1;
//   if (v) *v = cJSON_GetNumberValue(json);
//   return 0;
// }

static inline const char* json_object_get_string(cJSON *json, const char *key)
{
  cJSON *val = NULL;
  int r = json_get_by_path(json, key, &val);
  if (r) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return NULL;
  }

  if (!val) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return NULL;
  }
  if (!cJSON_IsString(val)) {
    char *t1 = cJSON_PrintUnformatted(json);
    char *t2 = cJSON_PrintUnformatted(val);
    W("item (%s) in ==%s== is not string ==%s==", t1, key, t2);
    free(t1);
    free(t2);
    return NULL;
  }

  return cJSON_GetStringValue(val);
}

static inline int json_get_number(cJSON *json, double *v)
{
  if (!cJSON_IsNumber(json)) return -1;
  if (v) *v = cJSON_GetNumberValue(json);
  return 0;
}

static inline int json_object_get_number(cJSON *json, const char *key, double *v)
{
  cJSON *val = NULL;
  int r = json_get_by_path(json, key, &val);
  if (r) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return -1;
  }

  if (!val) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return -1;
  }
  if (!cJSON_IsNumber(val)) {
    char *t1 = cJSON_PrintUnformatted(json);
    char *t2 = cJSON_PrintUnformatted(val);
    W("item (%s) in ==%s== is not string ==%s==", t1, key, t2);
    free(t1);
    free(t2);
    return -1;
  }

  if (v) *v = cJSON_GetNumberValue(val);

  return 0;
}

static inline cJSON* json_object_get_array(cJSON *json, const char *key)
{
  cJSON *val = NULL;
  int r = json_get_by_path(json, key, &val);
  if (r) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return NULL;
  }

  if (!val) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("no item (%s) found in ==%s==", key, t1);
    free(t1);
    return NULL;
  }
  if (!cJSON_IsArray(val)) {
    char *t1 = cJSON_PrintUnformatted(json);
    char *t2 = cJSON_PrintUnformatted(val);
    W("item (%s) in ==%s== is not array ==%s==", t1, key, t2);
    free(t1);
    free(t2);
    return NULL;
  }

  return val;
}

// static cJSON* json_object_get_object(cJSON *json, const char *key)
// {
//   cJSON *val = NULL;
//   int r = json_get_by_path(json, key, &val);
//   if (r) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("no item (%s) found in ==%s==", key, t1);
//     free(t1);
//     return NULL;
//   }
// 
//   if (!val) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("no item (%s) found in ==%s==", key, t1);
//     free(t1);
//     return NULL;
//   }
//   if (!cJSON_IsObject(val)) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     char *t2 = cJSON_PrintUnformatted(val);
//     W("item (%s) in ==%s== is not object ==%s==", t1, key, t2);
//     free(t1);
//     free(t2);
//     return NULL;
//   }
// 
//   return val;
// }

// static const char* json_array_get_string(cJSON *json, int item)
// {
//   if (!cJSON_IsArray(json)) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("json array expected but got  ==%s==", t1);
//     free(t1);
//     return NULL;
//   }
//   cJSON *val = cJSON_GetArrayItem(json, item);
//   if (!val) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("no item[#%d] found in ==%s==", item+1, t1);
//     free(t1);
//     return NULL;
//   }
// 
//   if (!cJSON_IsString(val)) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     char *t2 = cJSON_PrintUnformatted(val);
//     W("item[#%d] in ==%s== is not string ==%s==", item+1, t1, t2);
//     free(t1);
//     free(t2);
//     return NULL;
//   }
// 
//   return cJSON_GetStringValue(val);
// }

// static int json_array_get_number(const char *json_file, int icase, cJSON *json, int item, double *v)
// {
//   if (!cJSON_IsArray(json)) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("json_file [%s] #%d:\n"
//         "json array expected but got  ==%s==", json_file, icase+1, t1);
//     free(t1);
//     return -1;
//   }
//   cJSON *val = cJSON_GetArrayItem(json, item);
//   if (!val) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("json_file [%s] #%d:\n"
//         "no item[#%d] found in ==%s==", json_file, icase+1, item+1, t1);
//     free(t1);
//     return -1;
//   }
// 
//   if (!cJSON_IsNumber(val)) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     char *t2 = cJSON_PrintUnformatted(val);
//     W("json_file [%s] #%d:\n"
//       "item[#%d] in ==%s== is not string ==%s==", json_file, icase+1, item+1, t1, t2);
//     free(t1);
//     free(t2);
//     return -1;
//   }
// 
//   if (v) *v = cJSON_GetNumberValue(val);
// 
//   return 0;
// }

// static cJSON* json_array_get_array(const char *json_file, int icase, cJSON *json, int item)
// {
//   if (!cJSON_IsArray(json)) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("json_file [%s] #%d:\n"
//         "json array expected but got  ==%s==", json_file, icase+1, t1);
//     free(t1);
//     return NULL;
//   }
//   cJSON *val = cJSON_GetArrayItem(json, item);
//   if (!val) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("json_file [%s] #%d:\n"
//         "no item[#%d] found in ==%s==", json_file, icase+1, item+1, t1);
//     free(t1);
//     return NULL;
//   }
// 
//   if (!cJSON_IsArray(val)) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     char *t2 = cJSON_PrintUnformatted(val);
//     W("json_file [%s] #%d:\n"
//       "item[#%d] in ==%s== is not array ==%s==", json_file, icase+1, item+1, t1, t2);
//     free(t1);
//     free(t2);
//     return NULL;
//   }
// 
//   return val;
// }

// static cJSON* json_array_get_object(const char *json_file, int icase, cJSON *json, int item)
// {
//   if (!cJSON_IsArray(json)) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("json_file [%s] #%d:\n"
//         "json array expected but got  ==%s==", json_file, icase+1, t1);
//     free(t1);
//     return NULL;
//   }
//   cJSON *val = cJSON_GetArrayItem(json, item);
//   if (!val) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     W("json_file [%s] #%d:\n"
//         "no item[#%d] found in ==%s==", json_file, icase+1, item+1, t1);
//     free(t1);
//     return NULL;
//   }
// 
//   if (!cJSON_IsObject(val)) {
//     char *t1 = cJSON_PrintUnformatted(json);
//     char *t2 = cJSON_PrintUnformatted(val);
//     W("json_file [%s] #%d:\n"
//       "item[#%d] in ==%s== is not object ==%s==", json_file, icase+1, item+1, t1, t2);
//     free(t1);
//     free(t2);
//     return NULL;
//   }
// 
//   return val;
// }

static inline cJSON* load_json_from_file(const char *json_file, FILE *fn)
{
  int r, e;
  r = fseek(fn, 0, SEEK_END);
  if (r) {
    e = errno;
    W("fseek file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }
  long len = ftell(fn);
  if (len == -1) {
    e = errno;
    W("ftell file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }
  r = fseek(fn, 0, SEEK_SET);
  if (r) {
    e = errno;
    W("fseek file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }

  char *buf = malloc(len + 1);
  if (!buf) {
    W("out of memory when processing file [%s]", json_file);
    return NULL;
  }

  size_t bytes = fread(buf, 1, len, fn);
  if (bytes != (size_t)len) {
    e = errno;
    W("fread file [%s] failed: [%d]%s", json_file, e, strerror(e));
    free(buf);
    return NULL;
  }

  buf[bytes] = '\0';
  const char *next = NULL;
  cJSON *json = cJSON_ParseWithOpts(buf, &next, true);
  if (!json) {
    W("parsing file [%s] failed: bad syntax: @[%s]", json_file, next);
    free(buf);
    return NULL;
  }

  free(buf);

  return json;
}

static inline cJSON* load_json_file(const char *json_file, char *buf, size_t bytes)
{
  int r = 0;

  FILE *fn = fopen(json_file, "r");
  if (!fn) {
    int e = errno;
    W("open file [%s] failed: [%d]%s", json_file, e, strerror(e));
    return NULL;
  }

  cJSON *json = load_json_from_file(json_file, fn);
  if (buf) {
    int n = snprintf(buf, bytes, "%s", json_file);
    do {
      if (n<0 || (size_t)n>=bytes) {
        W("buffer too small");
        r = -1;
        break;
      }
      char *path = dirname(buf);
      if (!path) {
        W("`dirname` failed");
        r = -1;
        break;
      }
    } while (0);
  }

  if (r) {
    cJSON_Delete(json);
    json = NULL;
  }

  fclose(fn);

  return json;
}

#endif // _test_helper_h_

