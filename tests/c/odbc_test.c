#include "odbc_helpers.h"

#include "../test_helper.h"

static int _connect(SQLHANDLE hconn, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLConnect(hconn, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
  if (FAILED(sr)) {
    E("connect [dsn:%s,uid:%s,pwd:%s] failed", dsn, uid, pwd);
    return -1;
  }

  return 0;
}

static int _driver_connect(SQLHANDLE hconn, const char *connstr)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLDriverConnect(hconn, NULL, (SQLCHAR*)connstr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  if (FAILED(sr)) {
    E("driver_connect [connstr:%s] failed", connstr);
    return -1;
  }

  return 0;
}

static int run_case(SQLHANDLE hconn, cJSON *json)
{
  (void)hconn;

  int r = 0;

  const char *dsn     = json_object_get_string(json, "conn/dsn");
  const char *uid     = json_object_get_string(json, "conn/uid");
  const char *pwd     = json_object_get_string(json, "conn/pwd");
  const char *connstr = json_object_get_string(json, "conn/connstr");

  if (dsn) {
    r = _connect(hconn, dsn, uid, pwd);
    if (r) return -1;
  } else if (connstr) {
    r = _driver_connect(hconn, connstr);
    if (r) return -1;
  } else {
    char *t1 = cJSON_PrintUnformatted(json);
    W("lack conn/dsn or conn/driver in json, ==%s==", t1);
    free(t1);
    return -1;
  }

  CALL_SQLDisconnect(hconn);
  return 0;
}

static int run_json_file(SQLHANDLE hconn, cJSON *json)
{
  int r = 0;

  if (!cJSON_IsArray(json)) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("json array is required but got ==%s==", t1);
    free(t1);
    return -1;
  }

  for (int i=0; i>=0; ++i) {
    cJSON *json_case = NULL;
    if (json_get_by_item(json, i, &json_case)) break;
    if (!json_case) break;
    int positive = 1; {
      cJSON *pj = NULL;
      json_get_by_path(json_case, "positive", &pj);
      if (pj && !cJSON_IsTrue(pj)) positive = 0;
    }

    LOG_CALL("%s case[#%d]", positive ? "positive" : "negative", i+1);

    r = run_case(hconn, json_case);

    r = !(!r ^ !positive);
    LOG_FINI(r, "%s case[#%d]", positive ? "positive" : "negative", i+1);

    if (r) break;
  }

  return r;
}

static int try_and_run_file(SQLHANDLE hconn, const char *file)
{
  int r = 0;
  cJSON *json = load_json_file(file, NULL, 0);
  if (!json) return -1;

  const char *base = basename((char*)file);

  LOG_CALL("case %s", base);
  r = run_json_file(hconn, json);
  LOG_FINI(r, "case %s", base);

  cJSON_Delete(json);
  return r;
}

static int try_and_run(SQLHANDLE hconn, cJSON *json_test_case, const char *path)
{
  const char *s = json_to_string(json_test_case);
  if (!s) {
    char *t1 = cJSON_PrintUnformatted(json_test_case);
    W("json_test_case string expected but got ==%s==", t1);
    free(t1);
    return -1;
  }

  char buf[PATH_MAX+1];
  int n = snprintf(buf, sizeof(buf), "%s/%s.json", path, s);
  if (n<0 || (size_t)n>=sizeof(buf)) {
    W("buffer too small:%d", n);
    return -1;
  }

  return try_and_run_file(hconn, buf);
}

static int load_and_run(SQLHANDLE hconn, const char *json_test_cases_file)
{
  int r = 0;

  char path[PATH_MAX+1];
  cJSON *json_test_cases = load_json_file(json_test_cases_file, path, sizeof(path));
  if (!json_test_cases) return -1;

  do {
    if (!cJSON_IsArray(json_test_cases)) {
      char *t1 = cJSON_PrintUnformatted(json_test_cases);
      W("json_test_cases array expected but got ==%s==", t1);
      free(t1);
      r = -1;
      break;
    }
    cJSON *guess = cJSON_GetArrayItem(json_test_cases, 0);
    if (!cJSON_IsString(guess)) {
      r = try_and_run_file(hconn, json_test_cases_file);
      break;
    }
    for (int i=0; i>=0; ++i) {
      cJSON *json_test_case = NULL;
      if (json_get_by_item(json_test_cases, i, &json_test_case)) break;
      if (!json_test_case) break;
      r = try_and_run(hconn, json_test_case, path);
      if (r) break;
    }
  } while (0);

  cJSON_Delete(json_test_cases);

  return r;
}

static int process_by_args_conn(int argc, char *argv[], SQLHANDLE hconn)
{
  (void)argc;
  (void)argv;

  int r = 0;
  const char *json_test_cases_file = getenv("ODBC_TEST_CASES");
  if (!json_test_cases_file) {
    W("set environment `ODBC_TEST_CASES` to the test cases file");
    return -1;
  }

  LOG_CALL("load_and_run(%s)", json_test_cases_file);
  r = load_and_run(hconn, json_test_cases_file);
  LOG_FINI(r, "load_and_run(%s)", json_test_cases_file);
  return r;
}

static int process_by_args_env(int argc, char *argv[], SQLHANDLE henv)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hconn  = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (FAILED(sr)) return -1;

  r = process_by_args_conn(argc, argv, hconn);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);

  return r;
}

int main(int argc, char *argv[])
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE henv  = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  if (FAILED(sr)) return 1;

  do {
    sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (FAILED(sr)) { r = -1; break; }

    r = process_by_args_env(argc, argv, henv);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, henv);

  if (r == 0) D("==Success==");
  else        D("==Failure==");

  return !!r;
}

