/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "internal.h"

#include "os_port.h"
#include "conn.h"
#include "desc.h"
#include "enums.h"
#include "env.h"
#include "errs.h"
#include "log.h"
#include "setup.h"
#include "stmt.h"
#include "tls.h"

#include "conn_parser.h"

#include <string.h>

#include <sql.h>
#include <sqlext.h>
#include <sqlucode.h>

#include <odbcinst.h>
#pragma comment(lib, "comctl32.lib")
#include <commctrl.h>
#include "resource.h"

#define POST_INSTALLER_ERROR(hwndParent, code, fmt, ...)             \
do {                                                                 \
  char buf[4096];                                                    \
  char name[1024];                                                   \
  tod_basename(__FILE__, name, sizeof(name));                        \
  snprintf(buf, sizeof(buf), "%s[%d]%s():" fmt "",                   \
           name, __LINE__, __func__,                                 \
           ##__VA_ARGS__);                                           \
  SQLPostInstallerError(code, buf);                                  \
  if (hwndParent) {                                                  \
    MessageBox(hwndParent, buf, "Error", MB_OK|MB_ICONEXCLAMATION);  \
  }                                                                  \
} while (0)

static HINSTANCE ghInstance;
const char *className = "TAOS_ODBC_SetupLib";

BOOL setup_init(HINSTANCE hinstDLL)
{
  if (0) InitCommonControls();
  ghInstance = hinstDLL;

  WNDCLASSEX wcx;
  // Get system dialog information.
  wcx.cbSize = sizeof(wcx);
  if (0 && !GetClassInfoEx(NULL, MAKEINTRESOURCE(32770), &wcx)) return FALSE;
  wcx.hInstance = hinstDLL;
  wcx.lpszClassName = className;
  if (0 && !RegisterClassEx(&wcx)) return FALSE;

  return TRUE;
}

void setup_fini(void)
{
  UnregisterClass(className, ghInstance);
}

static int get_driver_dll_path(HWND hwndParent, char *buf, size_t len)
{
  HMODULE hm = NULL;

  if (GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
          (LPCSTR) &ConfigDSN, &hm) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleHandle failed, error = %d\n", ret);
      return -1;
  }
  if (GetModuleFileName(hm, buf, (DWORD)len) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleFileName failed, error = %d\n", ret);
      return -1;
  }
  return 0;
}

static const char *gDriver = NULL;
static const char *gAttributes = NULL;

static INT_PTR CALLBACK SetupDlg(HWND hDlg, UINT message, WPARAM wParam, LPARAM lParam)
{
  LPCSTR lpszDriver = gDriver;
  LPCSTR lpszAttributes = gAttributes;

  UNREFERENCED_PARAMETER(lParam);
  switch (message) {
  case WM_INITDIALOG:
    if (lpszAttributes) {
      const char *p = lpszAttributes;
      char k[4096], v[4096];
      while (p && *p) {
        get_kv(p, k, sizeof(k), v, sizeof(v));
        if (tod_strcasecmp(k, "DSN") == 0) {
          if (v[0]) {
            SetDlgItemText(hDlg, IDC_EDT_DSN, (LPCSTR)v);
            k[0] = '\0';
            SQLGetPrivateProfileString(v, "UNSIGNED_PROMOTION", "", k, sizeof(k), "Odbc.ini");
            CheckDlgButton(hDlg, IDC_CHK_UNSIGNED_PROMOTION, !!atoi(k));
            SQLGetPrivateProfileString(v, "TIMESTAMP_AS_IS", "", k, sizeof(k), "Odbc.ini");
            CheckDlgButton(hDlg, IDC_CHK_TIMESTAMP_AS_IS, !!atoi(k));
            SQLGetPrivateProfileString(v, "DB", "", k, sizeof(k), "Odbc.ini");
            SetDlgItemText(hDlg, IDC_EDT_DB, (LPCSTR)k);
            break;
          }
        }
        p += strlen(p) + 1;
      }
    }
    return (INT_PTR)TRUE;

  case WM_COMMAND:
    if (LOWORD(wParam) == IDCANCEL) {
      EndDialog(hDlg, LOWORD(wParam));
      return (INT_PTR)TRUE;
    }

    if (LOWORD(wParam) == IDOK) {
      char driver_dll[MAX_PATH + 1];
      int r = get_driver_dll_path(hDlg, driver_dll, sizeof(driver_dll));
      if (r) {
        MessageBox(hDlg, "get_driver_dll_path failed", "Warning!", MB_OK|MB_ICONEXCLAMATION);
        return (INT_PTR)FALSE;
      }

      char dsn[4096];
      dsn[0] = '\0';
      UINT nr = GetDlgItemText(hDlg, IDC_EDT_DSN, (LPSTR)dsn, sizeof(dsn));
      const char *dsn_start, *dsn_end;
      trim_string(dsn, nr, &dsn_start, &dsn_end);
      if (dsn_start == dsn_end) {
        MessageBox(hDlg, "DSN must be specified", "Warning", IDOK);
        return (INT_PTR)FALSE;
      }
      *(char*)dsn_end = '\0';

      UINT unsigned_promotion = !!IsDlgButtonChecked(hDlg, IDC_CHK_UNSIGNED_PROMOTION);
      UINT timestamp_as_is = !!IsDlgButtonChecked(hDlg, IDC_CHK_TIMESTAMP_AS_IS);
      char db[4096];
      db[0] = '\0';
      nr = GetDlgItemText(hDlg, IDC_EDT_DB, (LPSTR)db, sizeof(db));
      const char *db_start, *db_end;
      trim_string(db, nr, &db_start, &db_end);
      *(char*)db_end = '\0';

      BOOL ok = TRUE;

      if (ok) ok = SQLWritePrivateProfileString("ODBC Data Sources", dsn_start, lpszDriver, "Odbc.ini");
      if (ok) ok = SQLWritePrivateProfileString(dsn_start, "Driver", driver_dll, "Odbc.ini");
      if (ok) ok = SQLWritePrivateProfileString(dsn_start, "UNSIGNED_PROMOTION", unsigned_promotion ? "1" : "0", "Odbc.ini");
      if (ok) ok = SQLWritePrivateProfileString(dsn_start, "TIMESTAMP_AS_IS", timestamp_as_is ? "1" : "0", "Odbc.ini");
      if (ok) ok = SQLWritePrivateProfileString(dsn_start, "DB", db_start[0] ? db_start : "", "Odbc.ini");
      if (ok) ok = SQLWritePrivateProfileString(dsn_start, "DB", db_start[0] ? db_start : NULL, "Odbc.ini");
      if (ok) {
        EndDialog(hDlg, LOWORD(wParam));
        return (INT_PTR)TRUE;
      }
      return (INT_PTR)FALSE;
    }
    break;
  }
  return (INT_PTR)FALSE;
}

static BOOL doDSNAdd(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  if (hwndParent) {
    gDriver = lpszDriver;
    gAttributes = lpszAttributes;
    INT_PTR r = DialogBox(ghInstance, MAKEINTRESOURCE(IDD_SETUP), hwndParent, SetupDlg);
    if (r != IDOK) return FALSE;

    return TRUE;
  }

  char driver_dll[MAX_PATH + 1];
  driver_dll[0] = '\0';
  int r = get_driver_dll_path(hwndParent, driver_dll, sizeof(driver_dll));
  if (r) return FALSE;

  const char *p = lpszAttributes;
  char dsn[4096] = "TAOS_ODBC_DSN";
  dsn[0] = '\0';
  char k[4096], v[4096];
  while (p && *p) {
    get_kv(p, k, sizeof(k), v, sizeof(v));
    if (tod_strcasecmp(k, "DSN") == 0) {
      if (v[0]) {
        strcpy(dsn, v);
        break;
      }
    }
    p += strlen(p) + 1;
  }

  BOOL ok = TRUE;
  // if (ok) ok = SQLWritePrivateProfileString("ODBC Data Sources", dsn, lpszDriver, "Odbc.ini");
  // if (ok) ok = SQLWritePrivateProfileString(dsn, "Driver", driver_dll, "Odbc.ini");
  if (ok) ok = SQLWriteDSNToIni(dsn, lpszDriver);

  p = lpszAttributes;
  while (ok && p && *p) {
    get_kv(p, k, sizeof(k), v, sizeof(v));
    if (tod_strcasecmp(k, "DSN") && tod_strcasecmp(k, "DRIVER")) {
      if (ok) ok = SQLWritePrivateProfileString(dsn, k, v, "Odbc.ini");
    }
    p += strlen(p) + 1;
  }
  return ok;
}

static BOOL doDSNConfig(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  if (hwndParent) {
    gDriver = lpszDriver;
    gAttributes = lpszAttributes;
    INT_PTR r = DialogBox(ghInstance, MAKEINTRESOURCE(IDD_SETUP), hwndParent, SetupDlg);
    if (r != IDOK) return FALSE;

    return TRUE;
  }

  if (hwndParent) {
    MessageBox(hwndParent, "Please use odbcconf to config DSN for TAOS ODBC Driver", "Warning!", MB_OK|MB_ICONEXCLAMATION);
    return FALSE;
  }

  const char *p = lpszAttributes;
  while (p && *p) {
    p += strlen(p) + 1;
  }
  return FALSE;
}

static BOOL doDSNRemove(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  const char *p = lpszAttributes;
  char k[4096], v[4096];
  while (p && *p) {
    get_kv(p, k, sizeof(k), v, sizeof(v));
    if (tod_strcasecmp(k, "DSN") == 0) {
      if (v[0]) {
        return SQLRemoveDSNFromIni(v);
      }
    }
    p += strlen(p) + 1;
  }

  return FALSE;
}

static BOOL doConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = FALSE;
  const char *sReq = NULL;
  switch(fRequest) {
    case ODBC_ADD_DSN:    sReq = "ODBC_ADD_DSN";      break;
    case ODBC_CONFIG_DSN: sReq = "ODBC_CONFIG_DSN";   break;
    case ODBC_REMOVE_DSN: sReq = "ODBC_REMOVE_DSN";   break;
    default:              sReq = "UNKNOWN";           break;
  }
  switch(fRequest) {
    case ODBC_ADD_DSN: {
      r = doDSNAdd(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_CONFIG_DSN: {
      r = doDSNConfig(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_REMOVE_DSN: {
      r = doDSNRemove(hwndParent, lpszDriver, lpszAttributes);
    } break;
    default: {
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
      r = FALSE;
    } break;
  }
  return r;
}

BOOL INSTAPI ConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r;
  r = doConfigDSN(hwndParent, fRequest, lpszDriver, lpszAttributes);
  return r;
}

BOOL INSTAPI ConfigTranslator(HWND hwndParent, DWORD *pvOption)
{
  (void)hwndParent;
  (void)pvOption;
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}

BOOL INSTAPI ConfigDriver(HWND hwndParent, WORD fRequest, LPCSTR lpszDriver, LPCSTR lpszArgs,
                          LPSTR lpszMsg, WORD cbMsgMax, WORD *pcbMsgOut)
{
  (void)hwndParent;
  (void)fRequest;
  (void)lpszDriver;
  (void)lpszArgs;
  (void)lpszMsg;
  (void)cbMsgMax;
  (void)pcbMsgOut;
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}
