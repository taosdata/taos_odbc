;; MIT License
;;
;; Copyright (c) 2022-2023 freemine <freemine@yeah.net>
;;
;; Permission is hereby granted, free of charge, to any person obtaining a copy
;; of this software and associated documentation files (the "Software"), to deal
;; in the Software without restriction, including without limitation the rights
;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;; copies of the Software, and to permit persons to whom the Software is
;; furnished to do so, subject to the following conditions:
;;
;; The above copyright notice and this permission notice shall be included in
;; all copies or substantial portions of the Software.
;;
;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
;; SOFTWARE.

(write-line "Hello, Common Lisp!")

(asdf:oos 'asdf:load-op :plain-odbc)
(use-package :plain-odbc)

(defvar *con*)
(defvar *stm*)

(setf *con* (connect-generic :dsn "TAOS_ODBC_DSN" :uid "root" :pwd "taosdata" :database "bar"))
(assert (eq 0 (exec-update *con* "drop table if exists common_lisp")))
(assert (eq 0 (exec-update *con* "create table if not exists common_lisp(ts timestamp, name varchar(20))")))
(setf *stm* (prepare-statement *con* "insert into common_lisp(ts, name) values(?,?)" '(:string :in) '(:unicode-string :in)))
;; (assert (eq 1 (exec-prepared-update *stm* "2023-05-30 12:13:14.567" "你好hello中国")))

(defvar *params*)
(defvar *rows_expected*)
(setf *params*         '("2023-05-30 12:13:14.567" "你好hello中国"))
(setf *rows_expected* '(("2023-05-30 12:13:14.567" "你好hello中国")))
(assert (eq 1 (multiple-value-bind (ts name) (values-list *params*) (exec-prepared-update *stm* ts name))))

;; (assert (equal '(("2023-05-30 12:13:14.567" "你好hello中国")) (multiple-value-bind (x) (exec-query *con* "select * from common_lisp") x)))
(assert (equal *rows_expected* (exec-query *con* "select * from common_lisp")))

(free-statement *stm*)
(close-connection *con*)

(write-line "==success==")

