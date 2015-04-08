package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"encoding/json"
	"flag"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dsn string
	port string
	path string
	ignoreMaster bool
	delay int
)
	
func main() {
	flag.StringVar(&dsn, "d", "root:@/mysql", "DSN String")
	flag.StringVar(&port, "p", "23306", "Port number")
	flag.StringVar(&path, "t", "/", "Path for monitor URL")
	flag.BoolVar(&ignoreMaster, "m", true, "Ignoring that the server is not a slave")
	flag.IntVar(&delay, "l", 60, "Failed when the server is behind master over the seconds")

	http.HandleFunc(path, handler)
	http.ListenAndServe(":"+port, nil)
}

func handler(w http.ResponseWriter, r *http.Request) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		serverError(w, errors.New("Cannot connect to MySQL"))
		return
	}
	defer db.Close()

	rows, err := db.Query("SHOW SLAVE STATUS")
	if err != nil {
		serverError(w, err)
		return
	}
	if !rows.Next() {
		if ignoreMaster {
			serverOk(w, nil)
		} else {
			serverError(w, errors.New("Cannot get slave status."))
		}
		return
	}

	columns, _ := rows.Columns()
	values := make([]interface{}, len(columns))
	for i, _ := range values {
		var v sql.RawBytes
		values[i] = &v
	}
	err = rows.Scan(values...)
	if err != nil {
		serverError(w, err)
		return
	}

	slaveStatus := make(map[string]interface{})
	for i, name := range columns {
		bp := values[i].(*sql.RawBytes)
		vs := string(*bp)
		vi, err := strconv.ParseInt(vs, 10, 64)
		if err != nil {
			// エラー発生＝整数ではない
			slaveStatus[name] = vs
		} else {
			// エラー発生せず＝整数
			slaveStatus[name] = vi
		}
	}
	if slaveStatus["Slave_IO_Running"] != "Yes" {
		serverError(w, errors.New("Slave IO thread is not running."))
		return
	}
	if slaveStatus["Slave_SQL_Running"] != "Yes" {
		serverError(w, errors.New("Slave SQL thread is not running."))
		return
	}

	serverOk(w, slaveStatus)
}

func serverOk(w http.ResponseWriter, slaveStatus map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	log.Printf("%s", "Ok")
	encoder := json.NewEncoder(w)
	encoder.Encode(slaveStatus)
}

func serverError(w http.ResponseWriter, err error) {
	log.Printf("%s", err)
	code := http.StatusInternalServerError
	http.Error(w, fmt.Sprintf("%s", err), code)
}

