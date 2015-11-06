package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"strings"
)

func getRedisConf(w http.ResponseWriter, r *http.Request) {
	r.ParseForm() //解析参数，默认是不会解析的
	fmt.Println("path", r.URL.Path)
	for k, v := range r.Form {
		fmt.Println("key:", k)
		fmt.Println("val:", strings.Join(v, ""))
	}
	fmt.Fprintf(w, "Get Redis Configuer Info.\n") //这个写入到w的是输出到客户端的
}

type mysqlContent struct {
	dbrole      string
	ip          string
	port        int
	username    string
	passwd      string
	readweight  int
	writeweight int
	dbstatus    int
}

type mysqlResponse struct {
	status     string
	appName    string
	groupName  string
	dbName     string
	masterNode mysqlContent
	slaveNode  []mysqlContent
}

var db *sql.DB

func init() {
	db, _ = sql.Open("mysql", "yxb:yxb@tcp(127.0.0.1:3306)/config_center?timeout=4s&charset=utf8")
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)
	db.Ping()
}

func (myRes mysqlResponse) getMysqlResponse(appName string, groupName string) interface{} {
	myRes.status = "Error"
	myRes.appName = appName
	myRes.groupName = groupName

	sql := `SELECT 
	            appname,groupname,dbname,dbrole,ip,port,username, passwd, 
	            read_weight, write_weight, dbstatus 
			FROM config_info 
			WHERE appname='`
	sql += appName + "' AND groupname='"
	sql += groupName + "'"

	rows, err := db.Query(sql)
	if err != nil {
		log.Println(err)
		myRes.status = "Error"
		return myRes
	}

	var appname, groupname, dbname, dbrole, ip, username, passwd string
	var port, read_weight, write_weight, dbstatus int
	for rows.Next() {
		err := rows.Scan(&appname, &groupname, &dbname, &dbrole, &ip, &port, &username, &passwd, &read_weight, &write_weight, &dbstatus)
		if err != nil {
			log.Fatal(err)
		}

		// dbname
		if len(dbname) > 0 {
			myRes.dbName = dbname
		} else {
			myRes.status = "Error"
			log.Println("dbname is null")
			return myRes
		}

		if dbrole == "Master" && write_weight > 0 {
			myRes.masterNode.ip = ip
			myRes.masterNode.port = port
			myRes.masterNode.username = username
			myRes.masterNode.passwd = passwd
			myRes.masterNode.readweight = read_weight
			myRes.masterNode.writeweight = write_weight
			myRes.masterNode.dbstatus = dbstatus
			myRes.status = "OK"
		} else if dbrole == "Master" && write_weight == 0 {
			myRes.status = "Error"
			log.Println("AppName:", appname, "GroupName:", groupname, "The Master ", ip, port, dbname, " write_weight is 0. Please check!")
			return myRes
		} else {
			//slave node
			mycent := new(mysqlContent)
			mycent.ip = ip
			mycent.port = port
			mycent.username = username
			mycent.passwd = passwd
			mycent.readweight = read_weight
			mycent.writeweight = write_weight
			mycent.dbstatus = dbstatus

			myRes.slaveNode = append(myRes.slaveNode, *mycent)
			myRes.status = "OK"
		}
	}

	//body, err := json.Marshal(myRes)
	return myRes
}

func getMysqlConf(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	var appName, groupName string
	appName = strings.Join(r.Form["appName"], "")
	groupName = strings.Join(r.Form["groupName"], "")

	myResponse := new(mysqlResponse)
	rs := myResponse.getMysqlResponse(appName, groupName)
	body, _ := json.Marshal(rs)
	log.Println(string(body))

	respose := "xxx"
	fmt.Fprintf(w, respose)
}

func main() {
	http.HandleFunc("/getMysqlConf", getMysqlConf) //设置访问的路由
	http.HandleFunc("/getRedisConf", getRedisConf) //设置访问的路由
	err := http.ListenAndServe(":9090", nil)       //设置监听的端口
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
