package main

/*
* Filename: datacheck.go
* Date: 2015-09-01
* Auther: yxb
* Changer Logs:
*
 */

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/larspensjo/config"
	"log"
	"strconv"
	"strings"
)

var configFile = flag.String("configfile", "conf_datacheck.ini", "datacheck confige file.")
var (
	dataSourceNameFrom string
	driverNameTo       string
	dataSourceNameTo   string
	dbconnFrom         *sql.DB
	dbconnTo           *sql.DB
	c                  chan int
	dsnfromtbinfo      sourceinfo
	dsntotbinfo        targeinfo
	sharexinfo         shareinfo
)

type sourceinfo struct {
	table       string
	checkcolums string
	primarykey  string
	offset      int64
}

type targeinfo struct {
	tablepre    string
	checkcolums string
}

type shareinfo struct {
	isshare    string
	sharecolum int
	rule       string
	shareval   int64
}

func init() {
	dsnfromconf := dbconfParse("dataSourceNameFrom")
	dsntoconf := dbconfParse("dataSourceNameTo")
	shareconf := dbconfParse("shareRule")

	dsnfromtbinfo.table = dsnfromconf["table"]
	dsnfromtbinfo.checkcolums = dsnfromconf["checkcolums"]
	dsnfromtbinfo.primarykey = dsnfromconf["primarykey"]
	dsnfromtbinfo.offset, _ = strconv.ParseInt(dsnfromconf["offset"], 10, 64)

	dsntotbinfo.tablepre = dsntoconf["tablepre"]
	dsntotbinfo.checkcolums = dsntoconf["checkcolums"]

	sharexinfo.isshare = shareconf["isshare"]
	sharexinfo.sharecolum, _ = strconv.Atoi(shareconf["sharecolum"])
	sharexinfo.rule = shareconf["rule"]
	sharexinfo.shareval, _ = strconv.ParseInt(shareconf["shareval"], 10, 64)

	// dsn from information
	driverFrom := dsnfromconf["drivertype"]
	dsnFrom := setDsn(dsnfromconf)

	// dsn to information
	driverTo := dsntoconf["drivertype"]
	dsnTo := setDsn(dsntoconf)

	dbconnFrom, _ = sql.Open(driverFrom, dsnFrom)
	dbconnTo, _ = sql.Open(driverTo, dsnTo)
	dbconnFrom.SetMaxOpenConns(16)
	dbconnFrom.SetMaxIdleConns(8)
	dbconnFrom.Ping()

	dbconnTo.SetMaxOpenConns(16)
	dbconnTo.SetMaxIdleConns(8)
	dbconnTo.Ping()
}

func main() {
	item := make(chan []string, 1000)
	c = make(chan int)
	go Producer(item)

	go Consumer(item)
	go Consumer(item)
	<-c
	<-c

	fmt.Println("---------- Check Complete! -----------")
}

func MinMaxPk() (maxminpkmap map[string]int64) {
	sqlMinMaxPk := "SELECT MIN(" + dsnfromtbinfo.primarykey + ") AS minpk,"
	sqlMinMaxPk += "MAX(" + dsnfromtbinfo.primarykey + ") AS maxpk"
	sqlMinMaxPk += " FROM " + dsnfromtbinfo.table

	maxminpkmap = make(map[string]int64)
	rows, err := dbconnFrom.Query(sqlMinMaxPk)
	if err != nil {
		panic(err.Error())
	}
	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}

		var value int64
		for i, col := range values {
			if col == nil {
				value = 0
			} else {
				value, _ = strconv.ParseInt(string(col), 10, 64)
			}
			maxminpkmap[columns[i]] = value
		}
	}

	if err = rows.Err(); err != nil {
		panic(err.Error())
	}

	return
}

func SourceValues(minpk int64, maxpk int64, item chan []string) {
	sqlData1 := "SELECT " + dsnfromtbinfo.checkcolums + " FROM " + dsnfromtbinfo.table
	sqlData1 += " WHERE " + dsnfromtbinfo.primarykey + ">=? AND " + dsnfromtbinfo.primarykey
	sqlData1 += "<?"

	sqlData2 := "SELECT " + dsnfromtbinfo.checkcolums + " FROM " + dsnfromtbinfo.table
	sqlData2 += " WHERE " + dsnfromtbinfo.primarykey + ">=? AND " + dsnfromtbinfo.primarykey
	sqlData2 += "<=?"

	for i := minpk; i <= maxpk; i = i + dsnfromtbinfo.offset {
		if (i + dsnfromtbinfo.offset) < maxpk {
			rows, err := dbconnFrom.Query(sqlData1, i, (i + dsnfromtbinfo.offset))
			if err != nil {
				panic(err.Error())
			}
			columns, err := rows.Columns()
			if err != nil {
				panic(err.Error())
			}

			values := make([]sql.RawBytes, len(columns))
			scanArgs := make([]interface{}, len(values))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			for rows.Next() {
				// get RawBytes from data
				err = rows.Scan(scanArgs...)
				if err != nil {
					panic(err.Error())
				}

				var value string
				//var columnvalues = make(map[string]string)
				var columnarr []string
				for _, col := range values {
					if col == nil {
						value = "NULL"
					} else {
						value = string(col)
					}
					//columnvalues[columns[i]] = value
					columnarr = append(columnarr, value)
				}
				//item <- columnvalues
				item <- columnarr
			}
		} else {
			rows, err := dbconnFrom.Query(sqlData2, i, maxpk)
			if err != nil {
				panic(err.Error())
			}
			columns, err := rows.Columns()
			if err != nil {
				panic(err.Error())
			}

			values := make([]sql.RawBytes, len(columns))
			scanArgs := make([]interface{}, len(values))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			for rows.Next() {
				// get RawBytes from data
				err = rows.Scan(scanArgs...)
				if err != nil {
					panic(err.Error())
				}

				var value string
				//var columnvalues = make(map[string]string)
				var columnarr []string
				for _, col := range values {
					if col == nil {
						value = "NULL"
					} else {
						value = string(col)
					}
					//columnvalues[columns[i]] = value
					columnarr = append(columnarr, value)
				}
				//item <- columnvalues
				item <- columnarr
			}
			//time.Sleep(1 * time.Second)
			close(item)
		}
	}
}

func SplitStr(colums string) (str []string) {
	str = strings.Split(colums, ",")
	return
}

func GetSourceNum(val []string) (num int) {
	colums := SplitStr(dsnfromtbinfo.checkcolums)
	var wherestr string
	for i := 0; i < len(val); i++ {
		wherestr += " AND "
		wherestr += colums[i]
		wherestr += "='"
		wherestr += val[i]
		wherestr += "'"
	}
	sql := "SELECT COUNT(1) AS num FROM "
	sql += dsnfromtbinfo.table
	sql += " WHERE 1=1 "
	sql += wherestr
	row, err := dbconnFrom.Query(sql)
	if err != nil {
		panic(err.Error())
	}
	defer row.Close()

	for row.Next() {
		err := row.Scan(&num)
		if err != nil {
			panic(err.Error())
		}
	}
	return
}

func CheckDiff(tableName string, val []string) {
	colums := SplitStr(dsntotbinfo.checkcolums)
	var wherestr string
	for i := 0; i < len(val); i++ {
		wherestr += " AND "
		wherestr += colums[i]
		wherestr += "=?"
	}
	sql := "SELECT COUNT(1) AS num FROM "
	sql += tableName
	sql += " WHERE 1=1  "
	sql += wherestr
	var args []interface{}
	for _, v := range val {
		args = append(args, v)
	}
	row, err := dbconnTo.Query(sql, args...)
	if err != nil {
		panic(err.Error())
	}
	defer row.Close()

	var numT int
	for row.Next() {
		err := row.Scan(&numT)
		if err != nil {
			panic(err.Error())
		}
	}
	/*
	*  numT == 1 , 源表和目标表数据一致
	*  numT == 0 , 源表有数据，目标表没有数据
	*  numT  > 1 , 这种情况比较复杂：说明目标表满足条件的值不知一行，
	*             需要反查源表的数据行数，如果两个表的满足该条件的数据行数一样，
	*             则数据一致，否则就不一致
	 */
	if numT == 0 {
		fmt.Println("Source Data:", val, "\tTarget Data: NULL")
	} else if numT > 1 {
		numS := GetSourceNum(val)
		if numT != numS {
			fmt.Println("Source Data Numbers:", numS, "Target Data Numbers:", numT)
			fmt.Println("Data:", val)
		}
	}
}

func Producer(item chan []string) {
	minmaxpkMap := MinMaxPk()
	SourceValues(minmaxpkMap["minpk"], minmaxpkMap["maxpk"], item)

}

func Consumer(item chan []string) {
	for s := range item {
		sharecolvalue, _ := strconv.ParseInt(s[sharexinfo.sharecolum], 10, 64)
		targerTableName := parseShareRule(sharecolvalue)
		CheckDiff(targerTableName, s)
	}
	c <- 1
}

func parseShareRule(sharecolvalue int64) (targerTableName string) {
	/*
	* Share algorithms
	* INTRANGE: Share by int range, eg: 0-99 in table0, 100-199 in table1 ...
	*           100-199 in table1 ...
	* MODULE:   %
	 */

	/*
	* first decide whether is shared.
	* if shareRule.isshare == 'NO' means not shared, just same table.
	* so sharecolum,rule,shareval are all NULL
	* if shareRule.isshare == 'YES' mean table shared.
	* so shareRule.rule give the share algorithms, shareRule.sharecolum give
	* the share colum, and shareRule.shareval give the share value.
	* so you can use sharecolum,rule,shareval calculate the table name.
	 */

	//isshare := dbconfParse("shareRule")["isshare"]
	isshare := sharexinfo.isshare
	if isshare == "NO" {
		targerTableName = dsntotbinfo.tablepre
	} else if isshare == "YES" {
		switch sharexinfo.rule {
		case "INTRANGE":
			targerTableName = dsntotbinfo.tablepre
			targerTableName += shareIntRange(sharecolvalue)
		case "MODULE":
			fmt.Println("MODULE")
		}
	}

	return
}

func shareIntRange(shareid int64) (tableid string) {
	tableid = strconv.FormatInt(shareid/sharexinfo.shareval, 10)
	return
}

func setDsn(conf map[string]string) (dsn string) {
	dsn = conf["username"] + ":"
	dsn += conf["password"] + "@tcp("
	dsn += conf["ip"] + ":"
	dsn += conf["port"] + ")/"
	dsn += conf["database"] + "?timeout=4s&charset="
	dsn += conf["encoding"]

	return
}

func dbconfParse(key string) (conf map[string]string) {
	conf = make(map[string]string)
	flag.Parse()

	//set config file std
	cfg, err := config.ReadDefault(*configFile)
	if err != nil {
		log.Fatalf("Fail to find", *configFile)
	}

	if cfg.HasSection(key) {
		section, err := cfg.SectionOptions(key)
		if err == nil {
			for _, v := range section {
				options, err := cfg.String(key, v)
				if err == nil {
					conf[v] = options
				}
			}
		}
	}
	return
}
