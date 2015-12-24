package main

/*
* useage: ./sync_full -f xxx.ini
 */
import "fmt"
import "flag"
import "os"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "github.com/larspensjo/config"
import "strconv"
import "strings"

type sourceinfo struct {
	table       string
	checkcolums string
	primarykey  string
	tablerange  string
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
	confile            string
	masterFile         string
	masterPosition     int
)

/*
*  init() 读取配置文件，然后初始化一些重要的数据结构
*  建立到数据库的长连接
 */
func init() {
	confile = GetIniFile()
	dsnfromconf := ConfParse("dataSourceNameFrom")
	dsntoconf := ConfParse("dataSourceNameTo")
	shareconf := ConfParse("shareRule")

	dsnfromtbinfo.table = dsnfromconf["table"]
	dsnfromtbinfo.checkcolums = dsnfromconf["checkcolums"]
	dsnfromtbinfo.primarykey = dsnfromconf["primarykey"]
	dsnfromtbinfo.offset, _ = strconv.ParseInt(dsnfromconf["offset"], 10, 64)
	dsnfromtbinfo.tablerange = dsnfromconf["tablerange"]

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
	<-c
	fmt.Println("-- Full Data Sync Complete!")
}

/*
* Producer() 生产者，从源库拉取数据，然后放到队列中，供消费者消费。
 */
func Producer(item chan []string) {
	// 1. Flush Tables
	_, err := dbconnFrom.Exec("FLUSH /*!40101 LOCAL */ TABLES")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("-- Flush Tables OK.")

	// 2. Lock tables with read lock
	_, err = dbconnFrom.Exec("FLUSH TABLES WITH READ LOCK")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("-- FLUSH TABLES WITH READ LOCK OK.")

	// 3. Set the transaction isolation level to be repeatable read
	_, err = dbconnFrom.Exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("-- Set the transaction isolation level OK.")

	// 4. Start Transaction
	tx, err := dbconnFrom.Begin()
	if err != nil {
		panic(err.Error())
	}

	// 5. Get The Master Status
	masterInfo, err := tx.Query("SHOW MASTER STATUS")
	if err != nil {
		panic(err.Error())
	}
	var file string
	var position int
	var binlogDoDb string
	var binlogIgnoreDb string
	var exectedGtidSet string
	for masterInfo.Next() {
		err := masterInfo.Scan(&file, &position, &binlogDoDb, &binlogIgnoreDb, &exectedGtidSet)
		if err != nil {
			panic(err.Error())
		}
		masterFile = file
		masterPosition = position
	}
	fmt.Println("-- Get the Master Status Info OK.")
	fmt.Println("-- Binlogfile:", masterFile, "\tPosition:", masterPosition)

	// 6. unlock tables
	_, err = tx.Query("UNLOCK TABLES")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("-- Unlock tables OK.")

	// 7. 获取数据

	if strings.EqualFold(dsnfromtbinfo.tablerange, "NONE") {
		minmaxPKMap := MinMaxPK(tx, dsnfromtbinfo.table)
		SourceValues(minmaxPKMap["minpk"], minmaxPKMap["maxpk"], item, tx, dsnfromtbinfo.table)

		fmt.Println("-- Get Data From Source Table Complete ---- ", dsnfromtbinfo.table)
		fmt.Println("-- Commit the transaction.")
		tx.Commit()
		close(item)
	} else {
		tablerange := ParseTablerange()
		for i := 0; i < len(tablerange); i++ {
			fmt.Println("-- Start Get Data From Table: ", dsnfromtbinfo.table+tablerange[i])
			minmaxPKMap := MinMaxPK(tx, dsnfromtbinfo.table+tablerange[i])
			SourceValues(minmaxPKMap["minpk"], minmaxPKMap["maxpk"], item, tx, dsnfromtbinfo.table+tablerange[i])

			fmt.Println("-- Get Data From Source Table Complete ---- ", dsnfromtbinfo.table+tablerange[i])
		}
		tx.Commit()
		close(item)
		fmt.Println("-- Commit the transaction.")
	}
}

func ParseTablerange() (tbrange []string) {
	tbrange = strings.Split(dsnfromtbinfo.tablerange, "-")
	return
}

func Consumer(item chan []string) {
	if sharexinfo.isshare == "YES" && sharexinfo.rule == "MD5" {
		for s := range item {
			strmd5 := s[sharexinfo.sharecolum][0:8]
			sharecolvalue, _ := strconv.ParseInt(strmd5, 16, 64)
			targerTableName := parseShareRule(sharecolvalue)

			SyncTo(targerTableName, s)
		}
	} else {
		for s := range item {
			sharecolvalue, _ := strconv.ParseInt(s[sharexinfo.sharecolum], 10, 64)
			targerTableName := parseShareRule(sharecolvalue)

			// 将数据写入到目标库表中
			SyncTo(targerTableName, s)
		}
	}
	c <- 1
}

func SyncTo(tableName string, val []string) {
	sql := "INSERT INTO " + tableName
	sql += " (" + dsntotbinfo.checkcolums + ") "
	sql += " VALUES("
	for i := 0; i < len(val); i++ {
		if i < (len(val) - 1) {
			sql += "?,"
		} else {
			sql += "?)"
		}
	}
	/* 注意这里有个slice和[]interface的转化*/
	var args []interface{}
	for _, v := range val {
		args = append(args, v)
	}
	_, err := dbconnTo.Exec(sql, args...)
	if err != nil {
		panic(err.Error())
	}
}

func SplitStr(colums string) (str []string) {
	str = strings.Split(colums, ",")
	return
}

/*
* 分表规则解析:
* 配置文件中【shareRule】里面的键值对表明了分表的规则信息
* isshare: YES 表示分表;  NO 表示不分表(可以用于表迁移);
* sharecolum: 表示分区字段是[dataSourceNameFrom].checkcolums
*             中字段的第几个，序列从0开始。0表示按照第一个字段，
*             1表示按照第二个字段，以此类推。
* rule: 表示分表算法.
*       eg: INTRANGE: 表示按照int类型的范围分区；
*           MODULE:   表示按照取模的算法
*           MD5:      表示取MD5值的前8位，然后由16进制转化为10进制，然后取模
* shareval: 表示分表的值，如果是range，则按照这个值取商；
*                         如果是MODULE，则按照这个值取模；
 */
func parseShareRule(sharecolvalue int64) (targerTableName string) {
	isshare := sharexinfo.isshare
	if isshare == "NO" {
		targerTableName = dsntotbinfo.tablepre
	} else if isshare == "YES" {
		switch sharexinfo.rule {
		case "INTRANGE":
			targerTableName = dsntotbinfo.tablepre
			targerTableName += shareIntRange(sharecolvalue)
		case "MODULE":
			targerTableName = dsntotbinfo.tablepre
			targerTableName += shareModule(sharecolvalue)
		case "MD5":
			targerTableName = dsntotbinfo.tablepre
			targerTableName += shareModule(sharecolvalue)
		default:
			panic("Unknow Share Rule. Please Check!")
		}
	}

	return
}

// 取模分表规则
func shareModule(shareid int64) (tableid string) {
	tableid = strconv.FormatInt(shareid%sharexinfo.shareval, 10)
	return
}

// 按照int的range分表规则
func shareIntRange(shareid int64) (tableid string) {
	tableid = strconv.FormatInt(shareid/sharexinfo.shareval, 10)
	return
}

func SourceValues(minpk int64, maxpk int64, item chan []string, tx *sql.Tx, tableName string) {
	sqlData1 := "SELECT " + dsnfromtbinfo.checkcolums + " FROM " + tableName
	sqlData1 += " WHERE " + dsnfromtbinfo.primarykey + ">=? AND " + dsnfromtbinfo.primarykey
	sqlData1 += "<?"

	sqlData2 := "SELECT " + dsnfromtbinfo.checkcolums + " FROM " + tableName
	sqlData2 += " WHERE " + dsnfromtbinfo.primarykey + ">=? AND " + dsnfromtbinfo.primarykey
	sqlData2 += "<=?"

	for i := minpk; i <= maxpk; i = i + dsnfromtbinfo.offset {
		if (i + dsnfromtbinfo.offset) < maxpk {
			rows, err := tx.Query(sqlData1, i, (i + dsnfromtbinfo.offset))
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
				err = rows.Scan(scanArgs...)
				if err != nil {
					panic(err.Error())
				}

				var value string
				var columnarr []string
				for _, col := range values {
					if col == nil {
						value = "NULL"
					} else {
						value = string(col)
					}
					columnarr = append(columnarr, value)
				}
				item <- columnarr
			}
		} else {
			rows, err := tx.Query(sqlData2, i, maxpk)
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
				err = rows.Scan(scanArgs...)
				if err != nil {
					panic(err.Error())
				}

				var value string
				var columnarr []string
				for _, col := range values {
					if col == nil {
						value = "NULL"
					} else {
						value = string(col)
					}
					columnarr = append(columnarr, value)
				}
				item <- columnarr
			}
			//tx.Commit()
			//close(item)
		}
	}
}

func MinMaxPK(tx *sql.Tx, tableName string) (maxminpkMap map[string]int64) {
	sqlMinMaxPk := "SELECT MIN(" + dsnfromtbinfo.primarykey + ") AS minpk, "
	sqlMinMaxPk += "MAX(" + dsnfromtbinfo.primarykey + ") AS maxpk "
	sqlMinMaxPk += "FROM " + tableName

	maxminpkMap = make(map[string]int64)
	rows, err := tx.Query(sqlMinMaxPk)
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
			maxminpkMap[columns[i]] = value
		}
	}

	if err = rows.Err(); err != nil {
		panic(err.Error())
	}
	return
}

func ConfParse(key string) (conf map[string]string) {
	conf = make(map[string]string)
	flag.Parse()

	//set config file std
	cfg, err := config.ReadDefault(confile)
	if err != nil {
		fmt.Errorf("Fail to find", confile)
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

func GetIniFile() string {
	var confile string = "NULL"
	iniFile := flag.String("f", "iniFile", "The confile file name")
	flag.Parse()

	if _, err := os.Stat(*iniFile); err == nil {
		confile = *iniFile
	} else {
		fmt.Println("The confile", *iniFile, "not exist.")
		os.Exit(7)
	}

	return confile
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
