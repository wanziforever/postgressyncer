package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
	//"strings"
	"os"
	"github.com/lib/pq"
	"io/ioutil"
)

const (
	TABLE_FIELD_TYPE_STRING = 0
	TABLE_FIELD_TYPE_NUMBER = 1
)

var config Config

var tableDesc = make(map[string]map[string]int)

var warehouseConn *sql.DB
var listener *pq.Listener

type dmlsql interface {
	//init()
	AddColumnAndValue(col string, val string)
	ToSqlString()
	show()
	getSQL() string
}

type InsertStmt struct {
	Stmtstr string
	Tablename string
	Fields []string
	Values []string
}

func (s *InsertStmt) AddColumnAndValue(col string, val string) {
	fmt.Println("insertStmt enter addcolum and value")
	s.Fields = append(s.Fields, col)
	s.Values = append(s.Values, val)
}

func (s *InsertStmt) show() {
	fmt.Println(s.Fields)
	fmt.Println(s.Values)
}

func (s *InsertStmt) ToSqlString() {
	fmt.Println("going to generate sql string: ", len(s.Fields), len(s.Values))
	s.Stmtstr = "insert into " + s.Tablename + " "
	fieldNum := len(s.Fields)
	fieldstr := " ("
	valstr := " values ("
	columnDesc := tableDesc[s.Tablename]

	for i := 0; i < fieldNum; i++ {
		if i != 0 {
			fieldstr = fieldstr + ", "
			valstr = valstr + ", "
		}

		fieldstr = fieldstr + s.Fields[i]

		if _, ok := columnDesc[s.Fields[i]]; ok {
			if columnDesc[s.Fields[i]] == TABLE_FIELD_TYPE_STRING {
				valstr = valstr + "'" + s.Values[i] + "'"
			} else {
				valstr = valstr + s.Values[i]
			}
		} else {
			valstr = valstr + s.Values[i]
		}
	}

	fieldstr = fieldstr + ") "
	valstr = valstr + " )"

	s.Stmtstr = s.Stmtstr + fieldstr + valstr + ";"
	fmt.Println(s.Stmtstr)
}

func (s *InsertStmt) getSQL() string {
	return s.Stmtstr
}


type UpdateStmt struct {
	Stmtstr string
	Tablename string
	Fields []string
	Values []string
}

func (s *UpdateStmt) AddColumnAndValue(col string, val string) {
	s.Fields = append(s.Fields, col)
	s.Values = append(s.Values, val)
}

func (s *UpdateStmt) ToSqlString() {

}


func (s *UpdateStmt) show() {
	fmt.Println(s.Fields)
	fmt.Println(s.Values)
}

func (s *UpdateStmt) getSQL() string {
	return s.Stmtstr
}


func transformToSQL(triggerstr string) string {
	var result map[string]interface{}
	json.Unmarshal([]byte(triggerstr), &result)
	table := result["table"]
	action := result["action"]
	data := result["data"].(map[string]interface{})
	fmt.Println("table: ", table)
	fmt.Println("action: ", action)

	fieldsdesc := tableDesc[table.(string)]
	var stmt dmlsql
	if action == "INSERT" {
		stmt = &InsertStmt{Tablename: table.(string)}
	} else if action == "UPDATE" {
		stmt = &UpdateStmt{Tablename: table.(string)}
	}
	
	
	//fmt.Println("%+v", data)
	for key, value := range data {
		if _, ok := fieldsdesc[key]; !ok {
			continue
		}
		
		if value != nil {
			fmt.Println(key, value.(string))
			stmt.AddColumnAndValue(key, value.(string))
		}	
	}

	stmt.ToSqlString()
	return stmt.getSQL()
}

func waitForNotification(l *pq.Listener) {
	for {
		select  {
		case n := <-l.Notify:
			fmt.Println("Received data from channel [", n.Channel, "]")
			var prettyJSON bytes.Buffer
			notifyDMLToWarehouse(transformToSQL(n.Extra))

			
			err := json.Indent(&prettyJSON, []byte(n.Extra), "", "\t")
			if err != nil {
				fmt.Println("Error processing JSON: ", err)
				return
			}
			//fmt.Println(string(prettyJSON.Bytes()))
			return
		case <-time.After(90 * time.Second):
			fmt.Println("Received no events for 90 seconds checking connection")
			go func() {
				l.Ping()
			} ()
			return
		}
	}
}


func examineTableMaps() {
	for table, fields := range tableDesc {
		fmt.Println("table: ", table)
		for field, typ := range fields {
			fmt.Println("    field: ",field,  "type: ", typ)
		}
	}
}

func setupTableMap(config Config) {
	for _, table := range config.TableMaps {
		if _, ok := tableDesc[table.Tablename]; !ok {
			tableDesc[table.Tablename] = make(map[string]int)
		}

		sourceFields := table.Fields
		targetFields := tableDesc[table.Tablename]
		
		for _, field := range sourceFields {
			if field.Type == "string" {
				targetFields[field.Name] = TABLE_FIELD_TYPE_STRING
			} else if field.Type == "number" {
				targetFields[field.Name] = TABLE_FIELD_TYPE_NUMBER
			}
		}
	}
}

func loadConfig() {
	jsonFile, err := os.Open("config.json")
	if err != nil {
		panic(err)
	}
	bytevalue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(bytevalue, &config)
}


func notifyDMLToWarehouse(sql string) {
	fmt.Println("notifyDNLToWarehouse: ", sql)
	_, err := warehouseConn.Exec(sql)
	if err != nil {
		panic(err)
	}
}

func launchWarehouseConnection() *sql.DB {
	w := config.WarehouseServer
	var conninfo = fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		w.Host, w.Port, w.Dbname, w.Username, w.Password)
	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		panic(err)
	}
	return db
}


func launchSourceListener() {
	//var conninfo string = "dbname=postgres user=postgres port=5436 password=ff sslmode=disable"
	w := config.SourcePgServer
	var conninfo = fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		w.Host, w.Port, w.Dbname, w.Username, w.Password)
	_, err := sql.Open("postgres", conninfo)
	if err != nil {
		panic(err)
	}

	fmt.Println("finish open the connection of postgres");

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	listener = pq.NewListener(conninfo, 10*time.Second, time.Minute, reportProblem)
	err = listener.Listen("events")
	if err != nil {
		panic(err)
	}
}

func main() {
	loadConfig()
	fmt.Println("%+v\n", config)
	setupTableMap(config)
	examineTableMaps()

	warehouseConn = launchWarehouseConnection()

	launchSourceListener()
	
	fmt.Println("Start monitoring PostgreSQL...")
	for {
		waitForNotification(listener)
	}
}
