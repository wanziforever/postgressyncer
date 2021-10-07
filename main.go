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

type FieldDesc struct {
	Typ int
	IsPrimaryKey bool
}

var config Config

var tableDesc = make(map[string]map[string]*FieldDesc)

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
	fmt.Println("going to generate sql string: ",
		len(s.Fields), len(s.Values))
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
			if columnDesc[s.Fields[i]].Typ == TABLE_FIELD_TYPE_STRING {
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
	keyFields []string // from old
	keyValues []string // from old
}


func (s *UpdateStmt) AddKeyColumAndValue(col string, val string) {
	s.keyFields = append(s.keyFields, col)
	s.keyValues = append(s.keyValues, val)
}

func (s *UpdateStmt) AddColumnAndValue(col string, val string) {
	s.Fields = append(s.Fields, col)
	s.Values = append(s.Values, val)
}


// update statemnt will be converted to a delete and insert statment
func (s *UpdateStmt) ToSqlString() {
	// be careful of the dangous where the generated delete statment has no where
	// condition, it will delete all record, so if no condition found, just report
	// error, and not proceed
	fmt.Println("going to generate update sql string: ",
		len(s.Fields), len(s.Values))
	columnDesc := tableDesc[s.Tablename]
	fieldNum := len(s.Fields)
	
	// begin to generate the delete statment
	delstr := "delete from " + s.Tablename
	wherestr := "where "
	
	var count int = 0
	for i := 0; i < fieldNum; i++ {
		qualstr := ""
		valstr := ""
		if fd, ok := columnDesc[s.Fields[i]]; ok {
			if fd.IsPrimaryKey {
				if fd.Typ == TABLE_FIELD_TYPE_STRING {
					valstr = "'" + s.Values[i] + "'"
				}	else {
					valstr = s.Values[i]
				}
				qualstr = fmt.Sprintf("%s = %s", s.Fields[i], valstr)
				count += 1
			}
		}
		if count > 1 {
			wherestr += " and "
		}
		wherestr += qualstr
	}

	if count == 0 {
		fmt.Println("no delete where condition found, just ignore it")
		s.Stmtstr = ""
		return
	}
	// begin to generate the insert statment
	delstr += " " + wherestr + ";"

	instr := "insert into " + s.Tablename + " "
	fieldstr := " ("
	valstr := " values ("

	for i := 0; i < fieldNum; i++ {
		if i != 0 {
			fieldstr = fieldstr + ", "
			valstr = valstr + ", "
		}

		fieldstr = fieldstr + s.Fields[i]

		if _, ok := columnDesc[s.Fields[i]]; ok {
			if columnDesc[s.Fields[i]].Typ == TABLE_FIELD_TYPE_STRING {
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

	instr += s.Stmtstr + fieldstr + valstr + ";"

	s.Stmtstr = delstr + instr
	
	fmt.Println(s.Stmtstr)
}


func (s *UpdateStmt) show() {
	fmt.Println(s.Fields)
	fmt.Println(s.Values)
}

func (s *UpdateStmt) getSQL() string {
	return s.Stmtstr
}

type DeleteStmt struct {
	Stmtstr string
	Tablename string
	Fields []string
	Values []string
	keyFields []string // from old
	keyValues []string // from old
}


func (s *DeleteStmt) AddKeyColumAndValue(col string, val string) {
	s.keyFields = append(s.keyFields, col)
	s.keyValues = append(s.keyValues, val)
}

func (s *DeleteStmt) AddColumnAndValue(col string, val string) {
	s.Fields = append(s.Fields, col)
	s.Values = append(s.Values, val)
	fmt.Println("delete statment find old tuple", col, val)
}


func (s *DeleteStmt) ToSqlString() {
	// be careful of the dangous where the generated delete statment has no where
	// condition, it will delete all record, so if no condition found, just report
	// error, and not proceed
	fmt.Println("going to generate delete sql string: ",
		len(s.Fields), len(s.Values))
	columnDesc := tableDesc[s.Tablename]
	fieldNum := len(s.Fields)
	
	// begin to generate the delete statment
	delstr := "delete from " + s.Tablename
	wherestr := "where "
	
	var count int = 0
	for i := 0; i < fieldNum; i++ {
		qualstr := ""
		valstr := ""
		if fd, ok := columnDesc[s.Fields[i]]; ok {
			if fd.IsPrimaryKey {
				if fd.Typ == TABLE_FIELD_TYPE_STRING {
					valstr = "'" + s.Values[i] + "'"
				}	else {
					valstr = s.Values[i]
				}
				qualstr = fmt.Sprintf("%s = %s", s.Fields[i], valstr)
				count += 1
			}
		}
		if count > 1 {
			wherestr += " and "
		}
		wherestr += qualstr
	}

	if count == 0 {
		fmt.Println("no delete where condition found, just ignore it")
		s.Stmtstr = ""
		return
	}
	
	delstr += " " + wherestr + ";"
	
	s.Stmtstr = delstr
	
	fmt.Println(s.Stmtstr)
}


func (s *DeleteStmt) show() {
	fmt.Println(s.Fields)
	fmt.Println(s.Values)
}

func (s *DeleteStmt) getSQL() string {
	return s.Stmtstr
}


func transformInsert(table string, new map[string]interface{},
	old map[string]interface{}) string {
	// only handle the new data
	fieldsDesc := tableDesc[table]

	var stmt = &InsertStmt{Tablename: table}
	for key, value := range new {
		if _, ok := fieldsDesc[key]; !ok {
			continue
		}
		
		if value != nil {
			//fmt.Println(key, value.(string))
			stmt.AddColumnAndValue(key, value.(string))
		}	
	}

	stmt.ToSqlString()
	return stmt.getSQL()
}

func transformUpdate(table string,  new map[string]interface{},
	old map[string]interface{}) string {

	// firstly try to delete the tuple base on the old, and
	// insert a new a new one
	fieldsDesc := tableDesc[table]
	var stmt = &UpdateStmt{Tablename: table}

	for key, value := range new {
		if _, ok := fieldsDesc[key]; !ok {
			continue
		}

		if value != nil {
			stmt.AddColumnAndValue(key, value.(string))
		}
	}

	stmt.ToSqlString()
	return stmt.getSQL()
}

func transformDelete(table string, new map[string]interface{},
	old map[string]interface{}) string {

	fieldsDesc := tableDesc[table]
	var stmt = &DeleteStmt{Tablename: table}

	for key, value := range old {
		if _, ok := fieldsDesc[key]; !ok {
			continue
		}

		if value != nil {
			stmt.AddColumnAndValue(key, value.(string))
		}
	}

	stmt.ToSqlString()
	return stmt.getSQL()
}

func transformToSQL(triggerstr string) string {
	var result map[string]interface{}
	json.Unmarshal([]byte(triggerstr), &result)
	table := result["table"]
	action := result["action"]
	var newdata, olddata map[string]interface{}
	if _, ok := result["new"]; ok {
		if result["new"] != nil {
			newdata = result["new"].(map[string]interface{})
		}
	}

	if _, ok := result["old"]; ok {
		if result["old"] != nil {
			olddata = result["old"].(map[string]interface{})
		}
	}

	fmt.Println("table: ", table)
	fmt.Println("action: ", action)

	if action == "INSERT" {
		return transformInsert(table.(string), newdata, olddata)
	} else if action == "UPDATE" {
		return transformUpdate(table.(string), newdata, olddata)
	} else if action == "DELETE" {
		return transformDelete(table.(string), newdata, olddata)
	}
	panic("wrong operation type " + action.(string))
}

func waitForNotification(l *pq.Listener) {
	for {
		select  {
		case n := <-l.Notify:
			fmt.Println("Received data from channel [", n.Channel, "]")
			var prettyJSON bytes.Buffer
			err := json.Indent(&prettyJSON, []byte(n.Extra), "", "\t")
			if err != nil {
				fmt.Println("Error processing JSON: ", err)
				return
			}
			//fmt.Println(string(prettyJSON.Bytes()))

			notifyDMLToWarehouse(transformToSQL(n.Extra))
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
			tableDesc[table.Tablename] = make(map[string]*FieldDesc)
		}

		sourceFields := table.Fields
		targetFields := tableDesc[table.Tablename]
		
		for _, field := range sourceFields {
			fd := &FieldDesc{}
			fd.IsPrimaryKey = field.IsPrimaryKey
			if field.Type == "string" {
				fd.Typ = TABLE_FIELD_TYPE_STRING
			} else if field.Type == "number" {
				fd.Typ = TABLE_FIELD_TYPE_NUMBER
			}
			targetFields[field.Name] = fd
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
	if len(sql) == 0 {
		return
	}

	fmt.Println("notifyDNLToWarehouse: ", sql)

	_, err := warehouseConn.Exec(sql)
	if err != nil {
		panic(err)
	}
}

func launchWarehouseConnection() *sql.DB {
	w := config.WarehouseServer
	var conninfo = fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		w.Host, w.Port, w.Dbname, w.Username, w.Password)
	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		panic(err)
	}
	return db
}


func launchSourceListener() {
	w := config.SourcePgServer
	var conninfo = fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
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
