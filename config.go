package main

type SourcePgHost struct {
	Host string `json:"host"`
	Port string `json:"port"`
	Dbname string `json:"dbname"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type WarehouseHost struct {
	Host string `json:"host"`
	Port string `json:"port"`
	Dbname string `json:"dbname"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type NameType struct {
	Name string `json:"name"`
	Type string `json:"type"`
	IsPrimaryKey bool `json:"isPrimarykey"`
}

type TableMap struct {
	Tablename string `json:"tablename"`
	Mode string `json:"mode"`
	Fields []NameType `json:"fields"`
}

//type TableMaps struct {
//	TableMaps []TableMap `json:tablemaps`
//}

type Config struct {
	SourcePgServer SourcePgHost `json:"source_pg_server"`
	WarehouseServer WarehouseHost `json:"warehouse_server"`
	TableMaps []TableMap `json:"tablemaps"`
}
//https://tutorialedge.net/golang/parsing-json-with-golang/
