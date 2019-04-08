package main

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	// "github.com/flimzy/kivik" // Stable version of Kivik
	_ "github.com/go-kivik/couchdb" // The CouchDB driver
	"github.com/go-kivik/kivik"     // Development version of Kivik

	_ "github.com/lib/pq"
)

const (
	DB_USER     = "postgres"
	DB_PASSWORD = "123qweasd"
	DB_NAME     = "mahery"
	PORT        = "5000"
	RELPATH     = "./"
	SCHEMA      = "mahery"
	LIMIT       = 1000
)

func main() {
	client, err := kivik.New("couch", "http://13.229.79.91:5983/")
	if err != nil {
		panic(err)
	}
	db := client.DB(context.TODO(), "opensrp-form")

	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable host=127.0.0.1 port=%s",
		DB_USER, DB_PASSWORD, DB_NAME, PORT)

	dbpg, err := sql.Open("postgres", dbinfo)
	CheckErr(err)
	defer dbpg.Close()

	resetSchema(dbpg)

	rows, err := db.Query(context.TODO(), "_design/convert", "fieldAsMap")
	if err != nil {
		panic(err)
	}
	for rows.Next() {

		var key []interface{}
		if err := rows.ScanKey(&key); err != nil {
			panic(err)
		}

		var value map[string]interface{}
		if err := rows.ScanValue(&value); err != nil {
			panic(err)
		}

		insertSql(dbpg, key, value)

		// break

	}
	if rows.Err() != nil {
		panic(rows.Err())
	}

}

func interface2string(value interface{}) string {
	switch value.(interface{}).(type) {
	case string:
		return fmt.Sprintf("%s", value.(string))
	case float64:
		return fmt.Sprintf("%f", value.(float64))
	case float32:
		return fmt.Sprintf("%f", value.(float32))
	case int:
		return fmt.Sprintf("%d", value.(int))
	case int32:
		return fmt.Sprintf("%d", value.(int32))
	case int64:
		return fmt.Sprintf("%d", value.(int64))
	case bool:
		return fmt.Sprintf("%s", strconv.FormatBool(value.(bool)))
	default:
		return ""
	}
}

func insertSql(db *sql.DB, key []interface{}, value map[string]interface{}) {

	//-----------------------------------main table
	clientVersion := int(key[3].(float64))
	serverVersion := int(key[9].(float64))

	mainQuery := fmt.Sprintf(`INSERT INTO "mahery"."opensp-form"("_id", "_key", "anmId", "clientVersion", "entityId", "formDataDefinitionVersion", "formName", "instanceId", "locationId", "serverVersion", "type", "bind_type") VALUES('%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', %d, '%s', '%s') RETURNING "id";`, key[0], key[1], key[2], clientVersion, key[4], key[5], key[6], key[7], key[8], serverVersion, key[10], key[11])

	var insertedId int
	err := db.QueryRow(mainQuery).Scan(&insertedId)
	if err != nil {
		SqlErr(err, db, mainQuery)
	}

	//-----------------------------------sub table
	subQuery := fmt.Sprintf(`INSERT INTO "mahery"."%s"`, key[11])
	queryFields := `("opensp-form-id", `
	queryValues := fmt.Sprintf(`VALUES(%d, `, insertedId)
	for k, v := range value {
		queryFields += fmt.Sprintf(`"%s", `, k)

		val := strings.Replace(interface2string(v), "'", "`", -1)
		queryValues += fmt.Sprintf(`'%s', `, val)
	}

	queryFields = queryFields[:len(queryFields)-2] + ") "
	queryValues = queryValues[:len(queryValues)-2] + "); "
	subQuery += queryFields + queryValues

	_, err = db.Exec(subQuery)
	if err != nil {
		SqlErr(err, db, subQuery)
	}

	fmt.Println("Successfully insert " + strconv.Itoa(insertedId))
}

func resetSchema(db *sql.DB) {

	listTable := `select table_name from information_schema.tables where table_schema = 'mahery'; `
	rows, err := db.Query(listTable)

	if err != nil {
		panic(err)
	}

	tables := ``

	for rows.Next() {
		var table_name string
		err = rows.Scan(&table_name)
		tables += fmt.Sprintf(`"mahery"."%s", `, table_name)
	}

	tables = tables[:len(tables)-2] + " RESTART IDENTITY;"

	truncateQuery := "TRUNCATE " + tables
	_, err = db.Exec(truncateQuery)
	if err != nil {
		fmt.Println(truncateQuery)
		panic(err)
	}

	fmt.Println("Truncating Tables")
}

func SqlErr(err error, db *sql.DB, query string) error {

	//case 1: new field
	pattern := regexp.MustCompile(`pq: column "(.+)" of relation "(.+)" does not exist`)
	find := pattern.FindStringSubmatch(err.Error())
	if len(find) > 1 {
		alterQuery := fmt.Sprintf(`ALTER TABLE "mahery"."%s" ADD COLUMN "%s" text;`, find[2], find[1])
		_, err := db.Exec(alterQuery)
		if err != nil {
			panic(err)
		}
		fmt.Println("Alter table " + find[2] + " add column " + find[1])

		_, err = db.Exec(query)
		if err != nil {
			SqlErr(err, db, query)
		}

		return nil
	}

	//case 1: new table
	pattern = regexp.MustCompile(`pq: relation "mahery.(.+)" does not exist`)
	find = pattern.FindStringSubmatch(err.Error())
	if len(find) > 1 {
		alterQuery := fmt.Sprintf(`CREATE TABLE "mahery"."%s" (
			"opensp-form-id" integer
		);
		`, find[1])
		_, err := db.Exec(alterQuery)
		if err != nil {
			panic(err)
		}
		fmt.Println("Create table " + find[1])

		_, err = db.Exec(query)
		if err != nil {
			SqlErr(err, db, query)
		}

		return nil
	}

	if err != nil {
		fmt.Println(query)
		panic(err)
	}

	return nil
}

func CheckErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
}

func insertDummy(db *kivik.DB) {
	for i := 0; i < 10000; i++ {

		key := strconv.FormatInt(time.Now().UnixNano(), 10)

		doc := map[string]interface{}{
			"time": key,
			"test": "uhuy",
		}

		_, err := db.Put(context.TODO(), key, doc)
		if err != nil {
			panic(err)
		}
		fmt.Printf("New document inserted with key %s\n", key)
	}
}
