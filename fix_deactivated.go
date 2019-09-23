package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB
var ctx = context.Background()

func init() {
	log.Println("Connecting to mysql")
	var err error
	db, err = sql.Open("mysql", "exo:exo@tcp(localhost:3306)/exo")
	if err != nil {
		panic(err.Error())
	}
	log.Println("Connected")
}

func createContext(remoteID string) {
	_, err := db.ExecContext(ctx, "insert into STG_CONTEXTS (TYPE, NAME) values ('USER', ?)", remoteID)
	if err != nil {
		log.Fatal(err)
	}
}

func getContextID(remoteID string) int {
	context := db.QueryRowContext(ctx, "select CONTEXT_ID from STG_CONTEXTS where TYPE='USER' AND NAME=?", remoteID)
	var contextID int
	if err := context.Scan(&contextID); err != nil {
		log.Printf("\t No context found")
		createContext(remoteID)
		return getContextID(remoteID)
	}
	return contextID
}

func updateSetting(settingID int) {
	_, err := db.ExecContext(ctx, "update STG_SETTINGS set VALUE='false' where SETTING_ID=?", settingID)
	if err != nil {
		log.Fatal(err)
	}
}

func createSetting(contextID int) {
	_, err := db.ExecContext(ctx, "insert into STG_SETTINGS (NAME, VALUE, CONTEXT_ID, SCOPE_ID) values ('exo:isEnabled', 'false', ?, 1)", contextID)
	if err != nil {
		log.Fatal(err)
	}
}

func createOrUpdateEnableSetting(contextID int) {
	setting := db.QueryRowContext(ctx, "select SETTING_ID from STG_SETTINGS where NAME='exo:isEnabled' AND CONTEXT_ID=?", contextID)
	var settingID int
	if err := setting.Scan(&settingID); err != nil {
		log.Printf("\t No setting found, creating it")
		createSetting(contextID)
	} else {
		log.Printf("\t Updating setting %d", settingID)
		updateSetting(settingID)
	}
}

func getJbidAttrValue(login string, attr string) (int, int, string) {
	var value string
	var jbidID, valID int
	row := db.QueryRowContext(ctx, `select ID, ATTRIBUTE_ID, ATTR_VALUE from jbid_io ji
left join jbid_io_attr jia on jia.IDENTITY_OBJECT_ID = ji.ID and jia.name=?
left join jbid_io_attr_text_values jiatv on jiatv.TEXT_ATTR_VALUE_ID = jia.ATTRIBUTE_ID
where ji.name=? and identity_type = 3`, attr, login)

	err := row.Scan(&jbidID, &valID, &value)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("\t No attribute %s for %s", attr, login)
		return -1, -1, ""
	}

	return jbidID, valID, value
}

func updateJbidAttr(jbidIoID int, value string) {
	_, err := db.ExecContext(ctx, "update jbid_io_attr_text_values set ATTR_VALUE=? where TEXT_ATTR_VALUE_ID=?", value, jbidIoID)
	if err != nil {
		log.Fatal(err)
	}
}

func createJbidAttr(jbidIoID int, attr string, value string) {
	var id int64
	row := db.QueryRowContext(ctx, "select max(TEXT_ATTR_VALUE_ID) from jbid_io_attr_text_values")
	row.Scan(&id)
	id = id + 1

	_, err := db.ExecContext(ctx, "insert into jbid_io_attr (ATTRIBUTE_ID, IDENTITY_OBJECT_ID, NAME, ATTRIBUTE_TYPE) values (?, ?, ?, ?)", id, jbidIoID, attr, "text")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx, "insert into jbid_io_attr_text_values (TEXT_ATTR_VALUE_ID, ATTR_VALUE) values (?, ?)", id, value)
	if err != nil {
		log.Fatal(err)
	}
}

func updateEnable(login string) {
	jbidID, valID, val := getJbidAttrValue(login, "enabled")

	if jbidID != -1 {
		if valID > 0 {
			log.Printf("\t Updating jbid attr value %d (current val=%s)", valID, val)
			updateJbidAttr(valID, "false")
		} else {
			log.Printf("\t Creating enabled=false for %s\n", login)
			createJbidAttr(jbidID, "enabled", "false")
		}
	} else {
		log.Printf("\t User %s not found in idm tables", login)
	}
}

func main() {

	log.Println("Getting deactivated users list...")

	var count int
	row := db.QueryRowContext(ctx, "select count(*) from SOC_IDENTITIES where ENABLED=0")

	if err := row.Scan(&count); err != nil {
		panic(err.Error())
	}

	log.Printf("%d deactivated users found", count)

	rows, err := db.QueryContext(ctx, "select REMOTE_ID from SOC_IDENTITIES where ENABLED=0")
	if err != nil {
		panic(err.Error())
	}

	log.Println("Iterating on users...")

	pos := 0
	for rows.Next() {
		var remoteID string
		pos = pos + 1

		if err := rows.Scan(&remoteID); err != nil {
			panic(err.Error())
		}
		log.Printf("%d User %s", pos, remoteID)

		contextID := getContextID(remoteID)
		log.Printf("\t Context id %d", contextID)

		createOrUpdateEnableSetting(contextID)
		updateEnable(remoteID)
	}
	rows.Close()

	row = db.QueryRowContext(ctx, "select count(*) from jbid_io where name not in (select remote_id from SOC_IDENTITIES) and identity_type=3")

	if err := row.Scan(&count); err != nil {
		panic(err.Error())
	}

	log.Printf("%d jbid user not in social found", count)

	rows, err = db.QueryContext(ctx, "select ID, NAME from jbid_io where name not in (select remote_id from SOC_IDENTITIES) and identity_type=3")
	defer rows.Close()
	if err != nil {
		panic(err.Error())
	}

	pos = 0
	for rows.Next() {
		var jbidID int
		var name string
		pos = pos + 1

		if err := rows.Scan(&jbidID, &name); err != nil {
			panic(err.Error())
		}
		log.Printf("%d User %s", pos, name)

		updateEnable(name)
	}

}
