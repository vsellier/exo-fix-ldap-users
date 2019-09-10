package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB
var ctx = context.Background()

func init() {
	log.Println("Connecting to mysql")
	var err error
	db, err = sql.Open("mysql", "exo:exo@tcp(host.docker.internal:3306)/exo")
	if err != nil {
		panic(err.Error())
	}
	log.Println("Connected")
}

func createJbidAttr(jbidIoID int, login string, attr string, value string) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		log.Fatal(err)
	}

	var id int64
	row := db.QueryRowContext(ctx, "select max(TEXT_ATTR_VALUE_ID) from jbid_io_attr_text_values")
	row.Scan(&id)
	id = id + 1

	_, err = db.ExecContext(ctx, "insert into jbid_io_attr (ATTRIBUTE_ID, IDENTITY_OBJECT_ID, NAME, ATTRIBUTE_TYPE) values (?, ?, ?, ?)", id, jbidIoID, attr, "text")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx, "insert into jbid_io_attr_text_values (TEXT_ATTR_VALUE_ID, ATTR_VALUE) values (?, ?)", id, value)
	if err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

}

func getJbidAttrValue(login string, attr string) (int, string) {
	var value string
	var id int
	row := db.QueryRowContext(ctx, `select ID, ATTR_VALUE from jbid_io ji
left join jbid_io_attr jia on jia.IDENTITY_OBJECT_ID = ji.ID and jia.name=?
left join jbid_io_attr_text_values jiatv on jiatv.TEXT_ATTR_VALUE_ID = jia.ATTRIBUTE_ID
where ji.name=? and identity_type = 3`, attr, login)

	err := row.Scan(&id, &value)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("No attribute %s for %s", attr, login)
		return -1, ""
	}

	return id, value
}

func updateFirstName(login string, firstName string) {
	id, val := getJbidAttrValue(login, "firstName")
	if id != -1 && val == "" {
		log.Printf("Creating first name for %s : %s\n", login, firstName)
		createJbidAttr(id, login, "firstName", firstName)
	}
}

func updateLastName(login string, lastName string) {
	id, val := getJbidAttrValue(login, "lastName")
	if id != -1 && val == "" {
		log.Printf("Creating last name for %s : %s\n", login, lastName)
		createJbidAttr(id, login, "lastName", lastName)
	}
}

func updateEmail(login string, email string) {
	id, val := getJbidAttrValue(login, "email")
	if id != -1 && val == "" {
		log.Printf("Creating email for %s : %s\n", login, email)
		createJbidAttr(id, login, "email", email)
	}
}

func main() {

	/* Using a pre-populated table with social id, login, first name, last name and email
	create table user_extract (id bigint, login varchar(1000), first_name varchar(1000), last_name varchar(1000), email varchar(1000), primary key (id));

	insert into user_extract(id) select distinct(i.IDENTITY_ID) from SOC_IDENTITIES i
	  left join SOC_SPACES_MEMBERS on USER_ID = i.REMOTE_ID;
	// where SPACE_ID in (X,Y,Z);

	update user_extract set login = (select REMOTE_ID from SOC_IDENTITIES where IDENTITY_ID = user_extract.id);
	update user_extract set first_name = (select value from SOC_IDENTITY_PROPERTIES where IDENTITY_ID=id and name='firstName');
	update user_extract set last_name = (select value from SOC_IDENTITY_PROPERTIES where IDENTITY_ID=id and name='lastName');
	update user_extract set email = (select value from SOC_IDENTITY_PROPERTIES where IDENTITY_ID=id and name='email');
	*/

	log.Println("Getting user list...")
	users, err := db.QueryContext(ctx, "select id, login, first_name, last_name, email from user_extract where first_name is not null and last_name is not null and email is not null")
	if err != nil {
		panic(err.Error())
	}
	defer users.Close()

	log.Println("Iterating on users...")
	pos := 0
	for users.Next() {
		var id int
		var login, firstName, lastName, email string
		pos = pos + 1

		if err := users.Scan(&id, &login, &firstName, &lastName, &email); err != nil {
			panic(err.Error())
		}
		log.Println(fmt.Sprintf("%d User %s", pos, login))

		updateFirstName(login, firstName)
		updateLastName(login, lastName)
		updateEmail(login, email)

	}
}
