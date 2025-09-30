package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	_ "github.com/lib/pq"
)

// Flag Names
const (
	nmHost   = "host"
	nmPort   = "port"
	nmUser   = "user"
	nmPasswd = "password"
	nmDbname = "dbname"

	nmRecreate    = "recreate"
	nmDeleteAll   = "delall"
	nmDeleteOne   = "delone"
	nmInsertOne   = "insertone"
	nmSelectOne   = "selectone"
	nmSelectAll   = "selectall"
	nmUpdateOne   = "updateone"
	nmUseTrans    = "usetrans"
	nmUseTwoPhase = "usetwophase"
	nmHoldConn    = "holdconn"

	allCharacters = "qazwsxedcrfvYUIOPmnjklQWERT123456786"
)

// 数据库连接配置
var (
	host     *string
	port     *string
	user     *string
	password *string
	dbname   *string

	recreate    *bool
	delall      *bool
	delone      *bool
	insertone   *bool
	selectone   *bool
	selectall   *bool
	updateone   *bool
	usetrans    *bool
	usetwophase *bool
	holdconn    *bool
	help        *bool
)

// User 模型
type User struct {
	ID        int
	Name      string
	Email     string
	CreatedAt time.Time
}

var db *sql.DB

// initDB 初始化数据库连接
func initDB() {
	var psqlInfo string
	/*
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			host, port, user, password, dbname)
	*/
	if len(*password) != 0 {
		psqlInfo = "host=" + *host + " port=" + *port + " user=" + *user + " password=" + *password +
			" dbname=" + *dbname + " sslmode=disable"
	} else {
		psqlInfo = "host=" + *host + " port=" + *port + " user=" + *user +
			" dbname=" + *dbname + " sslmode=disable"
	}

	log.Println("connection string:", psqlInfo)

	var err error
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	fmt.Println("Successfully connected to PostgreSQL!")
}

// createTable 创建用户表
func createTable() {
	query := `drop table if exists users`
	_, err := db.Exec(query)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	query = `
	CREATE TABLE IF NOT EXISTS users (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email VARCHAR(100) UNIQUE NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)
	`

	_, err = db.Exec(query)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
	fmt.Println("Table created successfully")
}

// createUser 插入用户
func createUser(name, email string) (int, error) {
	query := `INSERT INTO users (name, email) VALUES ($1, $2)`

	db.QueryRow(query, name, email)

	return 0, nil
}

// getUserByID 查询用户
func getUserByID(id int) (User, error) {
	query := `SELECT id, name, email, created_at FROM users WHERE id = $1`

	var user User
	err := db.QueryRow(query, id).Scan(&user.ID, &user.Name, &user.Email, &user.CreatedAt)
	if err != nil {
		return User{}, err
	}
	return user, nil
}

// getAllUsers 获取所有用户
func getAllUsers() ([]User, error) {
	query := `SELECT id, name, email, created_at FROM users ORDER BY id`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.CreatedAt)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}

// updateUser 更新用户
func updateUser(id int, name, email string) error {
	query := `UPDATE users SET name = $1, email = $2 WHERE id = $3`

	_, err := db.Exec(query, name, email, id)
	return err
}

// deleteUser 删除用户
func deleteUser(id int) error {
	query := `DELETE FROM users WHERE id = $1`

	_, err := db.Exec(query, id)
	return err
}

// deleteAllUsers 删除用户
func deleteAllUsers() error {
	query := `DELETE FROM users`

	_, err := db.Exec(query)
	return err
}

// createUserWithTransaction 使用事务创建用户
func createUserWithTransaction(name, email string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // 如果事务失败，自动回滚

	// 插入用户
	tx.QueryRow(`INSERT INTO users (name, email) VALUES ($1, $2)`, name, email)
	if err != nil {
		return err
	}

	// 这里可以添加其他需要在同一事务中执行的操作
	// 例如：创建用户配置、日志记录等

	// 提交事务
	return tx.Commit()
}

// createUserWithTransaction 使用事务创建用户
func createUserWithTransaction2() error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // 如果事务失败，自动回滚

	for i := 0; i < 3; i++ {
		name := randomName()
		// 插入用户
		tx.QueryRow(`INSERT INTO users (name, email) VALUES ($1, $2)`, name, name+"@example.com")
		if err != nil {
			return err
		}
		fmt.Printf("Created user with name: %s in transaction \n", name)
	}

	// 这里可以添加其他需要在同一事务中执行的操作
	// 例如：创建用户配置、日志记录等

	// 提交事务
	return tx.Commit()
}

func initFlagSet() *flag.FlagSet {
	fset := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	host = fset.String(nmHost, "127.0.0.1", "pg host")
	port = fset.String(nmPort, "5432", "pg port")
	user = fset.String(nmUser, "linpin", "login user")
	password = fset.String(nmPasswd, "", "login passwd")
	dbname = fset.String(nmDbname, "postgres", "login database")

	recreate = fset.Bool(nmRecreate, false, "drop and rcreate table")
	delall = fset.Bool(nmDeleteAll, false, "delete all data")
	delone = fset.Bool(nmDeleteOne, false, "delete one row")
	insertone = fset.Bool(nmInsertOne, false, "insert one row")
	selectone = fset.Bool(nmSelectOne, false, "select one row")
	selectall = fset.Bool(nmSelectAll, false, "select all rows")
	updateone = fset.Bool(nmUpdateOne, false, "update one row")
	usetrans = fset.Bool(nmUseTrans, false, "update in trans")
	usetwophase = fset.Bool(nmUseTwoPhase, false, "update in trans")
	holdconn = fset.Bool(nmHoldConn, false, "sleep 10s after connecting to db")
	help = fset.Bool("help", false, "show the usage")

	// Ignore errors; CommandLine is set for ExitOnError.
	// nolint:errcheck
	fset.Parse(os.Args[1:])
	if *help {
		fset.Usage()
		os.Exit(0)
	}
	return fset
}

func randomInt(min, max int) int {
	return rand.Intn(max-min+1) + min
}

func randomName() string {
	startPos := randomInt(0, len(allCharacters)-10)
	nameLen := randomInt(5, 10)
	return allCharacters[startPos : startPos+nameLen]
}

func main() {
	initFlagSet()

	// 初始化数据库连接
	initDB()

	defer db.Close()

	if *holdconn {
		time.Sleep(time.Second * 20)
	}

	// 创建表
	if *recreate {
		createTable()
	}

	// 创建用户
	if *insertone {
		name := randomName()
		id, err := createUser(name, name+"@example.com")
		if err != nil {
			log.Fatal("Failed to create user:", err)
		}
		fmt.Printf("Created user with ID: %d name: %s\n", id, name)
	}

	// 查询用户
	if *selectone {
		id := randomInt(1, 10)
		user, err := getUserByID(id)
		if err != nil {
			log.Fatal("Failed to get user:", err)
		}
		fmt.Printf("User: %+v\n", user)
	}

	// 更新用户
	if *updateone {
		id := randomInt(1, 10)
		name := randomName()
		err := updateUser(id, name, name+"@example.com")
		if err != nil {
			log.Fatal("Failed to update user:", err)
		}
	}

	// 获取所有用户
	if *selectall {
		users, err := getAllUsers()
		if err != nil {
			log.Fatal("Failed to get users:", err)
		}
		fmt.Println("All users:")
		for _, u := range users {
			fmt.Printf("- %s (%s)\n", u.Name, u.Email)
		}
	}

	if *delone {
		id := randomInt(1, 10)
		err := deleteUser(id)
		if err != nil {
			log.Fatal("Failed to delete user with id: ", id)
		}
		fmt.Printf("delete user with id: %d \n", id)
	}

	if *delall {
		err := deleteAllUsers()
		if err != nil {
			log.Fatal("Failed to delete user with id: ")
		}
		fmt.Printf("delete all users\n")
	}

	// 使用事务
	if *usetrans {
		name := randomName()
		err := createUserWithTransaction(name, name+"@example.com")
		if err != nil {
			log.Fatal("Failed to create user with transaction:", err)
		}
		fmt.Printf("Created user with name: %s in transaction \n", name)
	}

	if *usetwophase {
		err := createUserWithTransaction2()
		if err != nil {
			log.Fatal("Failed to create user with transaction:", err)
		}
	}
}

func getDBConfig() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_NAME"))
}
