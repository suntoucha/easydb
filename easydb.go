package easydb

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"strings"
)

var (
	master *sqlx.DB //Общий пул коннектов к мастеру
	slave  *sqlx.DB //ОБщий пул коннектов к слейву
)

//-------------------------------------------------------------------------------------------------
// Соединяемся с базой и открываем пул коннектов (master и slave)
//-------------------------------------------------------------------------------------------------
func ConnectMaster(driverName string, connectString string) error {
	var (
		err error
	)

	master = nil
	master, err = sqlx.Connect(driverName, connectString)

	/*
	   - DB.SetMaxIdleConns(n int)
	   - DB.SetMaxOpenConns(n int)

	   By default, the pool grows unbounded, and connections will be created whenever there isn't a free connection available in the pool.
	   You can use DB.SetMaxOpenConns to set the maximum size of the pool.
	   Connections that are not being used are marked idle and then closed if they aren't required.
	   To avoid making and closing lots of connections,
	   set the maximum idle size with DB.SetMaxIdleConns to a size that is sensible for your query loads.

	   Отсюда: http://jmoiron.github.io/sqlx/
	*/

	return err
}

func ConnectSlave(driverName string, connectString string) error {
	var (
		err error
	)

	slave = nil
	slave, err = sqlx.Connect(driverName, connectString)

	return err
}

//-------------------------------------------------------------------------------------------------
// Выбор коннекта в зависимости от того, какая задача, и какие есть подключения:
// - для select пробуем отдать slave-коннект, если он подключен
// - во всех остальных случаях отдаем master-коннект
//-------------------------------------------------------------------------------------------------
func ChooseConnection(purpose string) *sqlx.DB {
	if purpose == "select" {
		if slave != nil {
			return slave
		}
	}

	return master
}

//-------------------------------------------------------------------------------------------------
// Получение одного элемента из запроса
//-------------------------------------------------------------------------------------------------
func Get(dest interface{}, query string, args ...interface{}) error {
	var (
		db *sqlx.DB
	)
	db = ChooseConnection("select")

	if db == nil {
		return errors.New("No connection to database")
	}

	return db.Get(dest, query, args...)
}

//-------------------------------------------------------------------------------------------------
// Получение массива из запроса
//-------------------------------------------------------------------------------------------------
func Select(dest interface{}, query string, args ...interface{}) error {
	var (
		db *sqlx.DB
	)
	db = ChooseConnection("select")

	if db == nil {
		return errors.New("No connection to database")
	}

	return db.Select(dest, query, args...)
}

//-------------------------------------------------------------------------------------------------
// Замена /*condition*/ на условие запроса
//-------------------------------------------------------------------------------------------------
func Condition(query string, condition string) string {
	return strings.Replace(query, "/*condition*/", condition, -1)
}

//-------------------------------------------------------------------------------------------------
// Выполнение запросов на изменение
//-------------------------------------------------------------------------------------------------
func NamedExec(query string, arg interface{}) (sql.Result, error) {
	var (
		db *sqlx.DB
	)
	db = ChooseConnection("update")

	if db == nil {
		return nil, errors.New("No connection to database")
	}
	return db.NamedExec(query, arg)
}
