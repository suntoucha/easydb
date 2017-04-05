package easydb

import (
	"database/sql"
	"errors"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	master *sqlx.DB // Общий пул коннектов к мастеру
	slave  *sqlx.DB // Общий пул коннектов к слейву
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
// Query то же самое, что и sqlx.Queryx с выбором подходящей базы
//-------------------------------------------------------------------------------------------------
func Query(query string, args ...interface{}) (*sqlx.Rows, error) {
	var (
		db *sqlx.DB
	)
	db = ChooseConnection("select")

	if db == nil {
		return nil, errors.New("No connection to database")
	}

	return db.Queryx(query, args...)
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

//-------------------------------------------------------------------------------------------------
// Выполнение запросов на изменение
//-------------------------------------------------------------------------------------------------
func Exec(query string, args ...interface{}) (sql.Result, error) {
	var (
		db *sqlx.DB
	)
	db = ChooseConnection("update")

	if db == nil {
		return nil, errors.New("No connection to database")
	}
	return db.Exec(query, args...)
}

//-------------------------------------------------------------------------------------------------
// Begin открывает транзакцию на мастере
//-------------------------------------------------------------------------------------------------
func Begin() (*sqlx.Tx, error) {
	db := ChooseConnection("update")

	if db == nil {
		return nil, errors.New("No connection to database")
	}

	return db.Beginx()
}

// Инстанс базы данных
type Instance struct {
	mode string // master|slave
	db   *sqlx.DB
}

//-------------------------------------------------------------------------------------------------
// Получение Инстанс
//-------------------------------------------------------------------------------------------------
func NewInstance(driverName, connectString, mode string) (*Instance, error) {
	i := new(Instance)
	db, err := sqlx.Connect(driverName, connectString)

	// При попытке записи в slave инстнас произойдет ошибка.
	// Тем не менее, мастер позволяет читать
	if mode == "master" || mode == "slave" {
		i.mode = mode
	} else {
		return nil, errors.New("Wrong mode for NewInstance")
	}
	i.db = db

	return i, err
}

//-------------------------------------------------------------------------------------------------
// Получение одного элемента из запроса
//-------------------------------------------------------------------------------------------------
func (s *Instance) Get(dest interface{}, query string, args ...interface{}) error {
	return s.db.Get(dest, query, args...)
}

//-------------------------------------------------------------------------------------------------
// Получение массива из запроса
//-------------------------------------------------------------------------------------------------
func (s *Instance) Select(dest interface{}, query string, args ...interface{}) error {
	return s.db.Select(dest, query, args...)
}

//-------------------------------------------------------------------------------------------------
// Query то же самое, что и sqlx.Queryx с выбором подходящей базы
//-------------------------------------------------------------------------------------------------
func (s *Instance) Query(query string, args ...interface{}) (*sqlx.Rows, error) {
	if s.mode != "master" {
		return nil, errors.New("Wrong instance selected. Slave instances do not support write ops")
	}
	return s.db.Queryx(query, args...)
}

//-------------------------------------------------------------------------------------------------
// Выполнение запросов на изменение
//-------------------------------------------------------------------------------------------------
func (s *Instance) NamedExec(query string, arg interface{}) (sql.Result, error) {
	if s.mode != "master" {
		return nil, errors.New("Wrong instance selected. Slave instances do not support write ops")
	}
	return s.db.NamedExec(query, arg)
}

//-------------------------------------------------------------------------------------------------
// Выполнение запросов на изменение
//-------------------------------------------------------------------------------------------------
func (s *Instance) Exec(query string, args ...interface{}) (sql.Result, error) {
	if s.mode != "master" {
		return nil, errors.New("Wrong instance selected. Slave instances do not support write ops")
	}
	return s.db.Exec(query, args...)
}

//-------------------------------------------------------------------------------------------------
// Begin открывает транзакцию на мастере
//-------------------------------------------------------------------------------------------------
func (s *Instance) Begin() (*sqlx.Tx, error) {
	if s.mode != "master" {
		return nil, errors.New("Wrong instance selected. Slave instances do not support write ops")
	}
	return s.db.Beginx()
}

//-------------------------------------------------------------------------------------------------
// Выполнение запросов на изменение
//-------------------------------------------------------------------------------------------------
func (s *Instance) Ping() error {
	return s.db.Ping()
}

func (s *Instance) Role() string {
	return s.mode
}
