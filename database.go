package database

import (
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

const main = "main"

type EffectorFunc func(dialector gorm.Dialector, opts ...gorm.Option) (db *gorm.DB, err error)

// Перевірка реалізації інтерфейсу
var _ Database = &database{}

// Database маніпулювання підключеннями до БД
type Database interface {
	initDb() *gorm.DB
	GetDb() *gorm.DB
}

var obj map[string]Database

func init() {
	obj = make(map[string]Database, 0)
}

type database struct {
	db  map[string]*gorm.DB
	cfg Configer
}

type Configer interface {
	GetPostgresDsn() string
	IsDebug() bool
	GetTabPrefix() string
	GetSchema() string
}

func NewPool(cfg Configer) Database {
	sch := cfg.GetSchema()
	if obj[sch] == nil {
		db := make(map[string]*gorm.DB)
		obj[sch] = &database{
			db:  db,
			cfg: cfg,
		}
	}
	return obj[sch]
}

// getDBLogLevel повертає рівень логування БД
func (d *database) getDBLogLevel() logger.LogLevel {
	if d.cfg.IsDebug() {
		return logger.Info
	}
	return logger.Warn
}

// InitDB ініціює підключення до БД
func (d *database) initDb() *gorm.DB {

	lgc := logger.New(log.StandardLogger(), logger.Config{
		SlowThreshold:             200 * time.Millisecond,
		LogLevel:                  d.getDBLogLevel(),
		IgnoreRecordNotFoundError: false,
		Colorful:                  false,
	})
	gormConfig := gorm.Config{
		Logger:                 lgc,  // Налаштування журналювання дій
		SkipDefaultTransaction: true, // Вимкнення застосування транзакцій
		NamingStrategy: schema.NamingStrategy{
			TablePrefix: d.tablePrefix(), // table name prefix, table for `User` would be `t_users`
		},
	}
	cf := connector(gorm.Open, 3, 2*time.Second)
	log.Debugf("attempt connect to %s schema %s:", d.cfg.GetPostgresDsn(), d.cfg.GetSchema())
	db, err := cf(postgres.Open(d.cfg.GetPostgresDsn()), &gormConfig)
	if err != nil {
		log.Println(err)
		os.Exit(0)
	}
	return db
}

func (d *database) tablePrefix() string {
	if d.cfg.GetSchema() == "" {
		return d.cfg.GetTabPrefix()
	}
	return d.cfg.GetSchema() + "." + d.cfg.GetTabPrefix()
}

// GetDb - повертає підключення до БД
func (d *database) GetDb() *gorm.DB {
	_, ok := d.db[main]
	if !ok {
		log.Infof("new connection to db schema %s", d.cfg.GetSchema())
		d.db[main] = d.initDb()
	} else {
		sqlDB, err := d.db[main].DB()
		if err != nil {
			panic(err)
		}
		if err = sqlDB.Ping(); err != nil {
			// Повновлення підключення
			log.Infof("renew connection to db schema %s", d.cfg.GetSchema())
			d.db[main] = d.initDb()
		}
	}
	return d.db[main]
}

// connector повертає функцію підключення до БД яка намагається виконати retties спроб підключення
// з паузами в delay * номер спроби
// Retry Pattern
func connector(effector EffectorFunc, retties int, delay time.Duration) EffectorFunc {
	return func(dialector gorm.Dialector, opts ...gorm.Option) (*gorm.DB, error) {
		for r := 0; ; r++ {
			db, err := effector(dialector, opts...)
			if err == nil || r >= retties {
				return db, err
			}
			delayInside := delay * time.Duration(r+1)
			log.Infof("attempt %d failed; retrying in %v", r+1, delayInside)
			select {
			case <-time.After(delayInside):
			}
		}
	}
}
