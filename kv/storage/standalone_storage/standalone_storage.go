package standalonestorage

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/zbchi/linkv/kv/config"
)

type StandaloneStorage struct {
	db *badger.DB
}


func NewStandaloneStorage(conf*config.Config)(*StandaloneStorage){
	opts:=badger.DefaultOptions(conf.DBPath)
	db,err:=badger.Open(opts)
	if 	err!=nil{
		panic(err)
	}
	return &StandaloneStorage{db:db}
}

func(s*StandaloneStorage)Stop()error{
	return s.db.Close()
}

