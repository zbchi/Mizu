package storage

type Storage interface{
	Start() error
	Stop() error

}