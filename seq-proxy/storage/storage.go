package storage

type IStorage interface {
	Load(biz string) (uint64, error)
	Store(biz string, ns uint64) error
}