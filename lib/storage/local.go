package storage

type LocalStoreManager struct {
	database map[string][]byte
}

func NewLocalStoreManager() (*LocalStoreManager) {
	return &LocalStoreManager{database: make(map[string][]byte)}
}

func (local *LocalStoreManager) Get(key string) ([]byte, error) {
	return local.database[key], nil
}

func (local *LocalStoreManager) Put(key string, val []byte) error {
	local.database[key] = val
	return nil
}
