package pkg

type Entry struct {
	Key       string
	Value     []byte
	Tombstone bool  // 是否删除
	Version   int64 // 数据版本
}

func NewEntry() *Entry {
	return &Entry{}
}

func (p *Entry) GetKey() string {
	return p.Key
}

func (p *Entry) GetValue() []byte {
	return p.Value
}

func (p *Entry) GetTombstone() bool {
	return p.Tombstone
}

func (p *Entry) GetVersion() int64 {
	return p.Version
}
