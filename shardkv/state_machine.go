package shardkv

type MemoryKVStateMachine struct {
	KV     map[string]string
	Status ShardStatus
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV:     make(map[string]string),
		Status: Normal,
	}
}

func (sm *MemoryKVStateMachine) Get(key string) (string, Err) {
	if value, ok := sm.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (sm *MemoryKVStateMachine) Put(key, value string) Err {
	sm.KV[key] = value
	return OK
}

func (sm *MemoryKVStateMachine) Append(key, value string) Err {
	sm.KV[key] += value
	return OK

}

func (sm *MemoryKVStateMachine) copyData() map[string]string {
	newKV := make(map[string]string)
	for k, v := range sm.KV {
		newKV[k] = v
	}
	return newKV
}
