package kvraft

type MemoryKVStateMachine struct {
	KV map[string]string
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV: make(map[string]string),
	}
}

func (sm *MemoryKVStateMachine) Get(key string) (string, Err) {
	if value, ok := sm.KV[key]; ok {
		return value, ""
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
