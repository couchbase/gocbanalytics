package main

import "sync"

type MutexProtectedResources[T any] struct {
	mutex     sync.Mutex
	resources map[string]T
}

func NewMutexProtectedResources[T any]() *MutexProtectedResources[T] {
	return &MutexProtectedResources[T]{
		mutex:     sync.Mutex{},
		resources: make(map[string]T),
	}
}

func (m *MutexProtectedResources[T]) Get(key string) (T, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	resource, ok := m.resources[key]

	return resource, ok
}

func (m *MutexProtectedResources[T]) Set(key string, resource T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.resources[key] = resource
}

func (m *MutexProtectedResources[T]) Delete(key string) (T, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	resource, ok := m.resources[key]

	delete(m.resources, key)

	return resource, ok
}

func (m *MutexProtectedResources[T]) Clear() map[string]T {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	resources := m.resources
	m.resources = make(map[string]T)

	return resources
}
