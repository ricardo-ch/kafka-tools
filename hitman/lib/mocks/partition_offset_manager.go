// Code generated by mockery v1.0.0. DO NOT EDIT.
package mocks

import "github.com/stretchr/testify/mock"
import "github.com/Shopify/sarama"

// PartitionOffsetManager is an autogenerated mock type for the PartitionOffsetManager type
type PartitionOffsetManager struct {
	mock.Mock
}

// AsyncClose provides a mock function with given fields:
func (_m *PartitionOffsetManager) AsyncClose() {
	_m.Called()
}

// Close provides a mock function with given fields:
func (_m *PartitionOffsetManager) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Errors provides a mock function with given fields:
func (_m *PartitionOffsetManager) Errors() <-chan *sarama.ConsumerError {
	ret := _m.Called()

	var r0 <-chan *sarama.ConsumerError
	if rf, ok := ret.Get(0).(func() <-chan *sarama.ConsumerError); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan *sarama.ConsumerError)
		}
	}

	return r0
}

// MarkOffset provides a mock function with given fields: offset, metadata
func (_m *PartitionOffsetManager) MarkOffset(offset int64, metadata string) {
	_m.Called(offset, metadata)
}

// NextOffset provides a mock function with given fields:
func (_m *PartitionOffsetManager) NextOffset() (int64, string) {
	ret := _m.Called()

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 string
	if rf, ok := ret.Get(1).(func() string); ok {
		r1 = rf()
	} else {
		r1 = ret.Get(1).(string)
	}

	return r0, r1
}

// ResetOffset provides a mock function with given fields: offset, metadata
func (_m *PartitionOffsetManager) ResetOffset(offset int64, metadata string) {
	_m.Called(offset, metadata)
}