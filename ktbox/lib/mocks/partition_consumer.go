// Code generated by mockery v1.0.0. DO NOT EDIT.
package mocks

import "github.com/stretchr/testify/mock"
import "github.com/Shopify/sarama"

// PartitionConsumer is an autogenerated mock type for the PartitionConsumer type
type PartitionConsumer struct {
	mock.Mock
}

// AsyncClose provides a mock function with given fields:
func (_m *PartitionConsumer) AsyncClose() {
	_m.Called()
}

// Close provides a mock function with given fields:
func (_m *PartitionConsumer) Close() error {
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
func (_m *PartitionConsumer) Errors() <-chan *sarama.ConsumerError {
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

// HighWaterMarkOffset provides a mock function with given fields:
func (_m *PartitionConsumer) HighWaterMarkOffset() int64 {
	ret := _m.Called()

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	return r0
}

// Messages provides a mock function with given fields:
func (_m *PartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	ret := _m.Called()

	var r0 <-chan *sarama.ConsumerMessage
	if rf, ok := ret.Get(0).(func() <-chan *sarama.ConsumerMessage); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan *sarama.ConsumerMessage)
		}
	}

	return r0
}