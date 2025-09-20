package kafkalight

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKey(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		key, err := NewKey("test")
		assert.NoError(t, err)
		assert.Equal(t, "test", key.String())
		assert.Equal(t, []byte("test"), key.Bytes())
		assert.True(t, key.Exists())
	})

	t.Run("[]byte", func(t *testing.T) {
		key, err := NewKey([]byte("test"))
		assert.NoError(t, err)
		assert.Equal(t, "test", key.String())
		assert.Equal(t, []byte("test"), key.Bytes())
		assert.True(t, key.Exists())
	})

	t.Run("invalid type", func(t *testing.T) {
		key, err := NewKey(123)
		assert.Error(t, err)
		assert.Nil(t, key)
	})

	t.Run("empty", func(t *testing.T) {
		key, err := NewKey("")
		assert.NoError(t, err)
		assert.False(t, key.Exists())
	})
}

func TestMessage_Bind(t *testing.T) {
	type test struct {
		Foo string `json:"foo"`
	}

	t.Run("valid", func(t *testing.T) {
		var v test
		msg := &Message{
			Value: []byte(`{"foo":"bar"}`),
		}
		err := msg.Bind(&v)
		assert.NoError(t, err)
		assert.Equal(t, "bar", v.Foo)
	})

	t.Run("invalid json", func(t *testing.T) {
		var v test
		msg := &Message{
			Value: []byte(`{"foo":"bar"`),
		}
		err := msg.Bind(&v)
		assert.Error(t, err)
	})
}

func TestConvertKafkaMessageToStruct(t *testing.T) {
	// This function will be tested in kafka_test.go
}
