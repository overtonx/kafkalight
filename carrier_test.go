package kafkalight

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageCarrier(t *testing.T) {
	msg := &Message{}
	carrier := NewMessageCarrier(msg)

	t.Run("Set and Get", func(t *testing.T) {
		carrier.Set("foo", "bar")
		assert.Equal(t, "bar", carrier.Get("foo"))
	})

	t.Run("Get non-existent", func(t *testing.T) {
		assert.Equal(t, "", carrier.Get("baz"))
	})

	t.Run("Keys", func(t *testing.T) {
		carrier.Set("key1", "val1")
		carrier.Set("key2", "val2")
		keys := carrier.Keys()
		assert.ElementsMatch(t, []string{"foo", "key1", "key2"}, keys)
	})
}
