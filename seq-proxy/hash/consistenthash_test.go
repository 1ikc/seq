package hash

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestConsistentHash(t *testing.T) {
	hash := New(3, func(data []byte) uint32 {
		i, _ := strconv.Atoi(string(data))
		return uint32(i)
	})

	hash.Add("6", "4", "2")

	cases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range cases {
		assert.Equal(t, hash.Get(k), v)
	}

	hash.Add("8")
	cases["27"] = "8"

	for k, v := range cases {
		assert.Equal(t, hash.Get(k), v)
	}
}