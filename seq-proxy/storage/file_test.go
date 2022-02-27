package storage

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var path = "./file_seq"
var dur = 10

func TestNewFileStorage(t *testing.T) {
	var cs uint64
	var err error

	// 首次测试 删除遗留文件
	if info, _ := os.Stat(path); info != nil {
		_ = os.Remove(path)
	}

	fs, err := NewFileStorage(path, time.Duration(dur))
	assert.Nil(t, err)
	defer fs.fd.Close()

	_, err = fs.Load("1")
	assert.EqualError(t, err, ErrNotRegBiz.Error())

	err = fs.Store("1", 1)
	assert.Nil(t, err)

	cs, err = fs.Load("1")
	assert.Nil(t, err)
	assert.Equal(t, cs, uint64(1))

	err = fs.Store("1", 10000)
	assert.Nil(t, err)

	cs, err = fs.Load("1")
	assert.Nil(t, err)
	assert.Equal(t, cs, uint64(10000))

	// 测试刷新磁盘
	time.Sleep(time.Duration(dur + 2) * time.Second)
}

// 测试从磁盘同步
func TestFileStorage_Load(t *testing.T) {
	fs, err := NewFileStorage(path, time.Duration(dur))
	assert.Nil(t, err)
	defer fs.fd.Close()

	cs, err := fs.Load("1")
	assert.Nil(t, err)
	assert.Equal(t, cs, uint64(10000))
}