package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultCleanInterval = 60
	DefaultSplitChar = ":"
	DefaultNewLineChar = "\n"
)

var (
	ErrNotOpenFile = errors.New("FileStorage.Load err: not open file")
	ErrNotRegBiz = errors.New("FileStorage.Load err: not register biz")
)

type FileStorage struct {
	// path 文件路径
	path string
	// dur 同步频率 单位：秒
	dur time.Duration
	// dirty 判断序号源是否污染
	dirty bool
	// meta 序号源
	meta sync.Map
	// fd 文件句柄
	fd *os.File
}

func NewFileStorage(path string, dur time.Duration) (*FileStorage, error) {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	f := &FileStorage{
		path: path,
		meta: sync.Map{},
		fd: fd,
		dur: DefaultCleanInterval * time.Second,
	}

	if dur > 0 {
		f.dur = dur * time.Second
	}

	// 从磁盘同步
	f.syncFromDisk()

	// 定时刷新磁盘
	go f.flushToDisk()

	return f, nil
}

func (f *FileStorage) Load(ssid string) (uint64, error) {
	if f.fd == nil {
		return 0, ErrNotOpenFile
	}

	csi, ok := f.meta.Load(ssid)
	if !ok {
		return 0, ErrNotRegBiz
	}

	cs := csi.(uint64)

	return cs, nil
}

func (f *FileStorage) Store(ssid string, ns uint64) error {
	if f.fd == nil {
		return ErrNotOpenFile
	}

	f.meta.Store(ssid, ns)
	f.dirty = true

	return nil
}

func (f *FileStorage) syncFromDisk() {
	br := bufio.NewReader(f.fd)
	for {
		content, _, err := br.ReadLine()
		if err != nil {
			break
		}
		arr := strings.Split(string(content), DefaultSplitChar)
		cs, err := strconv.ParseUint(arr[1], 10, 64)
		if err != nil {
			continue
		}
		f.meta.Store(arr[0], cs)
	}
}

func (f *FileStorage) flushToDisk() {
	var buf bytes.Buffer
	t := time.NewTicker(f.dur)

	walk := func(key, value interface{}) bool {
		ssid := key.(string)
		ns := value.(uint64)
		buf.WriteString(ssid)
		buf.WriteString(DefaultSplitChar)
		buf.WriteString(strconv.FormatUint(ns,10))
		buf.WriteString(DefaultNewLineChar)
		return true
	}

	for {
		select {
		case <-t.C:
			// 序号源没污染或正在同步，跳过同步
			if !f.dirty || buf.Len() > 0 {
				continue
			}

			f.meta.Range(walk)
			_, err := f.fd.WriteString(buf.String())
			if err != nil {
				log.Println(fmt.Sprintf("seq-proxy: FileStorage.syncDisk error: %v", err))
			}
			buf.Reset()
			f.dirty = false
		}
	}
}