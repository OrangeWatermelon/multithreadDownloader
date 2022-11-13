package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	log "github.com/OrangeWatermelon/zeglib/log"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

type writePart struct {
	id   int
	seek int
	data []byte
}
type MultithreadDownloader struct {
	fileName     string
	fileWriterwg *sync.WaitGroup
	wg           *sync.WaitGroup
	url          string
	userAgent    string
	threadNumber int
	writeChan    chan writePart
	fileHash     string
}

func NewMultithreadDownloader(url string, threadNumber int) *MultithreadDownloader {
	return &MultithreadDownloader{
		userAgent:    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE",
		url:          url,
		threadNumber: threadNumber,
		writeChan:    make(chan writePart),
		wg:           &sync.WaitGroup{},
		fileWriterwg: &sync.WaitGroup{},
	}
}

func (m *MultithreadDownloader) getFileSize() (int, error) {
	rsp, err := http.Head(m.url)
	if err != nil {
		return 0, err
	}
	if rsp.Header.Get("Accept-Ranges") != "bytes" {
		return 0, errors.New("not support multithread download")
	}
	contentLength := rsp.Header["Content-Length"]
	if len(contentLength) < 1 {
		return 0, errors.New("not support multithread download")
	}
	contentLengthNum, err := strconv.Atoi(contentLength[0])
	if err != nil {
		return 0, err
	}
	return contentLengthNum, nil
}

func (m *MultithreadDownloader) downloadPart(id, start, end int) error {
	defer m.wg.Done()
	headers := map[string]string{
		"User-Agent": m.userAgent,
		"Range":      fmt.Sprintf("bytes=%v-%v", start, end),
	}
	r, err := getNewRequest(m.url, "GET", headers)
	if err != nil {
		return err
	}
	partFileName := fmt.Sprintf("%s.part_%d", m.fileName, id)
	log.Infof("开始下载文件%s，从:%d到:%d", partFileName, start, end)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode > 299 {
		return errors.New(fmt.Sprintf("服务器错误状态码: %v", resp.StatusCode))
	}
	defer resp.Body.Close()

	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(bs) != (end - start + 1) {
		return errors.New("下载文件分片长度错误")
	}
	m.writeChan <- writePart{data: bs, seek: start, id: id}
	return nil
}
func (m *MultithreadDownloader) fileWriter() {

	defer m.fileWriterwg.Done()
	for part := range m.writeChan {
		partFileName := fmt.Sprintf("%s.part_%d", m.fileName, part.id)
		err := os.WriteFile(partFileName, part.data, 0644)
		if err != nil {
			log.Errorf("保存文件 %s 失败", partFileName)
		}
	}
}
func (m *MultithreadDownloader) mergeFile() error {
	fileHandle, err := os.OpenFile(m.fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return errors.New(fmt.Sprintf("创建文件失败: %v", err))
	}
	defer fileHandle.Close()
	sha256Obj := sha256.New()
	//for _, id := range m.threadNumber {
	for i := 0; i < m.threadNumber; i++ {
		partFileName := fmt.Sprintf("%s.part_%d", m.fileName, i)
		d, err := os.ReadFile(partFileName)
		if err != nil {
			log.Errorf("读取文件 %s 失败", partFileName)
		}
		sha256Obj.Write(d)
		fileHandle.Write(d)
	}
	m.fileHash = hex.EncodeToString(sha256Obj.Sum(nil))
	return nil
}
func (m *MultithreadDownloader) Start() error {

	//启动文件写入线程
	m.fileWriterwg.Add(1)
	go m.fileWriter()

	//打开文件
	var err error
	urlSplit := strings.Split(m.url, "/")
	if len(urlSplit) < 1 {
		return errors.New("获取文件名失败")
	}
	m.fileName = urlSplit[len(urlSplit)-1]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ctx
	//获取文件大小
	size, err := m.getFileSize()
	if err != nil {
		return errors.New(fmt.Sprintf("获取文件大小出错: %v", err))
	}

	var step int
	step = size / m.threadNumber
	if step*m.threadNumber < size {
		step++
	}
	//step = 10
	//多线程下载文件
	log.Infof("开始下载文件 %s , 线程数: %d", m.fileName, m.threadNumber)
	id := 0
	for i := 0; i < size; i += step {
		m.wg.Add(1)
		end := i + step
		if end > size {
			end = size
		}
		go m.downloadPart(id, i, end-1)
		id++
	}
	m.wg.Wait()
	close(m.writeChan)
	m.fileWriterwg.Wait()
	err = m.mergeFile()
	if err != nil {
		log.Errorf("合并文件失败: %v", err)
	}
	return nil
}
func getNewRequest(url, method string, headers map[string]string) (*http.Request, error) {
	r, err := http.NewRequest(
		method,
		url,
		nil,
	)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}

	return r, nil
}
func main() {
	if len(os.Args) != 3 {
		log.Error("参数数量必须为2")
		return
	}
	url := os.Args[1]
	threadNumStr := os.Args[2]
	threadNum, err := strconv.Atoi(threadNumStr)
	if err != nil {
		log.Errorf("解析线程数量错误: %v", err)
		return
	}
	downloader := NewMultithreadDownloader(url, threadNum)
	err = downloader.Start()
	if err != nil {
		log.Errorf("下载失败: %v", err)
		return
	}
	log.Infof("下载文件 %s 成功, SHA256: %s", downloader.fileName, downloader.fileHash)
}
