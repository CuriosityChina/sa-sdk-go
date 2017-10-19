package sensorsanalytics

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Consumer sensors analytics consumer data
type Consumer interface {
	Send(message map[string]interface{}) error
	Flush() error
	Close() error
}

// DefaultConsumer 默认的 Consumer实现，逐条、同步的发送数据给接收服务器。
type DefaultConsumer struct {
	urlPrefix string
	debug     bool
}

// NewDefaultConsumer 创建新的默认 Consumer
// :param serverURL: 服务器的 URL 地址。
func NewDefaultConsumer(serverURL string) (*DefaultConsumer, error) {
	var c DefaultConsumer
	c.urlPrefix = serverURL
	return &c, nil
}

// SetDebug enable/disable consumer debug
func (c *DefaultConsumer) SetDebug(debug bool) {
	c.debug = debug
}

// Send 发送数据
func (c *DefaultConsumer) Send(msg map[string]interface{}) error {
	data, s, err := c.encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("%s: %s", ErrIllegalDataException, err)
	}
	req, err := http.NewRequest("GET", c.urlPrefix, nil)
	q := req.URL.Query()
	q.Add("data", data)
	req.URL.RawQuery = q.Encode()
	if err != nil {
		return fmt.Errorf("%s: %s", ErrNetworkException, err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	var clt http.Client
	resp, err := clt.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %s", ErrNetworkException, err)
	}
	defer resp.Body.Close()
	if c.debug {
		log.Printf("message: %s", string(s))
		log.Printf("ret_code: %d", resp.StatusCode)
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("read response body: %s", err)
		}
		log.Printf("resp content: %s", string(body))
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s: %s", ErrNetworkException, fmt.Sprintf("Error response status code [code=%d]", resp.StatusCode))
	}
	return nil
}

// Flush flush data
func (c *DefaultConsumer) Flush() error {
	return nil
}

// Close close consumer
func (c *DefaultConsumer) Close() error {
	return nil
}

func (c *DefaultConsumer) encodeMsg(msg map[string]interface{}) (string, string, error) {
	s, err := json.Marshal(msg)
	if err != nil {
		return "", "", err
	}
	data := base64.StdEncoding.EncodeToString(s)
	return data, string(s), nil
}

func (c *DefaultConsumer) encodeMsgList(msgList []string) (string, string) {
	s := fmt.Sprintf("[%s]", strings.Join(msgList, ","))
	return base64.StdEncoding.EncodeToString([]byte(s)), s
}

// BatchConsumer  批量发送数据的 Consumer，当且仅当数据达到 buffer_size 参数指定的量时，才将数据进行发送。
type BatchConsumer struct {
	DefaultConsumer
	maxBatchSize int
	batchBuffer  []string
}

// NewBatchConsumer 创建新的 batch consumer
func NewBatchConsumer(serverURL string, maxBatchSize int) (*BatchConsumer, error) {
	var c BatchConsumer
	c.urlPrefix = serverURL
	if maxBatchSize > 0 && maxBatchSize <= 50 {
		c.maxBatchSize = maxBatchSize
	} else {
		c.maxBatchSize = 50
	}
	c.batchBuffer = []string{}
	return &c, nil
}

// Send 新的 msg 加入 buffer
func (c *BatchConsumer) Send(msg map[string]interface{}) error {
	_, s, err := c.encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("%s: %s", ErrIllegalDataException, err)
	}
	c.batchBuffer = append(c.batchBuffer, string(s))
	if len(c.batchBuffer) >= c.maxBatchSize {
		return c.Flush()
	}
	return nil
}

// Flush  用户可以主动调用 flush 接口，以便在需要的时候立即进行数据发送。
func (c *BatchConsumer) Flush() error {
	if len(c.batchBuffer) > 0 {
		dataList, s := c.encodeMsgList(c.batchBuffer)
		req, err := http.NewRequest("GET", c.urlPrefix, nil)
		q := req.URL.Query()
		q.Add("data_list", dataList)
		req.URL.RawQuery = q.Encode()
		if err != nil {
			return fmt.Errorf("%s: %s", ErrNetworkException, err)
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		var clt http.Client
		resp, err := clt.Do(req)
		if err != nil {
			return fmt.Errorf("%s: %s", ErrNetworkException, err)
		}
		defer resp.Body.Close()
		if c.debug {
			log.Printf("message: %s", string(s))
			log.Printf("ret_code: %d", resp.StatusCode)
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("read response body: %s", err)
			}
			log.Printf("resp content: %s", string(body))
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("%s: %s", ErrNetworkException, fmt.Sprintf("Error response status code [code=%d]", resp.StatusCode))
		}
		c.batchBuffer = []string{}
	}
	return nil
}

// Close 在发送完成时，调用此接口以保证数据发送完成。
func (c *BatchConsumer) Close() error {
	return c.Flush()
}

// AsyncBatchConsumer 异步、批量发送数据的 Consumer。使用独立的线程进行数据发送，当满足以下两个条件之一时进行数据发送:
type AsyncBatchConsumer struct {
	DefaultConsumer
	lock          sync.Mutex
	wg            sync.WaitGroup
	maxBatchSize  int
	bufferSize    int
	senderRunning bool
	batchBuffer   []string
	sendCh        chan string
	stopCh        chan bool
}

// NewAsyncBatchConsumer 创建新的 AsyncBatchConsumer
// :param serverURL: 服务器 URL 地址
// :param maxBatchSize 单个请求发送的最大大小
// :param bufferSize 接收数据缓冲区大小
func NewAsyncBatchConsumer(serverURL string, maxBatchSize int, bufferSize int) (*AsyncBatchConsumer, error) {
	var c AsyncBatchConsumer
	c.urlPrefix = serverURL
	c.maxBatchSize = maxBatchSize
	if maxBatchSize > 0 || maxBatchSize < 50 {
		c.maxBatchSize = maxBatchSize
	} else {
		c.maxBatchSize = 50
	}
	if bufferSize > 0 || bufferSize < 1000 {

	}
	c.bufferSize = bufferSize
	c.batchBuffer = []string{}
	c.stopCh = make(chan bool, 1)
	err := c.Run()
	return &c, err
}

// Run 运行 Seeder
func (c *AsyncBatchConsumer) Run() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.senderRunning {
		return errors.New("")
	}
	go c.runSender()
	c.senderRunning = true
	return nil
}

func (c *AsyncBatchConsumer) runSender() {
	c.sendCh = make(chan string, c.bufferSize)
	ticker := time.NewTicker(30 * time.Second)
	c.wg.Add(1)
	defer c.wg.Done()
ForLoop:
	for {
		select {
		case data, ok := <-c.sendCh:
			if ok {
				c.batchBuffer = append(c.batchBuffer, data)
			}
			if len(c.batchBuffer) >= c.maxBatchSize {
				err := c.Flush()
				if err != nil {
					log.Printf("AsyncBatchConsumer Flush Data: %s", err)
				}
			}
		case <-ticker.C:
			err := c.Flush()
			if err != nil {
				log.Printf("AsyncBatchConsumer Flush Data: %s", err)
			}
		case <-c.stopCh:
			close(c.sendCh)
			for data := range c.sendCh {
				c.batchBuffer = append(c.batchBuffer, data)
				if len(c.batchBuffer) >= c.maxBatchSize {
					err := c.Flush()
					if err != nil {
						log.Printf("AsyncBatchConsumer Flush Data: %s", err)
					}
				}
			}
			err := c.Flush()
			if err != nil {
				log.Printf("AsyncBatchConsumer Flush Data: %s", err)
			}
			if c.senderRunning {
				c.senderRunning = false
				break ForLoop
			}
		}
	}
}

// Stop  停止 Sender
func (c *AsyncBatchConsumer) Stop() error {
	c.stopCh <- true
	c.wg.Wait()
	return nil
}

// Send 发送数据
func (c *AsyncBatchConsumer) Send(msg map[string]interface{}) error {
	_, s, err := c.encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("%s: %s", ErrIllegalDataException, err)
	}
	c.sendCh <- string(s)
	return nil
}

// Flush  用户可以主动调用 flush 接口，以便在需要的时候立即进行数据发送。
func (c *AsyncBatchConsumer) Flush() error {
	if len(c.batchBuffer) > 0 {
		dataList, s := c.encodeMsgList(c.batchBuffer)
		req, err := http.NewRequest("GET", c.urlPrefix, nil)
		q := req.URL.Query()
		q.Add("data_list", dataList)
		req.URL.RawQuery = q.Encode()
		if err != nil {
			return fmt.Errorf("%s: %s", ErrNetworkException, err)
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		var clt http.Client
		resp, err := clt.Do(req)
		if err != nil {
			log.Printf("%s: %s", ErrNetworkException, err)
		}
		if c.debug {
			log.Printf("message: %s", string(s))
			log.Printf("ret_code: %d", resp.StatusCode)
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("read response body: %s", err)
			}
			log.Printf("resp content: %s", string(body))
		}
		if resp.StatusCode != 200 {
			log.Printf("%s: %s", ErrNetworkException, fmt.Sprintf("Error response status code [code=%d]", resp.StatusCode))
		}
		c.batchBuffer = []string{}
	}
	return nil
}

// SyncFlush  执行一次同步发送。 表示在发送失败时抛出错误。
func (c *AsyncBatchConsumer) SyncFlush() error {
	if len(c.batchBuffer) > 0 {
		dataList, s := c.encodeMsgList(c.batchBuffer)
		req, err := http.NewRequest("GET", c.urlPrefix, nil)
		q := req.URL.Query()
		q.Add("data_list", dataList)
		req.URL.RawQuery = q.Encode()
		if err != nil {
			return fmt.Errorf("%s: %s", ErrNetworkException, err)
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		var clt http.Client
		resp, err := clt.Do(req)
		if err != nil {
			return fmt.Errorf("%s: %s", ErrNetworkException, err)
		}
		if c.debug {
			log.Printf("message: %s", string(s))
			log.Printf("ret_code: %d", resp.StatusCode)
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("read response body: %s", err)
			}
			log.Printf("resp content: %s", string(body))
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("%s: %s", ErrNetworkException, fmt.Sprintf("Error response status code [code=%d]", resp.StatusCode))
		}
		c.batchBuffer = make([]string, c.maxBatchSize)
	}
	return nil
}

// Close close consumer
func (c *AsyncBatchConsumer) Close() error {
	return c.Stop()
}

// ConsoleConsumer 将数据直接输出到标准输出
type ConsoleConsumer struct {
}

// NewConsoleConsumer 创建新的 ConsoleConsumer
func NewConsoleConsumer() *ConsoleConsumer {
	var c ConsoleConsumer
	return &c
}

// Send 发送数据
func (c *ConsoleConsumer) Send(msg map[string]interface{}) error {
	b, err := json.MarshalIndent(msg, "", "    ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

// Flush  用户可以主动调用 flush 接口，以便在需要的时候立即进行数据发送。
func (c *ConsoleConsumer) Flush() error {
	return nil
}

// Close close consumer
func (c *ConsoleConsumer) Close() error {
	return nil
}

// DebugConsumer 调试用的 Consumer，逐条发送数据到服务器的Debug API,并且等待服务器返回的结果
// 具体的说明在http://www.sensorsdata.cn/manual/
type DebugConsumer struct {
	urlPrefix      string
	debugWriteData bool
}

// NewDebugConsumer 创建新的调试 consumer
func NewDebugConsumer(serverURL string, writeData bool) (*DebugConsumer, error) {
	var c DebugConsumer
	debugURL, err := url.Parse(serverURL)
	if err != nil {
		return &c, err
	}
	debugURL.Path = "/debug"
	c.urlPrefix = debugURL.String()
	c.debugWriteData = writeData
	return &c, err
}

// Send 发送数据
func (c *DebugConsumer) Send(msg map[string]interface{}) error {
	data, s, err := c.encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("%s: %s", ErrIllegalDataException, err)
	}
	req, err := http.NewRequest("GET", c.urlPrefix, nil)
	q := req.URL.Query()
	q.Add("data", data)
	req.URL.RawQuery = q.Encode()
	if err != nil {
		return fmt.Errorf("%s: %s", ErrNetworkException, err)
	}
	if !c.debugWriteData {
		req.Header.Add("Dry-Run", "true")
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	var clt http.Client
	resp, err := clt.Do(req)
	if err != nil {
		log.Printf("%s: %s", ErrNetworkException, err)
		return fmt.Errorf("%s: %s", ErrNetworkException, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		log.Printf("%s", s)
	} else {
		log.Printf("invalid message: %s", string(data))
		log.Printf("ret_code: %d", resp.StatusCode)
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("read response body: %s", err)
		}
		log.Printf("resp content: %s", string(body))
	}
	return nil
}

// Flush  用户可以主动调用 flush 接口，以便在需要的时候立即进行数据发送。
func (c *DebugConsumer) Flush() error {
	return nil
}

// Close 在发送完成时，调用此接口以保证数据发送完成。
func (c *DebugConsumer) Close() error {
	return nil
}

func (c *DebugConsumer) encodeMsg(msg map[string]interface{}) (string, string, error) {
	s, err := json.Marshal(msg)
	if err != nil {
		return "", "", err
	}
	data := base64.StdEncoding.EncodeToString(s)
	return data, string(s), nil
}
