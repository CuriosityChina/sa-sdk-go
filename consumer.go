package sensorsanalytics

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
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
}

// NewDefaultConsumer 创建新的默认 Consumer
// :param serverURL: 服务器的 URL 地址。
func NewDefaultConsumer(serverURL string) *DefaultConsumer {
	var c DefaultConsumer
	c.urlPrefix = serverURL
	return &c
}

// Send 发送数据
func (c *DefaultConsumer) Send(msg map[string]interface{}) error {
	data, err := c.encodeMsg(msg)
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
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
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

func (c *DefaultConsumer) encodeMsg(msg map[string]interface{}) (string, error) {
	s, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}
	data := base64.StdEncoding.EncodeToString(s)
	return data, nil
}

// BatchConsumer  批量发送数据的 Consumer，当且仅当数据达到 buffer_size 参数指定的量时，才将数据进行发送。
type BatchConsumer struct {
	DefaultConsumer
	bufferSize int
	buffer     []string
}

// NewBatchConsumer 创建新的 batch consumer
func NewBatchConsumer(serverURL string, bufferSize int) *BatchConsumer {
	var c BatchConsumer
	c.urlPrefix = serverURL
	if bufferSize > 0 && bufferSize <= 50 {
		c.bufferSize = bufferSize
	} else {
		c.bufferSize = 20
	}
	buffer := make([]string, c.bufferSize)
	c.buffer = buffer
	return &c
}

// Send 新的 msg 加入 buffer
func (c *BatchConsumer) Send(msg map[string]interface{}) error {
	data, err := c.encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("%s: %s", ErrIllegalDataException, err)
	}
	c.buffer = append(c.buffer, data)
	if len(c.buffer) >= c.bufferSize {
		return c.Flush()
	}
	return nil
}

// Flush  用户可以主动调用 flush 接口，以便在需要的时候立即进行数据发送。
func (c *BatchConsumer) Flush() error {
	for _, v := range c.buffer {
		req, err := http.NewRequest("GET", c.urlPrefix, nil)
		q := req.URL.Query()
		q.Add("data", v)
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
		if resp.StatusCode != 200 && resp.StatusCode != 201 {
			log.Printf("%s: %s", ErrNetworkException, fmt.Sprintf("Error response status code [code=%d]", resp.StatusCode))
		}
	}
	c.buffer = make([]string, c.bufferSize)
	return nil
}

// Close 在发送完成时，调用此接口以保证数据发送完成。
func (c *BatchConsumer) Close() error {
	return c.Flush()
}

// type AsyncBatchConsumer struct {
// 	DefaultConsumer
// }

// func (c *AsyncBatchConsumer) Send(msg map[string]interface{}) error {
// 	return nil
// }

// func (c *AsyncBatchConsumer) Flush() {
// }

// func (c *AsyncBatchConsumer) Sync_Flush() error {
// 	return nil
// }

// func (c *AsyncBatchConsumer) Close() error {
// 	return nil
// }

// func (c *AsyncBatchConsumer) Run() {

// }

type ConsoleConsumer struct {
}

func NewConsoleConsumer() *ConsoleConsumer {
	var c ConsoleConsumer
	return &c
}

func (c *ConsoleConsumer) Send(msg map[string]interface{}) error {
	b, err := json.MarshalIndent(msg, "", "    ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func (c *ConsoleConsumer) Flush() error {
	return nil
}

func (c *ConsoleConsumer) Close() error {
	return nil
}

type DebugConsumer struct {
	urlPrefix      string
	debugWriteData bool
}

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

func (c *DebugConsumer) Send(msg map[string]interface{}) error {
	data, err := c.encodeMsg(msg)
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
		log.Printf("%s", string(data))
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

func (c *DebugConsumer) Flush() error {
	return nil
}

func (c *DebugConsumer) Close() error {
	return nil
}

func (c *DebugConsumer) encodeMsg(msg map[string]interface{}) (string, error) {
	s, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}
	data := base64.StdEncoding.EncodeToString(s)
	return data, nil
}
