package sensorsanalytics

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
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
	var dc DefaultConsumer
	dc.urlPrefix = serverURL
	return &dc
}

// Send 发送数据
func (v *DefaultConsumer) Send(msg map[string]interface{}) error {
	type request struct {
		Data string `json:"data"`
		Gzip int    `json:"gzip"`
	}
	data, err := v.encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("%s: %s", ErrIllegalDataException, err)
	}
	req := request{
		Data: data,
		Gzip: 0,
	}
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	resp, err := http.Post(v.urlPrefix, "application/json; charset=utf-8", b)
	if err != nil {
		return fmt.Errorf("%s: %s", ErrNetworkException, err)
	}
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return fmt.Errorf("%s: %s", ErrNetworkException, fmt.Sprintf("Error response status code [code=%d]", resp.StatusCode))
	}
	return nil
}

// Flush flush data
func (v *DefaultConsumer) Flush() error {
	return nil
}

// Close close consumer
func (v *DefaultConsumer) Close() error {
	return nil
}

func (v *DefaultConsumer) encodeMsg(msg map[string]interface{}) (string, error) {
	s, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}
	data := base64.StdEncoding.EncodeToString(s)
	return data, nil
}
