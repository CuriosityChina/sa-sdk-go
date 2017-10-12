package sensorsanalytics

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"
)

// Client sensoranalytics client
type Client struct {
	consumer        Consumer
	projectName     *string
	enableTimeFree  bool
	appVersion      *string
	superProperties map[string]interface{}
	namePattern     *regexp.Regexp
}

// NewClient create new client
func NewClient(consumer Consumer, projectName string, timeFree bool) (*Client, error) {
	var c Client
	c.consumer = consumer
	if projectName == "" {
		return &c, errors.New("project_name must not be empty")
	}
	c.projectName = &projectName
	c.enableTimeFree = timeFree
	namePattern, err := regexp.Compile("^([a-zA-Z_$][a-zA-Z0-9_$]{0,99})")
	if err != nil {
		return nil, err
	}
	c.namePattern = namePattern
	c.ClearSuperProperties()
	return &c, nil
}

func (c *Client) match(input string) bool {
	if c.namePattern.Match([]byte(input)) {
		for _, keyword := range FieldKeywords {
			if keyword == input {
				return false
			}
		}
		return true
	}
	return false
}

func (c *Client) now() int64 {
	return time.Now().Unix() * 1000
}

// RegisterSuperProperties 设置每个事件都带有的一些公共属性，当 track 的 properties 和 super properties 有相同的 key 时，将采用 track 的
// :param superProperties 公共属性
func (c *Client) RegisterSuperProperties(superProperties map[string]interface{}) {
	for k, v := range superProperties {
		c.superProperties[k] = v
	}
}

// ClearSuperProperties 删除所有已设置的事件公共属性
func (c *Client) ClearSuperProperties() {
	c.superProperties = map[string]interface{}{
		"$lib":         "golang",
		"$lib_version": SDKVersion,
	}
}

// Track 跟踪一个用户的行为。
// :param distinctID: 用户的唯一标识
// :param eventName: 事件名称
// :param properties: 事件的属性
func (c *Client) Track(distinctID string, eventName string, properties map[string]interface{}, isLoginID bool) error {
	allProperties := c.superProperties
	if properties != nil {
		for k, v := range properties {
			allProperties[k] = v
		}
	}
	return c.trackEvent("track", eventName, distinctID, "", allProperties, isLoginID)
}

// TrackSignup 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:http://www.sensorsdata.cn/manual/track_signup.html，
// 并在必要时联系我们的技术支持人员。
// :param distinct_id: 用户注册之后的唯一标识
// :param original_id: 用户注册前的唯一标识
// :param properties: 事件的属性
func (c *Client) TrackSignup(distinctID string, originalID string, properties map[string]interface{}) error {
	if len(originalID) == 0 {
		return fmt.Errorf("%s: %s", ErrIllegalDataException, "property [original_id] must not be empty")
	}
	if len(originalID) > 255 {
		return fmt.Errorf("%s: %s", ErrIllegalDataException, "the max length of property [original_id] is 255")
	}
	allProperties := c.superProperties
	if properties != nil {
		for key, value := range properties {
			allProperties[key] = value
		}
	}
	return c.trackEvent("track_signup", "$SignUp", distinctID, originalID, allProperties, false)
}

func (c *Client) normalizeData(data map[string]interface{}) (map[string]interface{}, error) {
	// 检查 distinct_id
	distinctIDI, ok := data["distinct_id"]
	if !ok {
		return data, fmt.Errorf("%s: %s", ErrIllegalDataException, "property [distinct_id] must not be empty")
	}
	distinctID, ok := distinctIDI.(string)
	if !ok || len(distinctID) == 0 {
		return data, fmt.Errorf("%s: %s", ErrIllegalDataException, "property [distinct_id] must not be empty")
	}
	if len(distinctID) > 255 {
		return data, fmt.Errorf("%s: %s", ErrIllegalDataException, "the max length of [distinct_id] is 255")
	}
	// 检查 time
	tsI, ok := data["time"]
	if !ok {
		return data, fmt.Errorf("%s: %s", ErrIllegalDataException, "property [time] must not be empty")
	}
	ts, ok := tsI.(int64)
	if !ok {
		return data, fmt.Errorf("%s: %s", ErrIllegalDataException, "property [time] must be int64")
	}
	tsNum := len(strconv.FormatInt(ts, 10))
	if tsNum < 10 || tsNum > 13 {
		return data, fmt.Errorf("%s: %s", ErrIllegalDataException, "property [time] must be a timestamp in microseconds")
	}
	if tsNum == 10 {
		ts *= 1000
	}
	data["time"] = ts

	// 检查 event name
	eventI, ok := data["event"]
	if ok {
		event, ok := eventI.(string)
		if !ok {
			return data, fmt.Errorf("%s: %s", ErrIllegalDataException, "property [event] must no be empty")
		}
		if !c.match(event) {
			return data, fmt.Errorf("%s: %s", ErrIllegalDataException, fmt.Sprintf("event name must be a valid variable name. [event=%s]", event))
		}
	}
	// 检查 project name
	projectI, ok := data["project"]
	if ok {
		project, ok := projectI.(string)
		if !ok {
			return data, fmt.Errorf("%s: %s", ErrIllegalDataException, "property [project] must no be empty")
		}
		if !c.match(project) {
			return data, fmt.Errorf("%s: %s", ErrIllegalDataException, fmt.Sprintf("project name must be a valid variable name. [project=%s]", project))
		}
	}
	// 检查 properties
	propertiesi, ok := data["properties"]
	if ok {
		properties, ok := propertiesi.(map[string]interface{})
		if ok {
			for key, value := range properties {
				if len(key) > 255 {
					return data, fmt.Errorf("%s: %s", ErrIllegalDataException, fmt.Sprintf("the max length of property key is 256. [key=%s]", key))
				}
				if !c.match(key) {
					return data, fmt.Errorf("%s: %s", ErrIllegalDataException, fmt.Sprintf("the property key must be a valid variable name. [key=%s]", key))
				}
				switch value.(type) {
				case string:
					v, ok := value.(string)
					if ok {
						if len(v) > 8192 {
							return data, fmt.Errorf("%s: %s", ErrIllegalDataException, fmt.Sprintf("the max length of property value is 8192. [value=%s]", value))
						}
					}
				case int, int32, int64, float32, float64, []string:
					continue
				default:
					return data, fmt.Errorf("%s: %s", ErrIllegalDataException, fmt.Sprintf("property value must be a str/int/float/list. [value=%s]", reflect.TypeOf(value)))
				}
			}
		} else {
			return data, fmt.Errorf("%s: %s", ErrIllegalDataException, "properties must be a map[string]interface{}")
		}
	}
	return data, nil
}

func (c *Client) getLibProperties() map[string]interface{} {
	libProperties := map[string]interface{}{
		"$lib":         "golang",
		"$lib_version": SDKVersion,
		"$lib_method":  "code",
	}
	if appVersion, ok := c.superProperties["$app_version"]; ok {
		libProperties["$app_version"] = appVersion
	}
	return libProperties
}

// getCommonProperties 构造所有 Event 通用的属性
func (c *Client) getCommonProperties() map[string]interface{} {
	commonProperties := map[string]interface{}{
		"$lib":         "golang",
		"$lib_version": SDKVersion,
	}
	if c.appVersion != nil {
		commonProperties["$app_version"] = c.appVersion
	}
	return commonProperties
}

// extractUserTime 如果用户传入了 $time 字段，则不使用当前时间。
func (c *Client) extractUserTime(properties map[string]interface{}) *int64 {
	if properties != nil {
		ti, ok := properties["$time"]
		if ok {
			t, ok := ti.(int64)
			if ok {
				delete(properties, "$time")
				return &t
			}
		}
	}
	return nil
}

// ProfileSet 直接设置一个用户的 Profile，如果已存在则覆盖
// :param distinct_id: 用户的唯一标识
// :param profiles: 用户属性
func (c *Client) ProfileSet(distinctID string, profiles map[string]interface{}, isLoginID bool) error {
	return c.trackEvent("profile_set", "", distinctID, "", profiles, isLoginID)
}

// ProfileSetOnce 直接设置一个用户的 Profile，如果某个 Profile 已存在则不设置。
// :param distinct_id: 用户的唯一标识
// :param profiles: 用户属性
func (c *Client) ProfileSetOnce(distinctID string, profiles map[string]interface{}, isLoginID bool) error {
	return c.trackEvent("profile_set_once", "", distinctID, "", profiles, isLoginID)
}

// ProfileIncrement 增减/减少一个用户的某一个或者多个数值类型的 Profile。
// :param distinct_id: 用户的唯一标识
// :param profiles: 用户属性
func (c *Client) ProfileIncrement(distinctID string, profiles map[string]interface{}, isLoginID bool) error {
	return c.trackEvent("profile_increment", "", distinctID, "", profiles, isLoginID)
}

// ProfileAppend 追加一个用户的某一个或者多个集合类型的 Profile。
// :param distinct_id: 用户的唯一标识
// :param profiles: 用户属性
func (c *Client) ProfileAppend(distinctID string, profiles map[string]interface{}, isLoginID bool) error {
	return c.trackEvent("profile_append", "", distinctID, "", profiles, isLoginID)
}

// ProfileUnset 删除一个用户的一个或者多个 Profile。
// :param distinct_id: 用户的唯一标识
// :param profile_keys: 用户属性键值列表
func (c *Client) ProfileUnset(distinctID string, profileKeys []string, isLoginID bool) error {
	var profileMap map[string]interface{}
	for _, v := range profileKeys {
		profileMap[v] = true
	}
	return c.trackEvent("profile_unset", "", distinctID, "", profileMap, isLoginID)
}

// ProfileDelete 删除整个用户的信息。
// :param distinct_id: 用户的唯一标识
func (c *Client) ProfileDelete(distinctID string, isLoginID bool) error {
	return c.trackEvent("profile_delete", "", distinctID, "", nil, isLoginID)
}

func (c *Client) trackEvent(eventType string, eventName string, distinctID string, originalID string, properties map[string]interface{}, isLoginID bool) error {
	var eventTime int64
	t := c.extractUserTime(properties)
	if t != nil {
		eventTime = *t
	} else {
		eventTime = c.now()
	}
	if isLoginID {
		properties["$is_login_id"] = true
	}
	data := map[string]interface{}{
		"type":        eventType,
		"time":        eventTime,
		"distinct_id": distinctID,
		"properties":  properties,
		"lib":         c.getLibProperties(),
	}
	if c.projectName != nil {
		data["project"] = *c.projectName
	}
	if eventType == "track" || eventType == "track_signup" {
		data["event"] = eventName
	}
	if eventType == "track_signup" {
		data["original_id"] = originalID
	}
	if c.enableTimeFree {
		data["time_free"] = true
	}
	data, err := c.normalizeData(data)
	if err != nil {
		return err
	}
	return c.consumer.Send(data)
}

// Flush 对于不立即发送数据的 Consumer，调用此接口应当立即进行已有数据的发送。
func (c *Client) Flush() error {
	return c.consumer.Flush()
}

// Close 在进程结束或者数据发送完成时，应当调用此接口，以保证所有数据被发送完毕。如果发生意外，此方法将抛出异常。
func (c *Client) Close() error {
	return c.consumer.Close()
}
