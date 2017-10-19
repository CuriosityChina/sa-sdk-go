SA-SDK-GO
==================
SensorsAnalytics golang library

## Install
``` bash
go get -u gopkg.in/CuriosityChina/sa-sdk-go.v1
```

## Usage
### DefaultConsumer
``` go
    url := "http://127.0.0.1:8106/sa?project=default"
    consumer, err := sa.NewDefaultConsumer(url)
    if err != nil {
		log.Fatalln(err)
	}
    // consumer.SetDebug(true)
    defer consumer.Close()
	clt, err := sa.NewClient(consumer, "default", false)
	if err != nil {
		log.Fatalln(err)
    }
    distinctID := "ABCDEFG12345678"
    err = clt.Track(distinctID, "SDKTestEVENT", nil, false)
	if err != nil {
		log.Fatalln(err)
    }
    profile := map[string]interface{}{
			"Age":    33
			"Tag":    "Test",
    }
    err = clt.ProfileSet(distinctID, profile, false)
    if err != nil {
        log.Fatalln(err)
    }
```

### BatchConsumer
``` go
    url := "http://127.0.0.1:8106/sa?project=default"
    consumer, err := sa.NewBatchConsumer(url, 20)
    if err != nil {
        log.Fatalln(err)
    }
    // consumer.SetDebug(true)
    defer consumer.Close()
	clt, err := sa.NewClient(consumer, "default", false)
	if err != nil {
		log.Fatalln(err)
    }
    distinctID := "ABCDEFG12345678"
    err = clt.Track(distinctID, "SDKTestEVENT", nil, false)
	if err != nil {
		log.Fatalln(err)
    }
    profile := map[string]interface{}{
			"Age":    33
			"Tag":    "Test",
    }
    err = clt.ProfileSet(distinctID, profile, false)
    if err != nil {
		log.Fatalln(err)
    }
```

### AsyncBatchConsumer
``` go
    url := "http://127.0.0.1:8106/sa?project=default"
    consumer, err := sa.NewAsyncConsumer(url, 20, 2000)
    if err != nil {
        log.Fatalln(err)
    }
    // consumer.SetDebug(true)
    defer consumer.Close()
	clt, err := sa.NewClient(consumer, "default", false)
	if err != nil {
		log.Fatalln(err)
    }
    distinctID := "ABCDEFG12345678"
    err = clt.Track(distinctID, "SDKTestEVENT", nil, false)
	if err != nil {
		log.Fatalln(err)
    }
    profile := map[string]interface{}{
			"Age":    33
			"Tag":    "Test",
    }
    err = clt.ProfileSet(distinctID, profile, false)
    if err != nil {
		log.Fatalln(err)
    }
```

## Contributing

1. Fork it ( https://github.com/CuriosityChina/sa-sdk-go/fork )
2. Create your feature branch (`git checkout -b new-feature`)
3. Commit your changes (`git commit -asm 'Add some feature'`)
4. Push to the branch (`git push origin new-feature`)
5. Create a new Pull Request