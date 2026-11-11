# Geziyor
Geziyor is a blazing fast web crawling and web scraping framework. It can be used to crawl websites and extract structured data from them. Geziyor is useful for a wide range of purposes such as data mining, monitoring and automated testing. 

[![GoDoc](https://godoc.org/github.com/geziyor/geziyor?status.svg)](https://godoc.org/github.com/geziyor/geziyor)
[![report card](https://goreportcard.com/badge/github.com/geziyor/geziyor)](http://goreportcard.com/report/geziyor/geziyor)
[![Code Coverage](https://img.shields.io/codecov/c/github/geziyor/geziyor/master.svg)](https://codecov.io/github/geziyor/geziyor?branch=master)

## Features
- **JS Rendering**
- 5.000+ Requests/Sec
- Caching (Memory/Disk/LevelDB)
- Automatic Data Exporting (JSON, CSV, or custom)
- Metrics (Prometheus, Expvar, or custom)
- Limit Concurrency (Global/Per Domain)
- Request Delays (Constant/Randomized)
- Cookies, Middlewares, robots.txt
- Automatic response decoding to UTF-8
- Proxy management (Single, Round-Robin, Custom)

See scraper [Options](https://godoc.org/github.com/geziyor/geziyor#Options) for all custom settings. 

## Status
We highly recommend you to use Geziyor with go modules.

## Usage

This example extracts all quotes from *quotes.toscrape.com* and exports to JSON file.

```go
func main() {
    geziyor.NewGeziyor(&geziyor.Options{
        StartURLs: []string{"http://quotes.toscrape.com/"},
        ParseFunc: quotesParse,
        Exporters: []export.Exporter{&export.JSON{}},
    }).Start()
}

func quotesParse(g *geziyor.Geziyor, r *client.Response) {
    r.HTMLDoc.Find("div.quote").Each(func(i int, s *goquery.Selection) {
        g.Exports <- map[string]interface{}{
            "text":   s.Find("span.text").Text(),
            "author": s.Find("small.author").Text(),
        }
    })
    if href, ok := r.HTMLDoc.Find("li.next > a").Attr("href"); ok {
        g.Get(r.JoinURL(href), quotesParse)
    }
}
```

See [tests](https://github.com/geziyor/geziyor/blob/master/geziyor_test.go) for more usage examples.

## Documentation

### Installation

    go get -u github.com/geziyor/geziyor

If you want to make JS rendered requests, make sure you have Chrome installed.

**NOTE**: macOS limits the maximum number of open file descriptors.
If you want to make concurrent requests over 256, you need to increase limits.
Read [this](https://wilsonmar.github.io/maximum-limits/) for more.

### Making Normal Requests

Initial requests start with ```StartURLs []string``` field in ```Options```. 
Geziyor makes concurrent requests to those URLs.
After reading response, ```ParseFunc func(g *Geziyor, r *Response)``` called.

```go
geziyor.NewGeziyor(&geziyor.Options{
    StartURLs: []string{"http://api.ipify.org"},
    ParseFunc: func(g *geziyor.Geziyor, r *client.Response) {
        fmt.Println(string(r.Body))
    },
}).Start()
```

If you want to manually create first requests, set ```StartRequestsFunc```.
```StartURLs``` won't be used if you create requests manually.  
You can make requests using ```Geziyor``` [methods](https://godoc.org/github.com/geziyor/geziyor#Geziyor):

```go
geziyor.NewGeziyor(&geziyor.Options{
    StartRequestsFunc: func(g *geziyor.Geziyor) {
    	g.Get("https://httpbin.org/anything", g.Opt.ParseFunc)
        g.Head("https://httpbin.org/anything", g.Opt.ParseFunc)
    },
    ParseFunc: func(g *geziyor.Geziyor, r *client.Response) {
        fmt.Println(string(r.Body))
    },
}).Start()
``` 

### Making JS Rendered Requests

JS Rendered requests can be made using ```GetRendered``` method. 
By default, geziyor uses local Chrome application CLI to start Chrome browser. Set ```BrowserEndpoint``` option to use different chrome instance. Such as, "ws://localhost:3000"

```go
geziyor.NewGeziyor(&geziyor.Options{
    StartRequestsFunc: func(g *geziyor.Geziyor) {
        g.GetRendered("https://httpbin.org/anything", g.Opt.ParseFunc)
    },
    ParseFunc: func(g *geziyor.Geziyor, r *client.Response) {
        fmt.Println(string(r.Body))
    },
    //BrowserEndpoint: "ws://localhost:3000",
}).Start()
```

### Extracting Data

We can extract HTML elements using ```response.HTMLDoc```. HTMLDoc is Goquery's [Document](https://godoc.org/github.com/PuerkitoBio/goquery#Document).

HTMLDoc can be accessible on Response if response is HTML and can be parsed using Go's built-in HTML [parser](https://godoc.org/golang.org/x/net/html#Parse)
If response isn't HTML, ```response.HTMLDoc``` would be ```nil```.  

```go
geziyor.NewGeziyor(&geziyor.Options{
    StartURLs: []string{"http://quotes.toscrape.com/"},
    ParseFunc: func(g *geziyor.Geziyor, r *client.Response) {
        r.HTMLDoc.Find("div.quote").Each(func(_ int, s *goquery.Selection) {
            log.Println(s.Find("span.text").Text(), s.Find("small.author").Text())
        })
    },
}).Start()
```

### Exporting Data

You can export data automatically using exporters. Just send data to ```Geziyor.Exports``` chan.
[Available exporters](https://godoc.org/github.com/geziyor/geziyor/export)

```go
geziyor.NewGeziyor(&geziyor.Options{
    StartURLs: []string{"http://quotes.toscrape.com/"},
    ParseFunc: func(g *geziyor.Geziyor, r *client.Response) {
        r.HTMLDoc.Find("div.quote").Each(func(_ int, s *goquery.Selection) {
            g.Exports <- map[string]interface{}{
                "text":   s.Find("span.text").Text(),
                "author": s.Find("small.author").Text(),
            }
        })
    },
    Exporters: []export.Exporter{&export.JSON{}},
}).Start()
```


### Custom Requests - Passing Metadata To Callbacks

You can create custom requests with ```client.NewRequest```

Use that request on ```geziyor.Do(request, callback)``` 

```go
geziyor.NewGeziyor(&geziyor.Options{
    StartRequestsFunc: func(g *geziyor.Geziyor) {
        req, _ := client.NewRequest("GET", "https://httpbin.org/anything", nil)
        req.Meta["key"] = "value"
        g.Do(req, g.Opt.ParseFunc)
    },
    ParseFunc: func(g *geziyor.Geziyor, r *client.Response) {
        fmt.Println("This is our data from request: ", r.Request.Meta["key"])
    },
}).Start()
```

### Proxy - Use proxy per request
If you want to use proxy for your requests, and you have 1 proxy, you can just set these env values:
`HTTP_PROXY`
`HTTPS_PROXY`
And geziyor will use those proxies.

Also, you can use in-order proxy per request by setting `ProxyFunc` option to `client.RoundRobinProxy` 
Or any custom proxy selection function that you want. See `client/proxy.go` on how to implement that kind of custom proxy selection function.

Proxies can be HTTP, HTTPS and SOCKS5.

Note: If you use `http` scheme for proxy, It'll be used for http requests and not for https requests.   

```go
geziyor.NewGeziyor(&geziyor.Options{
    StartURLs:         []string{"http://httpbin.org/anything"},
    ParseFunc:         parseFunc,
    ProxyFunc:         client.RoundRobinProxy("http://some-http-proxy.com", "https://some-https-proxy.com", "socks5://some-socks5-proxy.com"),
}).Start()
```

## Replaying POST request bodies

When Geziyor retries a failed request, the request body is normally already
consumed and cannot be sent again. Enable body replay to buffer the body once,
before the first network attempt, and fully replay it on every retry and on
307/308 redirects. The mode is **disabled by default** and applies to plain
HTTP requests only (rendered Chrome requests are unaffected).

Bodies up to `BodyReplayMemoryLimit` bytes (**M**) stay in memory; larger bodies
spill to a temporary file. Bodies larger than `BodyReplayMaxSize` bytes
(**L**) fail with an error before any request is sent. The limits must satisfy
`0 < M <= L`; an invalid configuration disables replay with a log message.

```go
geziyor.NewGeziyor(&geziyor.Options{
    StartURLs: []string{"https://example.com"},
    BodyReplayEnabled:     true,
    BodyReplayMemoryLimit: 8 << 20,  // M: up to 8MB in memory, then a temp file
    BodyReplayMaxSize:     64 << 20, // L: reject bodies larger than 64MB
    StartRequestsFunc: func(g *geziyor.Geziyor) {
        g.Post("https://example.com/submit", strings.NewReader(payload), nil)
    },
    ParseFunc: func(g *geziyor.Geziyor, r *client.Response) {
        // ...
    },
}).Start()
```

Guarantees:

- Unknown-length and non-seekable body streams are supported.
- Memory usage never exceeds **M** plus one fixed read chunk; the full body is
  never held in memory when it is larger than **M**.
- Exceeding **L**, a body read failure, or a declared `Content-Length` that
  does not match the actual body size fail the request without sending it, and
  the error is reported once through the normal error path (`ErrorFunc`)
  without being retried.
- 307/308 redirects keep the original method and body, while 301/302/303 still
  convert the method to GET. Cookies, redirect limits and retry counts keep
  working as before.
- The caller-provided body is closed exactly once; temporary files and handles
  are released when the request finishes, even if every attempt failed.

## Benchmark

**8748 request per seconds** on *Macbook Pro 15" 2016*

See [tests](https://github.com/geziyor/geziyor/blob/master/geziyor_test.go) for this benchmark function:

```bash
>> go test -run none -bench Requests -benchtime 10s
goos: darwin
goarch: amd64
pkg: github.com/geziyor/geziyor
BenchmarkRequests-8   	  200000	    108710 ns/op
PASS
ok  	github.com/geziyor/geziyor	22.861s
```
