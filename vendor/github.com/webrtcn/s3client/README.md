# go-ceph-s3-client 

## Installation

Install the package with:

```
go get github.com/webrtcn/s3client
```

Import it with:

```
import "github.com/webrtcn/s3client"
```

## Support feature list

Feature | Status | Remark
---|---|---
List Buckets |  √
Delete Bucket|  √
Create Bucket|  √
Bucket ACLs (Get, Put) | √
Bucket Location | √
Bucket Object Versions | √
Get Bucket Info (HEAD) |√
Put Object|√|
Delete Object|√|
Get Object|√|
Object ACLs (Get, Put)|√|
Get Object Info (HEAD)|√|
POST Object|√|
Copy Object|√|
Multipart Uploads|√|
List Multipart Uploads|√


How to pre-signed

 https://www.bennadel.com/blog/2693-uploading-files-to-amazon-s3-using-pre-signed-query-string-authentication-urls.htm

## Example


```

package main

import (
	"fmt"
	"testing"
	. "github.com/webrtcn/s3client"
	. "github.com/webrtcn/s3client/models"
)

func main {
    	//list all buckets
	client := NewClient("http://example.com", "accessKey", "secretAccessKey")
	bucket := client.NewBucket()
	values, err := bucket.List()
	if err != nil {
	 	fmt.Println(err)
	 } else {
	 	fmt.Println(values.Owner.OwnerID)
	 }
}

```

## License

Apache2.0
