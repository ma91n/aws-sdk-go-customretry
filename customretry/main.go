package customretry

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"strings"
)

var (
	kc = kinesis.New(session.Must(
		session.NewSession(&aws.Config{
			Retryer: CustomRetryer{
				DefaultRetryer: client.DefaultRetryer{
					NumMaxRetries: client.DefaultRetryerMaxNumRetries,
				}},
			Endpoint: aws.String("http://localhost:4566"),
			Region:   aws.String(endpoints.ApNortheast1RegionID),
		}),
	))
	outStream = "local-test-stream"
)

// CustomRetryer wraps the SDK's built in DefaultRetryer adding additional custom features.
// read: connection reset時もリトライを許容する。At Least Onceになるが、登録APIは冪等なため許容する
type CustomRetryer struct {
	client.DefaultRetryer
}

type temporary interface {
	Temporary() bool
}

func (r CustomRetryer) ShouldRetry(req *request.Request) bool {
	if origErr := req.Error; origErr != nil {
		switch origErr.(type) {
		case temporary:
			if strings.Contains(origErr.Error(), "read: connection reset") {
				// デフォルトのSDKではリトライしないが、リトライ可にする
				// 直接ここで return trueすると、リトライ回数を無視することになるため、 retry stateを設定して
				// デフォルトのリトライに委ねる
				req.Retryable = aws.Bool(true)
			}
		}
	}
	return r.DefaultRetryer.ShouldRetry(req)
}

func PutAction(ctx context.Context) error {

	_, err := kc.PutRecordWithContext(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(outStream),
		PartitionKey: aws.String("aaaa"),
		Data:         []byte("aaaa"),
	})
	if err != nil {
		return fmt.Errorf("kinesis put record with customretry : %w", err)
	}

	return nil

}
