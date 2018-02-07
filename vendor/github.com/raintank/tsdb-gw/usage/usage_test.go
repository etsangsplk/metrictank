package usage

import (
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"gopkg.in/raintank/schema.v1"
)

func getMetricData(orgId, depth, count, interval int, prefix string) []*schema.MetricData {
	data := make([]*schema.MetricData, count)
	series := getSeriesNames(depth, count, prefix)
	for i, s := range series {

		data[i] = &schema.MetricData{
			Name:     s,
			Metric:   s,
			OrgId:    orgId,
			Interval: interval,
		}
		data[i].SetId()
	}
	return data
}

func getSeriesNames(depth, count int, prefix string) []string {
	series := make([]string, count)
	for i := 0; i < count; i++ {
		ns := make([]string, depth)
		for j := 0; j < depth; j++ {
			ns[j] = getRandomString(4)
		}
		series[i] = prefix + "." + strings.Join(ns, ".")
	}
	return series
}

// source: https://github.com/gogits/gogs/blob/9ee80e3e5426821f03a4e99fad34418f5c736413/modules/base/tool.go#L58
func getRandomString(n int, alphabets ...byte) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		if len(alphabets) == 0 {
			bytes[i] = alphanum[b%byte(len(alphanum))]
		} else {
			bytes[i] = alphabets[b%byte(len(alphabets))]
		}
	}
	return string(bytes)
}

func BenchmarkUsage(b *testing.B) {
	if err := Init("localhost:2004"); err != nil {
		b.Fatal(err.Error())
	}
	series := getMetricData(1, 7, b.N, 10, "foo")
	b.ResetTimer()
	b.ReportAllocs()
	LogRequest(1, fmt.Sprintf("api.request.%s.status.%d", "render", 200))

	LogRequest(1, fmt.Sprintf("api.request.%s.status.%d", "render", 200))
	for _, s := range series {
		LogDataPoint(s.Id)
	}

	LogRequest(1, fmt.Sprintf("api.request.%s.status.%d", "render", 200))
	Stop()
}
