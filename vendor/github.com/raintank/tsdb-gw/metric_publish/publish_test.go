package metric_publish

import (
	"testing"

	p "github.com/raintank/metrictank/cluster/partitioner"
	"gopkg.in/raintank/schema.v1"
)

func TestPartitioning(t *testing.T) {
	testData := []schema.MetricData{
		{Name: "name1"},
		{Name: "name2"},
		{Name: "name3"},
		{Name: "name4"},
		{Name: "name5"},
		{Name: "name6"},
		{Name: "name7"},
		{Name: "name8"},
		{Name: "name9"},
		{Name: "name10"},
	}

	part_old, _ := p.NewKafka("bySeries")

	var data []byte
	for partitions := int32(1); partitions <= 64; partitions++ {
		part_new := NewPartitioner(partitions)

		for _, m := range testData {
			data, _ = m.MarshalMsg(data)
			res_old, _ := part_old.Partition(&m, partitions)
			key_old, _ := part_old.GetPartitionKey(&m, nil)
			res_new, key_new, _ := part_new.partition(&m)
			if res_old != res_new {
				t.Fatalf("results did not match with %d partitions for %+v", partitions, m)
			}

			if string(key_old) != string(key_new) {
				t.Fatalf("keys did not match with %d partitions for %+v", partitions, m)
			}
		}
	}
}
