package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tamnd/zou-bench/probe"
)

func cmdProbe(argv []string) {
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	endpoint := fs.String("endpoint", "", "https://s3.us-east-1.amazonaws.com or any s3 compatible endpoint")
	bucket := fs.String("bucket", "", "bucket to probe, keys go under --prefix and are deleted afterward")
	region := fs.String("region", "us-east-1", "signing region")
	name := fs.String("name", "", "profile name, also names the output file")
	samples := fs.Int("samples", 200, "requests per op kind")
	payload := fs.Int("payload", 8192, "small object bytes, the latency curve pays this size")
	large := fs.Int("large", 16<<20, "large object bytes for the throughput estimate")
	prefix := fs.String("prefix", "zoubench-probe", "key prefix inside the bucket")
	cardsdir := fs.String("cardsdir", "pricecards", "directory the calibration file lands in, next to the price cards")
	out := fs.String("out", "", "output path, overrides the cardsdir default")
	fs.Parse(argv)
	if *endpoint == "" || *bucket == "" || *name == "" {
		usage()
	}
	access := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if access == "" || secret == "" {
		die(fmt.Errorf("probe needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment"))
	}

	c := &probe.Client{
		Endpoint:  *endpoint,
		Region:    *region,
		Bucket:    *bucket,
		AccessKey: access,
		SecretKey: secret,
	}
	fmt.Fprintf(os.Stderr, "probing %s from this machine, latency includes the network position of whoever runs this\n", *endpoint)
	sum, err := probe.Run(c, probe.Options{
		Samples: *samples,
		Payload: *payload,
		Large:   *large,
		Prefix:  *prefix,
		Name:    *name,
	}, func(line string) { fmt.Fprintln(os.Stderr, line) })
	die(err)

	path := *out
	if path == "" {
		path = filepath.Join(*cardsdir, *name+".calibration.json")
	}
	die(sum.Profile.Write(path))
	fmt.Println(path)
	pretty, _ := json.MarshalIndent(sum.Profile, "", "  ")
	fmt.Println(string(pretty))
	fmt.Printf("requests %d, throttled %d\n", sum.Attempts, sum.Slowdowns)
	fmt.Printf("use it with ZOU_STORE_SIM=%s\n", path)
}
