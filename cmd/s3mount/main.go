package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/nobonobo/s3fs"
)

func main() {
	region, bucket, rootKey := "ap-northeast-3", "nobonobo-bucket", "e4-5f-01-4f-ee-28/record"
	flag.StringVar(&region, "region", region, "AWS region")
	flag.StringVar(&bucket, "bucket", bucket, "AWS S3 bucket")
	flag.StringVar(&rootKey, "root", rootKey, "AWS S3 root key")
	flag.Parse()
	if flag.NArg() < 1 {
		log.Println("need mount point arg")
		flag.Usage()
		os.Exit(1)
	}
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		log.Fatalf("LoadDefaultConfig failed: %v", err)
	}
	rootKey = strings.TrimSuffix(rootKey, "/") + "/"
	client := s3.NewFromConfig(cfg)
	if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(rootKey),
	}); err != nil {
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(rootKey),
		}); err != nil {
			log.Fatal(err)
		}
	}
	hooks := map[*regexp.Regexp]s3fs.Saver{
		regexp.MustCompile(rootKey + `index\.m3u8\.tmp`): func(key string, data []byte) {
			log.Println("update:", key, len(data))
		},
	}
	root := s3fs.New(&s3fs.Config{
		Client:  client,
		Bucket:  bucket,
		RootKey: rootKey,
		Hooks:   hooks,
	})
	server, err := fs.Mount(flag.Arg(0), root, &fs.Options{
		GID: 100,
		RootStableAttr: &fs.StableAttr{
			Mode: fuse.S_IFDIR | 0775,
			Ino:  s3fs.StringToIno(root.Key()),
		},
		MountOptions: fuse.MountOptions{
			AllowOther:     true,
			RememberInodes: false,
			DisableXAttrs:  true,
			Debug:          false,
			FsName:         "s3mount",
			Name:           "s3",
			Options:        []string{"default_permissions"},
		},
	})
	if err != nil {
		log.Fatalf("Mount failed: %v", err)
	}
	defer server.Unmount()
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	//go func() {
	<-done
	log.Println("Received signal, exiting.")
	//server.Unmount()
	//}()
	//server.Wait()
}
