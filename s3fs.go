package s3fs

import (
	"bytes"
	"context"
	"hash/fnv"
	"io"
	"log"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type Saver func(key string, data []byte)

func StringToIno(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

var exestsFolder map[string]bool

type File struct {
	fs.Inode
	node    *Node
	client  *s3.Client
	bucket  string
	key     string
	hook    Saver
	buffer  []byte
	needPut bool
}

func (fp *File) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if fp.buffer == nil {
		return nil, syscall.EIO
	}
	n := copy(dest, fp.buffer[off:])
	//log.Println("Read:", fp.key, off, n)
	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (fp *File) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	if fp.buffer == nil {
		return 0, syscall.EIO
	}
	fp.needPut = true
	newEnd := off + int64(len(data))
	if int64(len(fp.buffer)) < newEnd {
		newBuf := make([]byte, newEnd)
		copy(newBuf, fp.buffer)
		fp.buffer = newBuf
	}
	n := copy(fp.buffer[off:newEnd], data)
	//log.Println("Write:", fp.key, off, n)
	return uint32(n), fs.OK
}

func (fp *File) Flush(ctx context.Context) syscall.Errno {
	if len(fp.buffer) == 0 {
		return fs.OK
	}
	if fp.needPut {
		fp.needPut = false
		log.Println("Flush:", fp.key, len(fp.buffer))
		if fp.hook != nil {
			fp.hook(fp.key, fp.buffer)
		}
		_, err := fp.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(fp.bucket),
			Key:    aws.String(fp.key),
			Body:   bytes.NewBuffer(fp.buffer),
		})
		if err != nil {
			log.Println("PutObject failed:", err)
			return syscall.EIO
		}
		fp.node.mu.Lock()
		defer fp.node.mu.Unlock()
		fp.node.attr = nil
		fp.node.modified = true
	}
	return fs.OK
}

type Config struct {
	Client  *s3.Client
	Bucket  string
	RootKey string
	Hooks   map[*regexp.Regexp]Saver
}

// Node
type Node struct {
	mu sync.Mutex
	fs.Inode
	client   *s3.Client
	bucket   string
	key      string
	hooks    map[*regexp.Regexp]Saver
	attr     *fuse.Attr
	modified bool
}

func New(cfg *Config) *Node {
	key := strings.TrimSuffix(cfg.RootKey, "/")
	return &Node{
		client: cfg.Client,
		bucket: cfg.Bucket,
		key:    key + "/",
		hooks:  cfg.Hooks,
	}
}

func (n *Node) Bucket() string { return n.bucket }
func (n *Node) Key() string    { return n.key }

func (n *Node) readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Println("Readdir:", n.key)
	results := []fuse.DirEntry{}
	var token *string
	for {
		entries, err := n.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(n.bucket),
			Prefix:            aws.String(n.key),
			Delimiter:         aws.String("/"),
			MaxKeys:           aws.Int32(100),
			ContinuationToken: token,
		})
		if err != nil {
			log.Println("ListObjectsV2 failed:", err)
			return nil, syscall.EIO
		}
		for _, item := range entries.Contents {
			name := (*item.Key)[len(n.key):]
			if name == "" {
				continue
			}
			results = append(results, fuse.DirEntry{
				Name: name,
				Mode: fuse.S_IFREG,
			})
		}
		for _, item := range entries.CommonPrefixes {
			name := (*item.Prefix)[len(n.key):]
			if name == "" {
				continue
			}
			name = strings.TrimSuffix(name, "/")
			results = append(results, fuse.DirEntry{
				Name: name,
				Mode: fuse.S_IFDIR,
			})
		}
		if !*entries.IsTruncated {
			break
		}
		token = entries.NextContinuationToken
	}
	return fs.NewListDirStream(results), fs.OK
}

func (n *Node) OnAdd(ctx context.Context) {
	if !n.IsDir() {
		return
	}
	log.Println("OnAdd:", n.key)
	entries, errno := n.readdir(ctx)
	if errno != fs.OK {
		log.Println("readdir failed:", errno)
		return
	}
	for entries.HasNext() {
		entry, errno := entries.Next()
		if errno != fs.OK {
			log.Println("readdir failed:", errno)
			return
		}
		if entry.Name == "." || entry.Name == ".." {
			continue
		}
		if entry.Mode == fuse.S_IFDIR {
			node := &Node{
				client: n.client,
				bucket: n.bucket,
				key:    filepath.Join(n.key, entry.Name) + "/",
				hooks:  n.hooks,
			}
			n.AddChild(entry.Name, n.NewInode(ctx, node, fs.StableAttr{Mode: fuse.S_IFDIR}), true)
		} else {
			node := &Node{
				client: n.client,
				bucket: n.bucket,
				key:    filepath.Join(n.key, entry.Name),
				hooks:  n.hooks,
			}
			n.AddChild(entry.Name, n.NewInode(ctx, node, fs.StableAttr{Mode: fuse.S_IFREG}), true)
		}
	}
}

func (n *Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.modified && n.attr != nil {
		//log.Println("Getattr(cached):", n.key, n.Inode.Path(nil), n.attr.Size)
		out.Attr = *n.attr
		return fs.OK
	}
	res, err := n.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &n.bucket,
		Key:    &n.key,
	})
	if err != nil {
		log.Println("HeadObject failed:", n.key, err)
		return syscall.ENOENT
	}
	if strings.HasSuffix(n.key, "/") {
		n.attr = &fuse.Attr{
			Ino:   StringToIno(n.key),
			Mode:  syscall.S_IFDIR | 0775,
			Size:  0,
			Ctime: uint64(res.LastModified.Unix()),
			Mtime: uint64(res.LastModified.Unix()),
			Atime: uint64(res.LastModified.Unix()),
		}
		out.Attr = *n.attr
		n.modified = false
		//log.Println("Getattr:", n.key, n.Inode.Path(nil), n.attr.Size)
		return fs.OK
	}
	n.attr = &fuse.Attr{
		Ino:   StringToIno(n.key),
		Mode:  syscall.S_IFREG | 0664,
		Size:  uint64(*res.ContentLength),
		Ctime: uint64(res.LastModified.Unix()),
		Mtime: uint64(res.LastModified.Unix()),
		Atime: uint64(res.LastModified.Unix()),
	}
	out.Attr = *n.attr
	n.modified = false
	//log.Println("Getattr:", n.key, n.Inode.Path(nil), n.attr.Size)
	return fs.OK
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	log.Println("Setattr:", n.key, in.Size)
	n.attr = &fuse.Attr{
		Ino:   StringToIno(n.key),
		Mode:  in.Mode,
		Size:  in.Size,
		Ctime: uint64(in.Ctime),
		Mtime: uint64(in.Mtime),
		Atime: uint64(in.Atime),
	}
	out.Attr = *n.attr
	n.modified = true
	return fs.OK
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Printf("Open: %s %x", n.key, flags)
	obj, err := n.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &n.bucket,
		Key:    &n.key,
	})
	if err != nil {
		log.Println("Open failed:", err)
		return nil, 0, syscall.ENOENT
	}
	buffer := []byte{}
	if flags&syscall.O_WRONLY == 0 || flags&syscall.O_APPEND != 0 {
		b, err := io.ReadAll(obj.Body)
		if err != nil {
			log.Println("ReadAll failed:", err)
			return nil, 0, syscall.EIO
		}
		buffer = b
	}
	var hook Saver
	for m, h := range n.hooks {
		if m.MatchString(n.key) {
			hook = h
			break
		}
	}
	fp := &File{
		node:   n,
		client: n.client,
		bucket: n.bucket,
		key:    n.key,
		hook:   hook,
		buffer: buffer,
	}
	return fp, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	fpath := filepath.Join(n.key, name)
	log.Println("Create:", fpath)
	obj, err := n.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &n.bucket,
		Key:    &n.key,
	})
	buffer := bytes.NewBuffer(nil)
	if err == nil {
		if _, err := io.Copy(buffer, obj.Body); err != nil {
			log.Println("Copy failed:", err)
			return nil, nil, 0, syscall.EIO
		}
	}
	np := &Node{
		client: n.client,
		bucket: n.bucket,
		key:    fpath,
		attr: &fuse.Attr{
			Ino:   StringToIno(fpath),
			Mode:  fuse.S_IFREG | 0664,
			Size:  0,
			Ctime: uint64(obj.LastModified.Unix()),
			Mtime: uint64(obj.LastModified.Unix()),
			Atime: uint64(obj.LastModified.Unix()),
		},
	}
	var hook Saver
	for m, h := range n.hooks {
		if m.MatchString(fpath) {
			hook = h
			break
		}
	}
	fp := &File{
		node:   np,
		client: n.client,
		bucket: n.bucket,
		key:    np.key,
		hook:   hook,
		buffer: buffer.Bytes(),
	}
	node := n.NewInode(ctx, np, fs.StableAttr{Mode: fuse.S_IFREG})
	return node, fp, 0, fs.OK
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fpath := filepath.Join(n.key, name) + "/"
	log.Println("Mkdir:", fpath)
	_, err := n.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(n.bucket),
		Key:    aws.String(fpath),
		Body:   strings.NewReader(""),
	})
	if err != nil {
		log.Println("Mkdir failed:", err)
		return nil, syscall.EIO
	}
	node := n.NewInode(ctx, &Node{
		client: n.client,
		bucket: n.bucket,
		key:    fpath,
	}, fs.StableAttr{Mode: fuse.S_IFDIR})
	out.Attr = fuse.Attr{
		Mode: fuse.S_IFDIR | 0755,
		Size: 0,
	}
	return node, fs.OK
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	fpath := filepath.Join(n.key, name)
	log.Println("Unlink:", fpath)
	_, err := n.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(n.bucket),
		Key:    aws.String(fpath),
	})
	if err != nil {
		log.Println("Unlink failed:", err)
		return syscall.EIO
	}
	return fs.OK
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	target := n.GetChild(name)
	if target == nil || !target.IsDir() {
		log.Println("target isn't directory:", name)
		return syscall.EIO
	}
	if len(target.Children()) > 0 {
		log.Println("target isn't empty:", name)
		return syscall.EIO
	}
	fpath := filepath.Join(n.key, name) + "/"
	log.Println("Rmdir:", fpath)
	_, err := n.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(n.bucket),
		Key:    aws.String(fpath),
	})
	if err != nil {
		log.Println("Rmdir failed:", err)
		return syscall.EIO
	}
	return fs.OK
}

func (n *Node) CopyFileRange(ctx context.Context, fhIn fs.FileHandle,
	offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64,
	length uint64, flags uint64) (uint32, syscall.Errno) {
	lfIn, ok := fhIn.(*File)
	if !ok {
		return 0, syscall.EIO
	}
	lfOut, ok := fhOut.(*File)
	if !ok {
		return 0, syscall.EIO
	}
	log.Println("CopyFileRange:", lfIn.key, lfOut.key, length)
	_, err := n.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(n.bucket),
		Key:        aws.String(lfOut.key),
		CopySource: aws.String(n.bucket + "/" + lfIn.key),
	})
	if err != nil {
		log.Println("CopyObject failed:", err)
		return 0, syscall.EIO
	}
	return uint32(length), fs.OK
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	fpath := filepath.Join(n.key, name)
	np, ok := newParent.(*Node)
	if !ok {
		return syscall.EIO
	}
	log.Println("Rename:", fpath, "->", np.key+newName)
	if _, err := n.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(n.bucket),
		CopySource: aws.String(n.bucket + "/" + fpath),
		Key:        aws.String(filepath.Join(np.key, newName)),
	}); err != nil {
		log.Println("CopyObject failed:", err)
		return syscall.EIO
	}
	if _, err := n.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(n.bucket),
		Key:    aws.String(n.bucket + "/" + fpath),
	}); err != nil {
		log.Println("DeleteObject failed:", err)
		return syscall.EIO
	}
	return fs.OK
}
