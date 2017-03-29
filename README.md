# s3proxy

This tool is intended to provide a local cache for S3 objects. It is primarily
focused on caching fewer, large objects. To that end it utilizes a memory
[LRU cache](http://github.com/karlseguin/ccache) which allows for frequently
accessed files to be efficiently cached in memory. It also attempts to avoid
the 'thundering herd' problem where multiple client cache misses cause multiple
upstream requests.

### Usage

The following options are available:

```
Usage of ./s3proxy:
  -c string
    	cache directory (default ".")
  -m int
    	size of in-memory cache (in MB) (default 1000)
  -p int
    	port to listen on (default 8080)
  -r string
    	region to use (default "us-west-2")
  -t int
    	time before objects are re-validated (in seconds) (default 600)
```

### Building

3rd party dependencies are vendored using [govendor](http://github.com/kardianos/govendor). Install with:

```
go get github.com/kardianos/govendor
```

Once this repo is cloned, you can build with the following:

```
cd src/s3proxy
govendor sync
cd -
go build -o s3proxy s3proxy/cmd
```

### Running as a systemd service

To use under `systemd` a simple service file is provided in `etc/`. Adjust any necessary options in the file, and then do the following as root:

```
cp etc/s3proxy.service /etc/systemd/system/
systemctl enable s3proxy.service
systemctl start s3proxy.service
systemctl status s3proxy.service
```

Output is managed by `systemd`. To view the output use:

```
journalctl -u s3proxy.service
```
