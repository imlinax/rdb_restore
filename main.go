package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/garyburd/redigo/redis"

	"github.com/groove-x/rdb"
	"github.com/groove-x/rdb/nopdecoder"
)

var (
	flagHost = flag.String("host", "127.0.0.1:6379", "host")
	flagFile = flag.String("file", "dump.rdb", "rdb file")
)

type decoder struct {
	db int
	i  int
	nopdecoder.NopDecoder
	conn redis.Conn
}

func (p *decoder) StartDatabase(n int, offset int) {
	fmt.Printf("select db=%d \n", n)
	p.db = n
	_, err := p.conn.Do("SELECT ", n)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}

	_, err = p.conn.Do("FLUSHDB", n)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}
}

func (p *decoder) Set(key, value []byte, expiry int64) {
	fmt.Printf("db=%d %q -> %q\n", p.db, key, value)
	_, err := p.conn.Do("SET", key, value)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}
}

func (p *decoder) Hset(key, field, value []byte) {
	fmt.Printf("db=%d %q . %q -> %q\n", p.db, key, field, value)

	_, err := p.conn.Do("HSET", key, field, value)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}
}

func (p *decoder) Sadd(key, member []byte) {
	fmt.Printf("db=%d %q { %q }\n", p.db, key, member)
	_, err := p.conn.Do("SADD", key, member)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}
}

func (p *decoder) StartList(key []byte, length, expiry int64) {
	p.i = 0
}

func (p *decoder) Rpush(key, value []byte) {
	fmt.Printf("db=%d %q[%d] -> %q\n", p.db, key, p.i, value)
	p.i++

	_, err := p.conn.Do("RPUSH", key, value)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}
}

func (p *decoder) StartZSet(key []byte, cardinality, expiry int64) {
	p.i = 0
}

func (p *decoder) Zadd(key []byte, score float64, member []byte) {
	fmt.Printf("db=%d %q[%d] -> {%q, score=%g}\n", p.db, key, p.i, member, score)
	p.i++

	_, err := p.conn.Do("ZADD", key, score, member)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}

}

func maybeFatal(err error) {
	if err != nil {
		fmt.Printf("Fatal error: %s\n", err)
		os.Exit(1)
	}
}

func main() {
	flag.Parse()
	conn, err := redis.Dial("tcp", *flagHost)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	f, err := os.Open(*flagFile)
	maybeFatal(err)
	err = rdb.Decode(f, &decoder{conn: conn})
	maybeFatal(err)
}
