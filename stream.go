package logstream

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

type LogStream struct {
	tempFile *os.File
	// close when writer is closed
	done chan struct{}

	// broadcast for state change -> new data / completed
	c        *sync.Cond
	cm       sync.Mutex
	fileName string
}

type LogStreamReader struct {
	c    *sync.Cond
	done chan struct{}
	file *os.File
}

func New(dir, prefix string) (*LogStream, error) {
	tfile, err := ioutil.TempFile(dir, prefix)
	if err != nil {
		return nil, err
	}

	l := &LogStream{
		tempFile: tfile,
		done:     make(chan struct{}, 1),
	}
	l.c = sync.NewCond(&l.cm)
	l.fileName = l.tempFile.Name()
	return l, nil
}

func (l *LogStream) Write(buffer []byte) (int, error) {
	// hold mutex while writing
	select {
	case <-l.done:
		return 0, errors.New("write to closed stream")
	default:
	}
	n, err := l.tempFile.Write(buffer)
	if err != nil {
		defer func() {
			// Close will broadcast state change
			_ = l.Close()
		}()
		return 0, err
	}
	defer l.c.Broadcast()
	return n, err
}

func (l *LogStream) Close() error {
	select {
	case <-l.done:
		return nil
	default:
	}
	defer l.c.Broadcast()
	close(l.done)
	return nil
}

func (l *LogStream) NewReader() (io.Reader, error) {
	file, err := os.Open(l.fileName)
	if err != nil {
		return nil, err
	}
	return &LogStreamReader{
		c:    l.c,
		done: l.done,
		file: file,
	}, nil
}

func (lr *LogStreamReader) Read(buffer []byte) (int, error) {
	for {
		n, err := lr.file.Read(buffer)
		if err != nil && err != io.EOF {
			return n, err
		}
		if n > 0 {
			return n, nil
		}
		// at this point, n is 0 and err is either nil or io.EOF
		select {
		case <-lr.done:
			return 0, io.EOF
		default:
		}
		lr.c.L.Lock()
		lr.c.Wait()
		lr.c.L.Unlock()
	}
	// don't check for EOF if you actually read some bytes
}
