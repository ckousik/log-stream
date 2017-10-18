package logstream

import (
	"bytes"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const (
	testBufSize     = 4068
	testReaderCount = 25
)

func fillRandom(buffer []byte, n int) {
	for i := 0; i < n; i++ {
		buffer[i] = byte(rand.Intn(128))
	}
}

func TestSequentialWriteAndRead(t *testing.T) {
	stream, err := New("", "log-stream-test:")
	if err != nil {
		t.Fatal(err)
	}
	writeBuf := make([]byte, testBufSize)
	// fill the buffer with random bytes
	fillRandom(writeBuf, testBufSize)

	_, err = stream.Write(writeBuf)
	if err != nil {
		t.Fatal(err)
	}
	// There is no way close returns anything but nil
	_ = stream.Close()

	readBuf, data := []byte{}, []byte{0}
	reader, err := stream.NewReader()
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.Read(data)
	for err != io.EOF {
		readBuf = append(readBuf, data...)
		_, err = reader.Read(data)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
	}

	if !bytes.Equal(readBuf, writeBuf) {
		t.Fatal("Bytes not equal")
	}
}

func TestMultipleReaders(t *testing.T) {
	stream, err := New("", "log-stream-test:concurrent:")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	writeResult := []byte{}
	writer := func() {
		for i := 0; i < 10; i++ {
			writeData := make([]byte, 1024)
			fillRandom(writeData, 1024)
			_, err := stream.Write(writeData)
			if err != nil {
				wg.Done()
				t.Fatal(err)
			}
			writeResult = append(writeResult, writeData...)
			time.Sleep(200 * time.Millisecond)
		}
		_ = stream.Close()
		wg.Done()
	}
	reader := func() {
		readResult, data := []byte{}, make([]byte, 2048)
		reader, err := stream.NewReader()
		if err != nil {
			wg.Done()
			t.Fatal(err)
		}
		n, err := reader.Read(data)
		for err != io.EOF {
			readResult = append(readResult, data[:n]...)
			n, err = reader.Read(data)
			if err != nil && err != io.EOF {
				wg.Done()
				t.Fatal(err)
			}
		}
		defer wg.Done()
		if !bytes.Equal(writeResult, readResult) {
			t.Fatal("bytes not equal")
		}
		// t.Logf("WriteBuf length: %d, ReadBuf Length: %d\n", len(writeResult), len(readResult))
	}

	for i := 0; i <= testReaderCount; i++ {
		wg.Add(1)
		if i == 0 {
			go writer()
		} else {
			// Shift the start time of various readers to ensure the whole log is read
			// regardless of when the reader starts reading
			time.Sleep(300 * time.Millisecond)
			go reader()
		}
	}
	wg.Wait()
}

func TestReadWriteSync(t *testing.T) {
	stream, err := New("", "log-stream-test:sync:")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	done := make(chan struct{}, 1)

	writeResult := []byte{}
	writer := func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			writeData := make([]byte, 1024)
			fillRandom(writeData, 1024)
			_, err := stream.Write(writeData)
			if err != nil {
				t.Fatal(err)
			}
			<-done
			writeResult = append(writeResult, writeData...)
		}
		_ = stream.Close()
	}

	reader := func() {
		defer wg.Done()
		resultBuf, data := []byte{}, make([]byte, 2048)
		reader, err := stream.NewReader()
		if err != nil {
			t.Fatal(err)
		}

		var n int
		for err != io.EOF {
			n, err = reader.Read(data)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			done <- struct{}{}
			resultBuf = append(resultBuf, data[:n]...)
		}
		if !bytes.Equal(resultBuf, writeResult) {
			t.Fatal("bytes not equal")
		}
	}

	wg.Add(2)
	go reader()
	go writer()
	wg.Wait()
}
