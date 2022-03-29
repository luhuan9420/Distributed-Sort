package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"

	"gopkg.in/yaml.v2"

	//"flag"
	"bytes"
	"encoding/binary"
	"math"
	"net"
)

var serversConn = make(map[int]net.Conn)
var records = make([][]byte, 0)
var lock sync.Mutex

var wg_dial sync.WaitGroup
var wg_read sync.WaitGroup

// var wg_send sync.WaitGroup

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort

		read the input file,
		appropriately partition the data,
		send relevant data to peer servers,
		receive data from peers,
		sort the data,
		write the sorted data to the output file
	*/

	var numServers = len(scs.Servers)
	fmt.Println("Number of servers: ", numServers)

	serverInfo := scs.Servers[serverId]
	laddr := fmt.Sprintf("%v:%v", serverInfo.Host, serverInfo.Port)

	//creates server
	go listenForData(laddr, numServers)

	// let all other peers connect to server
	for i := 0; i < numServers; i++ {
		if i == serverId {
			continue
		}
		serverInfo = scs.Servers[i]
		laddr = fmt.Sprintf("%v:%v", serverInfo.Host, serverInfo.Port)
		wg_dial.Add(1)
		go handleDial(i, laddr)

	}

	// read records from file
	fmt.Println("Reading from file ", os.Args[2])
	f_in, err := ioutil.ReadFile(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	size := len(f_in) / 100
	wg_dial.Wait()
	for i := 0; i < size; i++ {
		line := f_in[i*100 : (i+1)*100]
		receiver := findId(numServers, line)
		// send records to servers based on first n bits
		// (based on how many servers we have)
		if receiver != serverId {
			wg_dial.Wait()
			// wg_send.Add(1)
			// go sendData(receiver, line)
			fmt.Println("Sending record to host ", receiver)
			_, err := serversConn[receiver].Write(line)
			if err != nil {
				log.Panicln(err)
			}
		} else {
			lock.Lock()
			records = append(records, line)
			lock.Unlock()
		}
	}
	// wg_send.Wait()
	// close servers once all the data are sent
	for _, server := range serversConn {
		server.Close()
	}

	wg_read.Wait()
	// sort records for this record
	sort.SliceStable(records, func(i, j int) bool {
		return bytes.Compare(records[i][:10], records[j][:10]) < 0
	})

	// write record to output file
	f_out, err := os.Create(os.Args[3])

	if err != nil {
		log.Fatal(err)
	}
	defer f_out.Close()

	for _, s := range records {
		_, err := f_out.Write(s)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// func sendData(id int, record []byte) {
// 	fmt.Println("Sending record to host ", id)
// 	_, err := serversConn[id].Write(record)
// 	if err != nil {
// 		log.Panicln(err)
// 	}
// 	wg_send.Done()
// }

// go routine for sending data
func handleDial(index int, laddr string) {
	c, err2 := net.Dial("tcp", laddr)

	for err2 != nil {
		c, err2 = net.Dial("tcp", laddr)
	}

	fmt.Println("Successfully connected to ", c.RemoteAddr().String())
	lock.Lock()
	serversConn[index] = c
	lock.Unlock()
	wg_dial.Done()
}

// go routine for listening incoming request
func listenForData(laddr string, size int) {
	l, err1 := net.Listen("tcp", laddr)
	if err1 != nil {
		log.Panicln(err1)
	}
	fmt.Println("Listening to connections on ", l.Addr().String())
	defer l.Close()
	// wait to accept the connection request
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Panicln(err)
			continue
		}
		// keep track of how many servers have connected already
		// count += 1
		fmt.Printf("Recieved a new connections from " + conn.RemoteAddr().String())
		wg_read.Add(1)
		go handleReadingRequest(conn)
	}
}

// determine which server a record is belong to based on the first
// n bits of records
func findId(serverNum int, record []byte) int {
	bits := int(math.Log2(float64(serverNum)))
	id := int(binary.BigEndian.Uint32(record))
	id = id >> (32 - bits)
	return id
}

// read data sent from other servers
func handleReadingRequest(conn net.Conn) {
	fmt.Println("Accepted new connection: ", conn.RemoteAddr().String())
	defer conn.Close()
	defer fmt.Println("Closed connection: ", conn.RemoteAddr().String())

	// read data from connected servers
	for {
		// reach 1 record (100 bytes)
		buf := make([]byte, 100)
		size, err := conn.Read(buf)

		// if a server closer, err will rise when try to read data
		if err != nil {
			fmt.Println(err)
			wg_read.Done()
			return
		}
		fmt.Printf("Server received %d bytes\n", size)
		lock.Lock()
		records = append(records, buf[:size])
		lock.Unlock()
	}
}
