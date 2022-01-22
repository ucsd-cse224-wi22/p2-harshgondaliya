package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"
	"gopkg.in/yaml.v2"
)
const RecordSize = 100

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

type record struct {
	data []byte
}

type fileRecords []record
var myFileRecords fileRecords

func (f fileRecords) Len() int {
	return len(f)
}

func (f fileRecords) Swap(i, j int){
	temp_i := make([]byte, 100)
	temp_j := make([]byte, 100)
	copy(temp_i, f[i].data)
	copy(temp_j, f[j].data)
	copy(f[i].data, temp_j)
	copy(f[j].data, temp_i)
}

func (f fileRecords) Less(i, j int) bool {
    return bytes.Compare(f[i].data, f[j].data) < 0
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
/*
client need to worry about synchronisation while sending data
client just sends data to the connection
server is having a separate thread to handle each connection
Dedicated threads will accept data from each connection but when it comes to saving those received data to shared data structure channels come into picture
*/

/*
If your struct happens to include arrays, slices, or pointers, 
then you'll need to perform a deep copy of the referenced objects 
unless you want to retain references between copies. 
Golang provides no builtin deep copy functionality so you'll have 
to implement your own or use one of the many freely available libraries that provide it.
*/

/*
Learnings:
first parition data locally and then start listening to connections and sending data.
while paritioning data rather than thinking of 2d byte slice think of storing a huge buffer for each server.
then you can keep reading 100B records from this buffer.
hence hashmap map[int][]byte would be great option
it will avoid to loss of records we are experiencing in large file sizes

other way is to use io.ReadFull and ensure that we always read 100B
*/
func readData() fileRecords{
	var readFileRecords fileRecords
	inputFile, err := os.Open(os.Args[2])
	if err != nil{
		log.Fatal(err)
	}
	defer inputFile.Close()
	buf := make([]byte, RecordSize)
	for {
		bytesRead, err := inputFile.Read(buf)
		if err != nil{
			if err != io.EOF { // even though the last chunk may be less than 100B, the last chunk is read successfully and then when solely EOF is encountered, we get error.EOF
				log.Panicln(err)
			}
			break
		}
		if bytesRead != 100{
			log.Fatal("didnt read 100B from file")
		}
		copy_buf := make([]byte, RecordSize)
		copy(copy_buf, buf) // do not pass buf every time. otherwise, you keep overwriting buf and your slice has memory ref to it. 
						 // hence, if buf is appended each time, at the end all inputFileRecords will have value equal to last entered element.
		readFileRecords = append(readFileRecords, record{copy_buf[:bytesRead]})
	}
	return readFileRecords
}
func partitionData(numOfServers int, readFileRecords fileRecords)map[int]fileRecords{
	partitionedFileRecords := make(map[int]fileRecords)
	for i:= 0; i<numOfServers; i++{
		partitionedFileRecords[i] = fileRecords{}
	}
	n_partitionBits := int(math.Log2(float64(numOfServers))) // assumed that fewer than 256 servers there. Otherwise, endianness needs to be taken care of 
	for i:=0; i<len(readFileRecords); i++{
		copy_buf := make([]byte, RecordSize)
		copy(copy_buf, readFileRecords[i].data) // do not pass buf every time. otherwise, you keep overwriting buf and your slice has memory ref to it. 
						 // hence, if buf is appended each time, at the end all inputFileRecords will have value equal to last entered element.
		serverKey := uint8(copy_buf[0] >> (8 - n_partitionBits))
		partitionedFileRecords[int(serverKey)] = append(partitionedFileRecords[int(serverKey)], record{copy_buf[:100]})
	}
	return partitionedFileRecords	
}
func openConnections(scs ServerConfigs, myServerId int) map[int]net.Conn{ // I only want to send records through these connections
	openConnectionsMap := make(map[int]net.Conn)
	for i:=0;i<len(scs.Servers);i++ {
		if i != myServerId { // dial to all servers except myself
			var c net.Conn
			for j:=0;j<5;j++{ // keep retrying for 5 seconds
				var err error
				var addr string = scs.Servers[i].Host + ":" + scs.Servers[i].Port
				c, err = net.Dial("tcp", addr)
				if err == nil { 
					break
				}
				time.Sleep(1 * time.Second)
			}
			if c != nil{
				openConnectionsMap[scs.Servers[i].ServerId] = c
				log.Println("Server" + strconv.Itoa(myServerId) + " successfully connected to " + strconv.Itoa(scs.Servers[i].ServerId))
			} else {
				log.Println("Server" + strconv.Itoa(myServerId) + " unable to connect to " + strconv.Itoa(scs.Servers[i].ServerId))
			}
		}
	}
	return openConnectionsMap
}

func listenForConnections(scs ServerConfigs, myServerId int, recCh chan<- []byte, finCh chan<- int){
	l, err := net.Listen("tcp", scs.Servers[myServerId].Host + ":" + scs.Servers[myServerId].Port)
	if err != nil {
		log.Panicln(err)
	}
	for{
		conn, err := l.Accept()
		if err != nil{
			log.Panicln(err)
		}
		log.Println("Accepted a connection")
		go handleConnection(conn, myServerId, recCh, finCh)
	}
}

func handleConnection(conn net.Conn, myServerId int, recCh chan<- []byte, finCh chan<- int){
	for{
		buf := make([]byte, RecordSize)
		n, err := io.ReadFull(conn, buf) // blocks until some read is done; VIMP: use io.ReadFull so that it always reads bytes equal to buf size
		if err != nil{
			if err != io.EOF{
				log.Panicln(err)
			} else {
				finCh <- 1
				break
			}
		}
		//log.Println("Server" + strconv.Itoa(myServerId) + " received a record")
		copy_buf := make([]byte, RecordSize)
		copy(copy_buf, buf[:n])
		if n!= 100 {
			log.Fatalln("not 100B", n)
		}
		recCh <- copy_buf
	}
}
func consolidateFileRecords(numOfClients int, recCh <-chan []byte, finCh <-chan int) fileRecords{
	var numOfClientsCompleted int
	var receivedFileRecords fileRecords
	numOfClientsCompleted = 0
	for{
		if numOfClientsCompleted == numOfClients{
			log.Println("hi. exiting.")
			break
		}
		select {
			case fin := <-finCh:
				log.Println("Received at finCh")	
				numOfClientsCompleted = numOfClientsCompleted + fin
				log.Println("numOfClientsCompleted", numOfClientsCompleted)	
			
			case buf := <-recCh:
				//log.Println("Received at recCh")	
				copy_buf := make([]byte, RecordSize)
				copy(copy_buf, buf)
				receivedFileRecords = append(receivedFileRecords, record{copy_buf[:100]})
				//log.Println("Added to received FR")
		}
	}
	//myFileRecords = append(myFileRecords, receivedFileRecords...)
	return receivedFileRecords
}
func sendFileRecords(conn net.Conn, fr fileRecords){
	for i:=0; i<len(fr); i++{
		n, err := conn.Write(fr[i].data)
		if err != nil{
			log.Panicln(err)
		} 
		if n != 100{
			log.Panicln("File record not multiple of 100B", n)
		}
	}
	time.Sleep(3 * time.Second)
	conn.Close()
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	myServerId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid myServerId, must be an int %v", err)
	}
	fmt.Println("My server Id:", myServerId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)
	numOfClients := len(scs.Servers) - 1
	
	// read and partition data
	
	readFileRecords := readData()
	partitionedFileRecords := partitionData(numOfClients+1, readFileRecords)
	myFileRecords = append(myFileRecords, partitionedFileRecords[myServerId]...)

	// declare channels
	recCh := make(chan []byte)
	finCh := make(chan int)

	// start accepting connections
	go listenForConnections(scs, myServerId, recCh, finCh)
	
	// open connections to all servers
	openConnectionsMap := openConnections(scs, myServerId)
	
	// send filerecords
//	sendFileRecords(openConnectionsMap, numOfClients+1, myServerId)
	for i:=0; i<numOfClients+1; i++{
		if i!=myServerId{
			go sendFileRecords(openConnectionsMap[i], partitionedFileRecords[i])
		}
	}
	// consolidate file records
	receivedFileRecords := consolidateFileRecords(numOfClients, recCh, finCh)	
	myFileRecords = append(myFileRecords, receivedFileRecords...)
	//sort file records
	sort.Stable(fileRecords(myFileRecords))
	
	// writing records one by one to the output file	
	outputFile, err := os.Create(os.Args[3])
	if err!= nil{
		log.Fatal(err)
	}
	defer outputFile.Close()
	for j:= 0; j < len(myFileRecords); j++ {
		_, err := outputFile.Write(myFileRecords[j].data)
		if err != nil{
			log.Fatal(err)
		}
	}
}
