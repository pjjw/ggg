package main

import (
	"os"
	"io"
	"fmt"
	"log"
	"xml"
	"flag"
	"net"
	"time"
	"strings"
)

type GangliaXml struct {
	XMLName xml.Name `xml:"GANGLIA_XML"`
	Grid    []Grid
	Cluster []Cluster
}

type Grid struct {
	XMLName xml.Name `xml:"GRID"`
	Cluster []Cluster
}

type Cluster struct {
	XMLName xml.Name
	Host []Host
}

type Host struct {
	XMLName xml.Name `xml:"HOST"`
	Name    string   `xml:"attr"`
	Ip      string   `xml:"attr"`
	Metric []Metric
}

type Metric struct {
	XMLName   xml.Name       `xml:"METRIC"`
	Name      string         `xml:"attr"`
	Val       string         `xml:"attr"`
	Type      string         `xml:"attr"`
	Units     string         `xml:"attr"`
	TN        string         `xml:"attr"`
	TMax      string         `xml:"attr"`
	DMax      string         `xml:"attr"`
	Slope     string         `xml:"attr"`
	ExtraData []ExtraElement `xml:"extra_data>extra_element"`
}

type ExtraElement struct {
	XMLName xml.Name `xml:"EXTRA_ELEMENT"`
	Name    string   `xml:"attr"`
	Val     string   `xml:"attr"`
}

var ganglia_addr = flag.String("ganglia_addr", "localhost:8649", "ganglia address")
var carbon_addr = flag.String("carbon_addr", "localhost:2003", "carbon address")
var metric_prefix = flag.String("prefix", "ggg.", "prefix for metric names")
var timestamp = time.Seconds()

var runeMap = map[int]int{
	46: 95, // '.' -> '_'
}

func graphiteStringMap(rune int) (ret int) {
	ret, ok := runeMap[rune]
	if !ok {
		ret = rune
	}
	return
}

func readXmlFromFile(in io.Reader) (gmeta GangliaXml, err os.Error) {
	p := xml.NewParser(in)
	p.CharsetReader = CharsetReader

	gmeta = GangliaXml{}
	err = p.Unmarshal(&gmeta, nil)
	return
}

func printClusterMetrics(out io.Writer, cl *Cluster, ret chan int) {
	ch := make(chan int)
	log.Print("Reading hosts")
	for _, hst := range cl.Host {
		log.Printf("Reading host %s", hst.Name)
		go printHostMetrics(out, hst, ch)
	}
	for _ = range cl.Host {
		<-ch
	}
	ret <- 1
}

func printHostMetrics(out io.Writer, h Host, ret chan int) {
	ch := make(chan int)
	log.Printf("Reading %s metrics", h.Name)
	for _, m := range h.Metric {
		go printMetric(out, strings.Map(graphiteStringMap, h.Name), m, ch)
	}
	// drain the channel
	for _ = range h.Metric {
		<-ch
	}
	ret <- 1
}

func printMetric(out io.Writer, host string, m Metric, ret chan int) {
	if m.Type != "string" {
		fmt.Fprintf(out, "%s%s.%s %s %d\n", *metric_prefix, host, m.Name, m.Val, timestamp)
	}
	ret <- 1
}

func main() {
	flag.Parse()

	// open connection to ganglia
	ganglia_conn, err := net.Dial("tcp", *ganglia_addr)
	if err != nil {
		log.Fatal("Dial ganglia: ", err)
	}
	defer ganglia_conn.Close()

	// open connection to carbon-agent
	carbon_conn, err := net.Dial("tcp", *carbon_addr)
	if err != nil {
		log.Fatal("Dial: ", err)
	}
	defer carbon_conn.Close()

	// read xml into memory
	gmeta, err := readXmlFromFile(ganglia_conn)
	if err != nil {
		log.Fatal("xml.unmarshal: ", err)
	}

	c := make(chan int)

	// dispatch goroutines
	for _, cl := range gmeta.Cluster {
		log.Print("Reading clusters")
		/* log.Printf("Cluster %s: %#v\n", cl.Name, cl)*/
		go printClusterMetrics(carbon_conn, &cl, c)
	}

	for _, gr := range gmeta.Grid {
		for _, cl := range gr.Cluster {
			go printClusterMetrics(carbon_conn, &cl, c)
		}
		// drain the channel for last grid
		for _ = range gr.Cluster {
			<-c
		}
	}
	// drain channel for clusters outside grid statements
	for _ = range gmeta.Cluster {
		<-c
	}
}
