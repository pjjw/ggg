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
  XMLName xml.Name "GANGLIA_XML"
  Version string "attr"
  Source  string "attr"
  Grid []Grid
  Cluster []Cluster
}

type Grid struct {
  XMLName   xml.Name  "GRID"
  Name      string    "attr"
  Authority string    "attr"
  Localtime string    "attr"
  Cluster   []Cluster
}

type Cluster struct {
  XMLName xml.Name
  Name    string "attr"
  LocalTime string "attr"
  Owner string "attr"
  Host []Host
}

type Host struct {
  XMLName xml.Name "HOST"
  Name string "attr"
  Ip string "attr"
  Reported string "attr"
  TN string "attr"
  TMax string "attr"
  DMax string "attr"
  Location string "attr"
  Gmond_Started string "attr"
  Metric []Metric
}

type Metric struct {
  XMLName xml.Name "METRIC"
  Name string "attr"
  Val string "attr"
  Type string "attr"
  Units string "attr"
  TN string "attr"
  TMax string "attr"
  DMax string "attr"
  Slope string "attr"
  ExtraData []ExtraElement "extra_data>extra_element"
}

type ExtraElement struct {
  XMLName xml.Name "EXTRA_ELEMENT"
  Name string "attr"
  Val string "attr"
}

var ganglia_addr = flag.String("ganglia_addr", "localhost:8649", "ganglia address")
var carbon_addr = flag.String("carbon_addr", "localhost:2003", "carbon address")
var metric_prefix = flag.String("prefix", "ggg.", "prefix for metric names")

func ReadXmlFromFile(in io.Reader) (gmeta GangliaXml, err os.Error) {
  p := xml.NewParser(in)
  p.CharsetReader = CharsetReader

  gmeta = GangliaXml{}
  err = p.Unmarshal(&gmeta, nil)
  return
}

func graphiteStringMap(rune int) (ret int) {
  if rune == 46 { // 46 == '.'
    ret = 95 // 95 == '_'
  } else { ret = rune }
  return
}

func PrintClusterMetrics(out io.Writer, cl *Cluster, ret chan int) {
  ch := make(chan int)
  log.Print("Reading hosts")
  for _,hst := range cl.Host {
    log.Printf("Reading host %s", hst.Name)
    go PrintHostMetrics(out, hst, ch)
  }
  for _ = range cl.Host {
    <-ch
  }
  ret <- 1
}

func PrintHostMetrics(out io.Writer, h Host, ret chan int) {
  ch := make(chan int)
  log.Printf("Reading %s metrics", h.Name)
  for _,m := range h.Metric {
    go PrintMetric(out, strings.Map(graphiteStringMap, h.Name), m, ch)
  }
  // drain the channel
  for _ = range h.Metric {
    <-ch
  }
  ret <- 1
}

func PrintMetric(out io.Writer, host string, m Metric, ret chan int) {
  if m.Type != "string" {
    fmt.Fprintf(out, "%s%s.%s %s %d\n", *metric_prefix, host, m.Name, m.Val, time.Seconds())
  }
  ret <- 1
}

func main () {
  flag.Parse()

  // open connection to ganglia
  ganglia_conn,err := net.Dial("tcp", *ganglia_addr)
  if err != nil {
    log.Fatal("Dial ganglia: ", err)
  }
  defer ganglia_conn.Close()

  // open connection to carbon-agent
  carbon_conn,err := net.Dial("tcp", *carbon_addr)
  if err != nil {
    log.Fatal("Dial: ", err)
  }
  defer carbon_conn.Close()

  // read xml into memory
  gmeta,err := ReadXmlFromFile(ganglia_conn)
  if err != nil {
    log.Fatal("xml.unmarshal: ", err)
  }

  c := make(chan int)

  // dispatch goroutines
  for _,cl := range gmeta.Cluster {
    log.Print("Reading clusters")
    /* log.Printf("Cluster %s: %#v\n", cl.Name, cl)*/
    go PrintClusterMetrics(carbon_conn, &cl, c)
  }

  for _,gr := range gmeta.Grid {
    for _,cl := range gr.Cluster {
      go PrintClusterMetrics(carbon_conn, &cl, c)
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

