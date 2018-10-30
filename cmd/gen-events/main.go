package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/melan/gen-events/events_generator"
	"github.com/melan/gen-events/output"
	"github.com/melan/gen-events/pipeline"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Output string

const (
	KinesisOutput = "kinesis"
	FileOutput    = "file"
)

type config struct {
	configFile    string
	caseIds       []events_generator.Case
	orgSize       events_generator.OrgSize
	orgsCount     int
	listenAddr    string
	cleanupOnExit bool
	debugEvents   bool
	output        Output
	outDir        string
	tags          map[string]*string
	dryRun        bool
	interval      int
}

func main() {
	cfg := parseArgs()
	log.Printf("Initializing orgs with the following configuration: %#v", cfg)

	var publisherFactory output.PublisherFactory
	if cfg.output == KinesisOutput {
		sess, err := session.NewSession()
		if err != nil {
			log.Panicf("can't create new AWS session. Error: %s", err.Error())
		}
		kinesisClient := kinesis.New(sess)
		publisherFactory = output.CreateKinesisPublisherFactory(kinesisClient, cfg.tags)
	} else {
		publisherFactory = output.CreateFilePublisherFactory(cfg.outDir)
	}

	// generate orgs
	orgs := make([]*events_generator.Org, 0, cfg.orgsCount*len(cfg.caseIds))
	var orgId = 1
	for _, caseId := range cfg.caseIds {
		if cfg.orgsCount == 1 {
			org := events_generator.GenerateOrg(fmt.Sprintf("%d", orgId), cfg.orgSize, caseId, cfg.debugEvents)
			orgs = append(orgs, org)
			orgId++
		} else {
			for j := 0; j < cfg.orgsCount; j++ {
				orgSize := events_generator.GuessOrgSize()
				org := events_generator.GenerateOrg(fmt.Sprintf("%d", orgId), orgSize, caseId, cfg.debugEvents)
				orgs = append(orgs, org)
				orgId++
			}
		}
	}

	mainContext, mainCancel := context.WithCancel(context.Background())

	log.Printf("creating events generators for %d orgs", len(orgs))
	g := &sync.WaitGroup{}
	cleanups := make([]pipeline.CleanupFunc, 0, len(orgs))
	abort := false

	for _, org := range orgs {
		if !cfg.dryRun {
			log.Printf("launching events generator for %s of org %s", org.StreamName(), org.OrgId)
			log.Printf("creating publisher for %s", org.OrgId)
			publisher := publisherFactory(org)
			if err := publisher.Init(); err != nil {
				log.Printf("can't provision publisher because of an error %s", err.Error())
				abort = true
				mainCancel()
				break
			} else {
				cleanups = append(cleanups, publisher.Cleanup)
			}

			log.Printf("creating generator for %s", org.OrgId)
			pump := pipeline.NewPipeline(publisher, org, time.Duration(cfg.interval)*time.Second)
			pipelineContext, _ := context.WithCancel(mainContext)
			go func(ctx context.Context, pump *pipeline.Pipeline, g *sync.WaitGroup) {
				g.Add(1)
				if cfg.cleanupOnExit {
					log.Printf("adding cleanup for %s", pump.OrgId)
					defer pump.Cleanup(g)
				} else {
					defer g.Done()
				}

				pump.Pump(ctx)
			}(pipelineContext, pump, g)
		} else {
			log.Printf("skipping launch of the events generator because of dry run")
		}
	}

	if abort {
		log.Println("initialization was aborted. Exiting")
		g.Wait()
		os.Exit(1)
	}

	log.Printf("enabling metrics endpoint")
	http.Handle("/metrics", promhttp.Handler())
	g.Add(1)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func(sigs chan os.Signal, cancel context.CancelFunc) {
		<-sigs
		log.Printf("received terminate signal. stopping the party")
		cancel()
	}(sigs, mainCancel)

	server := &http.Server{
		Addr:    cfg.listenAddr,
		Handler: nil,
	}

	httpContext, _ := context.WithCancel(mainContext)
	go func(ctx context.Context, server *http.Server, g *sync.WaitGroup) {
		defer g.Done()
		<-ctx.Done()
		log.Printf("shutting down http server")
		server.Shutdown(context.Background())
	}(httpContext, server, g)

	log.Print("prime time")
	server.ListenAndServe()
	g.Wait()
	log.Println("bye bye")
}

func parseArgs() config {
	cfg := config{}

	a := kingpin.New(filepath.Base(os.Args[0]), "Generator of platform events")
	a.HelpFlag.Short('h')

	//a.Flag("config-file", "Configuration file path.").
	//	Default("gen-platform-events.yml").StringVar(&cfg.configFile)

	a.Flag("listen-address", "Address where prometheus /metrics endpoint will be available").
		Default(":8080").StringVar(&cfg.listenAddr)

	a.Flag("orgs-count", "Number of different Orgs to generate").
		Default("1").IntVar(&cfg.orgsCount)

	a.Flag("cleanup", "Cleanup Kinesis streams on exit").
		Default("false").BoolVar(&cfg.cleanupOnExit)

	a.Flag("debug-events", "Output additional debug info from events generators").
		Default("false").BoolVar(&cfg.debugEvents)

	a.Flag("dry-run", "Parse parameters and really do nothing else").
		Default("false").BoolVar(&cfg.dryRun)

	a.Flag("interval", "Interval in seconds between metrics generation cycles").
		Default("60").IntVar(&cfg.interval)

	var outputDestination string
	a.Flag("output", "Destination for output").
		Default(string(KinesisOutput)).
		EnumVar(&outputDestination,
			string(KinesisOutput),
			string(FileOutput))

	var outDir string
	a.Flag("output-path", "Path to output file").
		Default("").StringVar(&outDir)

	var caseIds []string
	a.Flag("case-id", "Id of the test scenario to run").
		Default(string(events_generator.CaseOne)).
		EnumsVar(&caseIds,
			string(events_generator.CaseOne),
			string(events_generator.CaseTwo),
			string(events_generator.CaseThree),
			string(events_generator.CaseFour),
			string(events_generator.CaseFive),
		)

	var orgSize string
	a.Flag("org-size", "Size of the Org").
		Default(string(events_generator.TinyOrg)).
		EnumVar(&orgSize,
			string(events_generator.TinyOrg),
			string(events_generator.SmallOrg),
			string(events_generator.MediumOrg),
			string(events_generator.LargeOrg))

	var tagsPairs []string
	a.Flag("tag", "Tag pair delimited by `=`. Can be used multiple times").StringsVar(&tagsPairs)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	if len(caseIds) > 0 {
		cases := make(map[events_generator.Case]bool, len(caseIds))
		for _, caseIdName := range caseIds {
			caseId := events_generator.Case(caseIdName)
			cases[caseId] = true
		}

		for k := range cases {
			cfg.caseIds = append(cfg.caseIds, k)
		}
	} else {
		log.Printf("No cases were defined, will go with the 1st one")
		cfg.caseIds = []events_generator.Case{events_generator.CaseOne}
	}

	if orgSize != "" {
		cfg.orgSize = events_generator.OrgSize(orgSize)
	}

	if outputDestination != "" {
		cfg.output = Output(outputDestination)
	}

	if cfg.output == FileOutput {
		if outDir != "" {
			absPath, err := filepath.Abs(outDir)
			if err != nil {
				log.Fatalf("can't resolve path to output directory %s. Error: %s", outDir, err)
			} else {
				cfg.outDir = absPath
			}
		} else {
			absPath, err := os.Getwd()
			if err != nil {
				log.Fatalf("can't resolve current work directory and --output-path is unset. Error: %s", err)
			} else {
				cfg.outDir = absPath
			}
		}
	}

	cfg.tags = make(map[string]*string)
	if tagsPairs != nil && len(tagsPairs) > 0 {
		for _, tagPair := range tagsPairs {
			split := strings.Split(tagPair, "=")
			if len(split) != 2 {
				log.Printf("can't parse tag %s. Skipping", tagPair)
				continue
			}

			cfg.tags[split[0]] = &split[1]
		}
	}

	return cfg
}
