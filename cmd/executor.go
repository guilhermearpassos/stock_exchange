package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/quickfixgo/quickfix"
	"github.com/spf13/cobra"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"stock_exchange/internal/services/order_gateway"
	"syscall"
)

var (
	ExecutorCmd = &cobra.Command{
		Use:     "ordermatch",
		Short:   "Start an order matching (FIX acceptor) service",
		Long:    "Start an order matching (FIX acceptor) service",
		Aliases: []string{"oms"},
		Example: "se ordermatch [YOUR_FIX_CONFIG_FILE_HERE.cfg] (default is ./config/ordermatch.cfg)",
		RunE:    execute,
	}
)

func execute(cmd *cobra.Command, args []string) error {
	var cfgFileName string
	argLen := len(args)
	switch argLen {
	case 0:
		{
			log.Printf("FIX config file not provided...")
			log.Printf("attempting to use default location './config/ordermatch.cfg' ...")
			cfgFileName = path.Join("config", "ordermatch.cfg")
		}
	case 1:
		{
			cfgFileName = args[0]
		}
	default:
		{
			return fmt.Errorf("incorrect argument number")
		}
	}

	cfg, err := os.Open(cfgFileName)
	if err != nil {
		return fmt.Errorf("error opening %v, %v", cfgFileName, err)
	}
	defer func(cfg *os.File) {
		_ = cfg.Close()
	}(cfg)
	stringData, readErr := io.ReadAll(cfg)
	if readErr != nil {
		return fmt.Errorf("error reading cfg: %s,", readErr)
	}

	appSettings, err := quickfix.ParseSettings(bytes.NewReader(stringData))
	if err != nil {
		return fmt.Errorf("error reading cfg: %s,", err)
	}

	logFactory, err := quickfix.NewFileLogFactory(appSettings)
	if err != nil {
		return fmt.Errorf("error creating file log factory: %s,", err)
	}
	app := order_gateway.NewApplication()

	log.Printf("acceptor", bytes.NewReader(stringData))
	acceptor, err := quickfix.NewAcceptor(app, quickfix.NewMemoryStoreFactory(), appSettings, logFactory)
	if err != nil {
		return fmt.Errorf("unable to create acceptor: %s", err)
	}

	err = acceptor.Start()
	if err != nil {
		return fmt.Errorf("unable to start FIX acceptor: %s", err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-interrupt
		acceptor.Stop()
		os.Exit(0)
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()

		//switch value := scanner.Text(); value {
		//case "#symbols":
		//	app.Display()
		//default:
		//	app.DisplayMarket(value)
		//}
	}
}
