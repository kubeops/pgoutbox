/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kubeops.dev/pgoutbox/apis"
	"kubeops.dev/pgoutbox/internal/listener"
	"kubeops.dev/pgoutbox/internal/listener/transaction"

	scfg "github.com/ihippik/config"
	"github.com/urfave/cli/v2"
)

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"v"},
		Usage:   "print only the version",
	}

	version := scfg.GetVersion()

	app := &cli.App{
		Name:    "PgOutbox",
		Usage:   "listen PostgreSQL events",
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Value:   "config_example.yml",
				Aliases: []string{"c"},
				Usage:   "path to config file",
			},
		},
		Action: func(c *cli.Context) error {
			ctx, cancel := signal.NotifyContext(c.Context, syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			cfg, err := apis.InitConfig(c.String("config"))
			if err != nil {
				return fmt.Errorf("get config: %w", err)
			}

			if err = cfg.Validate(); err != nil {
				return fmt.Errorf("validate config: %w", err)
			}

			if err = scfg.InitSentry(cfg.Monitoring.SentryDSN, version); err != nil {
				return fmt.Errorf("init sentry: %w", err)
			}

			logger := scfg.InitSlog(cfg.Logger, version, cfg.Monitoring.SentryDSN != "")

			go scfg.InitMetrics(cfg.Monitoring.PromAddr, logger)

			conn, rConn, err := initPgxConnections(cfg.Database, logger, time.Minute*30)
			if err != nil {
				return fmt.Errorf("pgx connection: %w", err)
			}

			if err = configureReplicaIdentityToFull(conn, cfg.Listener.Filter); err != nil {
				return fmt.Errorf("configure replica identity: %w", err)
			}
			pub, err := factoryPublisher(ctx, cfg.Publisher, logger)
			if err != nil {
				return fmt.Errorf("factory publisher: %w", err)
			}

			defer func() {
				if err := pub.Close(); err != nil {
					slog.Error("close publisher failed", "err", err.Error())
				}
			}()

			svc := listener.NewWalListener(
				cfg,
				logger,
				listener.NewRepository(conn),
				rConn,
				pub,
				transaction.NewBinaryParser(logger, binary.BigEndian),
				apis.NewMetrics(),
			)

			go svc.InitHandlers(ctx)

			if err = svc.Process(ctx); err != nil {
				slog.Error("service process failed", "err", err.Error())
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("service error", "err", err)
	}
}
