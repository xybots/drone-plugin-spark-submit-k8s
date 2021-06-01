package main

import (
	"fmt"
	"os"

	"encoding/json"

	"strings"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	AppName    = "Spark Submit Plugin"
	AppUsage   = ""
	AppVersion = "0.2.1"
)

func main() {

	app := cli.NewApp()
	app.Name = AppName
	app.Usage = AppUsage
	app.Action = run
	app.Version = fmt.Sprintf("%s", AppVersion)

	app.Flags = []cli.Flag{

		cli.StringFlag{
			Name:   "plugin.spark.submit.options",
			Usage:  "Spark submit options",
			EnvVar: "PLUGIN_SPARK_SUBMIT_OPTIONS",
		},
		cli.StringFlag{
			Name:   "plugin.spark.submit.configs",
			Usage:  "Spark submit spark configs",
			EnvVar: "PLUGIN_SPARK_SUBMIT_CONFIGS",
		},
		cli.StringFlag{
			Name:   "plugin.spark.submit.app_args",
			Usage:  "Spark submit application arguments",
			EnvVar: "PLUGIN_SPARK_SUBMIT_APP_ARGS",
		},
	}
	app.Run(os.Args)
}

func run(c *cli.Context) {

	plugin := Plugin{
		Config: Config{
			SubmitOptions: ProcessPluginJSONInput(c.String("plugin.spark.submit.options")),
			SparkConfig:   ProcessPluginJSONInput(c.String("plugin.spark.submit.configs")),
			AppArgs:       strings.Split(c.String("plugin.spark.submit.app_args"), ","),
			Env:           pluginEnv(),
		},
	}

	if err := plugin.Exec(); err != nil {
		log.Fatalf("plugin execution failed with the error: %s", err)
		os.Exit(1)
	}
}

// ProcessPluginJSONInput reads properties holding JSON data and transforms them into a map
func ProcessPluginJSONInput(jsonStr string) map[string]string {
	var keyValues map[string]string

	if jsonStr != "" {
		err := json.Unmarshal([]byte(jsonStr), &keyValues)
		if err != nil {
			logrus.Fatalf("Unable to parse values: %+v", err)
		}
		logrus.Debugf("Map values %#v", keyValues)
	}
	return keyValues
}

func pluginEnv() map[string]string {
	pluginEnv := map[string]string{}
	for _, envVar := range os.Environ() {
		keyVal := strings.SplitN(envVar, "=", 2)
		pluginEnv[keyVal[0]] = keyVal[1]
	}
	logrus.Debugf("plugin env map: %s", pluginEnv)
	return pluginEnv
}
