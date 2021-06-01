package main

import (
	"fmt"
	"os"

	"bytes"
	"github.com/progrium/go-shell"
	log "github.com/sirupsen/logrus"
	"gopkg.in/go-playground/validator.v9"
	"text/template"
	"strconv"
	"errors"
	"strings"
)

const (
	SPARK_SUBMIT_TEMPLATE_NAME = "spark-submit"
	SPARK_SUBMIT_TEMPLATE      = `{{- define "spark-submit-tpl" }}/opt/spark/bin/spark-submit --verbose --deploy-mode cluster
{{- range $key, $value := .SubmitOptions }} --{{ $key }} {{ $value }}{{ end -}}
{{- range $key, $value := .SparkConfig }} --conf {{ $key -}} = {{- $value }}{{ end -}}
{{- range $key, $value := .AppArgs }} {{ $value -}} {{ end -}} {{ end -}}
{{- template "spark-submit-tpl" . -}}
`
)

type (
	// the configuration required to set up the spark submit command
	Config struct {
		// arguments passed to the spark submit command (with --<option>)
		SubmitOptions map[string]string

		// spark configuration passed as --conf
		SparkConfig map[string]string

		// application arguments
		AppArgs []string

		// the plugin environment
		Env map[string]string
	}

	Plugin struct {
		Config Config
	}
)

var validate *validator.Validate

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
	shell.Trace = true
	shell.Shell = []string{"/bin/bash", "-c"}

}

// Exec executes the plugin logic
func (p *Plugin) Exec() error {
	log.Infof("executing [ %s ]", AppName)

	validate = validator.New()
	err := validate.Struct(p)

	if err != nil {
		for _, v := range err.(validator.ValidationErrors) {
			log.Errorf("[ %s ] field validation error (%+v)", v.Field(), v)
		}
		return nil
	}

	// handle defaults and conditional configs
	err = p.Config.decorateConfig();
	if err != nil {
		log.Errorf("error while decorating plugin configuration: %s", err)
		return err
	}

	// replace placeholders
	p.Config.ProcessTemplateConfigs()

	sparkRunCmd, err := p.Config.AssembleSparkSubmitCommand()
	if err != nil {
		log.Errorf("error while assembling spark submit command: %s", err)
		return err
	}

	log.Debugf("Spark Command: %s", sparkRunCmd)
	sparkRunResult := shell.Run(sparkRunCmd)
	log.Infof("Exit code: %d", sparkRunResult.ExitStatus)
	log.Debugf("Stdout: %s", sparkRunResult.Stdout)
	log.Debugf("Stderr: %s", sparkRunResult.Stderr)

	if sparkRunResult.ExitStatus != 0 {
		log.Errorf("spark submit returned with the code: [ %s ]", sparkRunResult.ExitStatus)
		return errors.New(fmt.Sprintf("spark submit returned with the code: [ %s ]", sparkRunResult.ExitStatus))
	}

	return nil
}

// SubmitCommandStr assembles the spark submit command string for the given configuration
func (submitConfigs *Config) AssembleSparkSubmitCommand() (string, error) {
	log.Debug("assembling the spark submit command ...")

	partTpl, err := template.New(SPARK_SUBMIT_TEMPLATE_NAME).Parse(SPARK_SUBMIT_TEMPLATE)
	if err != nil {
		log.Errorf("couldn't parse the submit command template, err: %s", err)
		return "", err
	}

	var out bytes.Buffer
	err = partTpl.ExecuteTemplate(&out, SPARK_SUBMIT_TEMPLATE_NAME, submitConfigs)
	if err != nil {
		log.Errorf("couldn't execute the submit command template, err: %s", err)
		return "", err
	}

	log.Debugf("successfully assembled the spark submit options: %s", out.String())

	return out.String(), err
}

// decorateConfig decorates the configuration with defaults or calculated values
func (config *Config) decorateConfig() error {
	log.Debugf("decorating spark submit configuration ...")

	localDeploy, ok := config.Env["PLUGIN_SPARK_KUBERNETES_LOCAL_DEPLOY"];
	if !ok {
		log.Debugf("the env var [ %s ] is not set, defaulting it to true", "PLUGIN_SPARK_KUBERNETES_LOCAL_DEPLOY")
		localDeploy = "true"
	}

	ld, err := strconv.ParseBool(localDeploy)
	if err != nil {
		log.Errorf("invalid value provided for the local deploy config: %s", err)
		return err
	}

	if ld {
		// the spark master url gets added to the options (local deploy)
		config.SubmitOptions["master"] = fmt.Sprintf("k8s://https://%s:%s", config.Env["KUBERNETES_PORT_443_TCP_ADDR"],
			config.Env["KUBERNETES_SERVICE_PORT_HTTPS"])

		log.Debugf("added --master option:[ %s ]", config.SubmitOptions["master"])
	} else {
		// keys added in case of in cluster
		config.SparkConfig["spark.kubernetes.authenticate.submission.caCertFile"] =
			config.Env["PLUGIN_SPARK_KUBERNETES_AUTHENTICATE_SUBMISSION_CACERTFILE"]

		config.SparkConfig["spark.kubernetes.authenticate.submission.clientCertFile"] =
			config.Env["PLUGIN_SPARK_KUBERNETES_AUTHENTICATE_SUBMISSION_CLIENTCERTFILE"]

		config.SparkConfig["spark.kubernetes.authenticate.submission.clientKeyFile"] =
			config.Env["PLUGIN_SPARK_KUBERNETES_AUTHENTICATE_SUBMISSION_CLIENTKEYFILE"]

		log.Debugf("added in cluster spark configs")
	}
	log.Debugf("decorating spark submit configuration ... done.")

	return nil

}

func (config *Config) ProcessTemplateConfigs() {
	processTemplateConfigs(&config.SubmitOptions, &config.Env)
	processTemplateConfigs(&config.SparkConfig, &config.Env)
	processTemplateAppArgs(config.AppArgs, &config.Env)
}

func processTemplateConfigs(configMap, pluginEnv *map[string]string) error {
	log.Debug("processing configurations given as go templates")

	for configKey, configValue := range *configMap {
		newKey := replaceValues(configKey, *pluginEnv)
		newVal := replaceValues(configValue, *pluginEnv)

		if strings.Compare(configKey, newKey) != 0 {
			delete(*configMap, configKey)
		}

		(*configMap)[newKey] = newVal
	}

	return nil
}

func processTemplateAppArgs(argList []string, pluginEnv *map[string]string) error {
	log.Debug("processing app args given as go templates")

	for index, argVal := range argList {
		argList[index] = replaceValues(argVal, *pluginEnv)
	}

	return nil
}

func replaceValues(configTpl string, pluginEnv map[string]string) string {
	log.Debugf("executing template: [%s]", configTpl)

	goTpl, err := template.New("tpl").Parse(configTpl)
	if err != nil {
		log.Fatalf("failed to create template: [%s]", err.Error())
	}

	var tpl bytes.Buffer
	err = goTpl.ExecuteTemplate(&tpl, "tpl", pluginEnv)
	if err != nil {
		log.Fatalf("failed to execute template: [%s]", err.Error())
	}
	return tpl.String()
}
