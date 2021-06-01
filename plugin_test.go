package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCase describes the input data and the expected results
type TestCase struct {
	ExpectedCommand string
	Message         string
	SparkSubmitConf Config
}

// TestPlugin_AssembleSparkSubmitCommand checks the assembled command string
func TestPlugin_AssembleSparkSubmitCommand(t *testing.T) {

	testCases := []TestCase{
		TestCase{// happy flow with all types of args
			ExpectedCommand: "/opt/spark/bin/spark-submit --verbose --deploy-mode cluster --opt1 val1 --opt2 val2 --conf prop1key=prop1val --conf prop2key=prop2val appArg1 appArg2",
			Message: "the assembled command string is not as expected",
			SparkSubmitConf: Config{
				SubmitOptions: map[string]string{
					"opt1": "val1",
					"opt2": "val2"},
				SparkConfig: map[string]string{
					"prop1key": "prop1val",
					"prop2key": "prop2val",
				}, AppArgs: []string{"appArg1", "appArg2"},
			},
		}, TestCase{// no spark configs
			ExpectedCommand: "/opt/spark/bin/spark-submit --verbose --deploy-mode cluster --opt1 val1 --opt2 val2 appArg1 appArg2",
			Message: "the assembled command string is not as expected",
			SparkSubmitConf: Config{
				SubmitOptions: map[string]string{
					"opt1": "val1",
					"opt2": "val2"},
				SparkConfig: nil,
				AppArgs:     []string{"appArg1", "appArg2"},
			},
		}, TestCase{// no app args, no spark configs
			ExpectedCommand: "/opt/spark/bin/spark-submit --verbose --deploy-mode cluster --opt1 val1 --opt2 val2",
			Message: "the assembled command string is not as expected",
			SparkSubmitConf: Config{
				SubmitOptions: map[string]string{
					"opt1": "val1",
					"opt2": "val2"},
				SparkConfig: nil,
				AppArgs:     nil,
			}},
	}

	assert := assert.New(t)

	for _, tc := range testCases {
		cmd, _ := tc.SparkSubmitConf.AssembleSparkSubmitCommand()
		assert.Equal(tc.ExpectedCommand, cmd, "the assembled command string is not as expected")
	}

}

func TestConfig_ProcessTemplateConfigs(t *testing.T) {
	config := Config{
		SubmitOptions: map[string]string{"key.{{ .SUBMIT_KEY_PLACEHOLDER }}": "value {{ .SUBMIT_VALUE_PLACEHOLDER }}",},
		SparkConfig:   map[string]string{"key.{{ .CONFIG_KEY_PLACEHOLDER }}": "value {{ .CONFIG_VALUE_PLACEHOLDER }}",},
		AppArgs:       []string{"{{ .APP_ARG_PLACEHOLDER }}={{ .APP_ARG_VALUE_PLACEHOLDER }}"},
		Env: map[string]string{
			"SUBMIT_KEY_PLACEHOLDER":    "submit_replaced",
			"SUBMIT_VALUE_PLACEHOLDER":  "submit space replaced",
			"CONFIG_KEY_PLACEHOLDER":    "config_replaced",
			"CONFIG_VALUE_PLACEHOLDER":  "config space replaced",
			"APP_ARG_PLACEHOLDER":       "appArg",
			"APP_ARG_VALUE_PLACEHOLDER": "appArgValue",
		},
	}

	config.ProcessTemplateConfigs()
	assert.New(t)

	assert.Equal(t, "value submit space replaced", config.SubmitOptions["key.submit_replaced"])
	assert.Equal(t, "value config space replaced", config.SparkConfig["key.config_replaced"])
	assert.Equal(t, "appArg=appArgValue", config.AppArgs[0])

}
