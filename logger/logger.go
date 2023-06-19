package logger

import (
	"errors"
	"strconv"
	"strings"

	"github.com/ssergomol/raft/utils"
)

const serversFileName string = "all-servers.txt"
const serverStateFileName string = "server-state.txt"

func AddServer(serverName string, port string) error {
	var err = utils.CreateFileIfNotExists(serversFileName)
	if err != nil {
		return err
	}
	registryLog := serverName + "," + port + "\n"
	err = utils.WriteToFile(serversFileName, registryLog)
	if err != nil {
		return err
	}
	return nil
}

func ListAllServers() (map[string]int, error) {
	m := make(map[string]int)
	registeryLines, err := utils.ReadFile(serversFileName)
	if err != nil {
		return m, err
	}
	for _, line := range registeryLines {
		splits := strings.Split(line, ",")
		port, _ := strconv.Atoi(splits[1])
		m[splits[0]] = port
	}
	return m, nil
}

func PersistServerState(serverStateLog string) error {
	var err = utils.CreateFileIfNotExists(serverStateFileName)
	if err != nil {
		return err
	}
	err = utils.WriteToFile(serverStateFileName, serverStateLog+"\n")
	if err != nil {
		return err
	}
	return nil
}

func GetLatestServerStateIfPresent(serverName string) (string, error) {
	serverStateLogs, err := utils.ReadFile(serverStateFileName)
	var serverStateLog = ""
	if err != nil {
		return "", err
	}
	for _, line := range serverStateLogs {
		splits := strings.Split(line, ",")
		if splits[0] == serverName {
			serverStateLog = line
		}
	}
	if serverStateLog == "" {
		return "", errors.New("no existing server state found")
	}
	return serverStateLog, nil
}
