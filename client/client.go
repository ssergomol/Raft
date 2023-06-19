package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ssergomol/raft/logger"
)

func ValidateSet(cmd string) error {
	cmdSplits := strings.Split(cmd, " ")
	if cmdSplits[0] != "SET" {
		return errors.New("invalid SET command, bad request")
	}

	if len(cmdSplits) != 3 {
		return errors.New("need a key for GET/DELETE operation, bad request")
	}

	_, err := strconv.Atoi(cmdSplits[2])
	if err != nil {
		fmt.Println(err)
		return errors.New("not a valid integer value, bad request")
	}

	return nil
}

func ValidateDelete(cmd string) error {
	cmdSplits := strings.Split(cmd, " ")

	if cmdSplits[0] != "DELETE" {
		return errors.New("invalid DELETE command, bad request")
	}

	if len(cmdSplits) != 2 {
		return errors.New("need a key for GET/DELETE operation, bad request")
	}

	return nil
}

func ValidateGet(cmd string) error {
	cmdSplits := strings.Split(cmd, " ")
	if cmdSplits[0] != "GET" {
		return errors.New("invalid GET command, bad request")
	}

	if len(cmdSplits) != 2 {
		return errors.New("need a key for GET/DELETE operation, bad request")
	}

	return nil
}

func main() {
	var addr string = "localhost"

	for {
		allServers, _ := logger.ListRegisteredServer()
		rand.Seed(time.Now().UnixNano())

		// Create a slice to store the values
		ports := make([]int, 0, len(allServers))

		// Iterate over the map and append values to the slice
		for _, port := range allServers {
			ports = append(ports, port)
		}
		// Generate a random permutation of servers
		randServers := rand.Perm(len(allServers))
		// currentServer := 0
		var randomPort string

		reader := bufio.NewReader(os.Stdin)
		fmt.Print(">")
		text, _ := reader.ReadString('\n')
		text = strings.TrimRight(text, "\n")

		reqBody := []byte(text)
		cmdSplits := strings.Split(text, " ")

		var err error
		var resp *http.Response
		switch cmdSplits[0] {
		case "GET":
			err = ValidateGet(text)
			if err != nil {
				break
			}

			for _, serverIdx := range randServers {
				randomPort = strconv.Itoa(ports[serverIdx])

				baseURL := "http://" + addr + ":" + randomPort
				parameters := url.Values{}
				parameters.Add("key", cmdSplits[1])
				url := fmt.Sprintf("%s?%s", baseURL, parameters.Encode())
				resp, err = http.Get(url)

				if err == nil {
					break
				}
			}

			if err != nil {
				fmt.Println("The service is down:", err)
				return
			}

		case "SET":
			err = ValidateSet(text)
			if err != nil {
				break
			}

			for _, serverIdx := range randServers {
				randomPort = strconv.Itoa(ports[serverIdx])

				resp, err = http.Post("http://"+addr+":"+randomPort, "text/plain", bytes.NewBuffer(reqBody))
				if err == nil {
					break
				}
			}

			if err != nil {
				fmt.Println("The service is down:", err)
				return
			}

		case "DELETE":
			err = ValidateDelete(text)
			if err != nil {
				break
			}
			for _, serverIdx := range randServers {

				randomPort = strconv.Itoa(ports[serverIdx])
				baseURL := "http://" + addr + ":" + randomPort
				parameters := url.Values{}
				parameters.Add("key", cmdSplits[1])
				url := fmt.Sprintf("%s?%s", baseURL, parameters.Encode())

				req, err := http.NewRequest("DELETE", url, nil)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}

				client := &http.Client{}
				resp, err = client.Do(req)
				if err == nil {
					break
				}
			}

			if err != nil {
				fmt.Println("The service is down:", err)
				return
			}

		default:
			err = errors.New("invalid command")
		}

		if err != nil {
			fmt.Println("Error sending request from client:", err)
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("Error reading response body:", err)
			resp.Body.Close()
			continue
		}

		// fmt.Println("Response:", string(body))
		message := string(body)

		fmt.Print("> " + message)
		if strings.TrimSpace(string(text)) == "EXIT" {
			fmt.Println("HTTP client exiting.")
			resp.Body.Close()
			return
		}
		resp.Body.Close()
	}
}
