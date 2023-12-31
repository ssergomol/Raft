package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ssergomol/raft/database"

	"github.com/ssergomol/raft/model"

	"github.com/ssergomol/raft/logger"
)

var (
	serverName = flag.String("server-name", "", "name for the server")
	port       = flag.String("port", "", "port for running the server")
)

const (
	BroadcastPeriod    = 3000
	ElectionMinTimeout = 3001
	ElectionMaxTimeout = 10000
)

type Server struct {
	port           string
	db             *database.Database
	serverState    *model.ServerState
	Logs           []string
	currentRole    string
	leaderNodeId   string
	peerdata       *model.PeerData
	electionModule *model.ElectionModule
}

func (s *Server) sendMessageToFollowerNode(message string, port int) {
	addr := "http://127.0.0.1:" + strconv.Itoa(port)
	reqBody := []byte(message)
	resp, err := http.Post(addr, "text/plain", bytes.NewBuffer(reqBody))

	if err != nil || resp.StatusCode != http.StatusOK {
		s.peerdata.SuspectedNodes[port] = true
		return
	}
	_, ok := s.peerdata.SuspectedNodes[port]
	if ok {
		delete(s.peerdata.SuspectedNodes, port)
	}

	go s.handleResponse(resp, addr)
}

func (s *Server) replicateLog(followerName string, followerPort int) {
	if followerName == s.serverState.Name {
		go s.commitLogEntries()
		return
	}
	var prefixTerm = 0
	prefixLength := s.peerdata.SentLength[followerName]
	if prefixLength > 0 {
		logSplit := strings.Split(s.Logs[prefixLength-1], "#")
		prefixTerm, _ = strconv.Atoi(logSplit[1])
	}
	logRequest := model.NewLogRequest(s.serverState.Name, s.serverState.CurrentTerm, prefixLength, prefixTerm, s.serverState.CommitLength, s.Logs[s.peerdata.SentLength[followerName]:])
	s.sendMessageToFollowerNode(logRequest.String(), followerPort)
}

func (s *Server) addLogs(log string) []string {
	s.Logs = append(s.Logs, log)
	return s.Logs
}

func (s *Server) appendEntries(prefixLength int, commitLength int, suffix []string) {
	if len(suffix) > 0 && len(s.Logs) > prefixLength {
		var index int
		if len(s.Logs) > (prefixLength + len(suffix)) {
			index = prefixLength + len(suffix) - 1
		} else {
			index = len(s.Logs) - 1
		}
		if parseLogTerm(s.Logs[index]) != parseLogTerm(suffix[index-prefixLength]) {
			s.Logs = s.Logs[:prefixLength]

		}
	}

	if prefixLength+len(suffix) > len(s.Logs) {
		for i := (len(s.Logs) - prefixLength); i < len(suffix); i++ {
			s.addLogs(suffix[i])
			err := s.db.LogCommand(suffix[i], s.serverState.Name)
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	if commitLength > s.serverState.CommitLength {
		for i := s.serverState.CommitLength; i < commitLength; i++ {
			s.db.PerformDbOperations(strings.Split(s.Logs[i], "#")[0])
		}
		s.serverState.CommitLength = commitLength
		s.serverState.LogServerPersistedState()
	}
}

func (s *Server) handleLogResponse(message string) string {
	lr, _ := model.ParseLogResponse(message)
	if lr.CurrentTerm > s.serverState.CurrentTerm {
		s.serverState.CurrentTerm = lr.CurrentTerm
		s.currentRole = "follower"
		s.serverState.VotedFor = ""
		go s.electionTimer()
	}
	if lr.CurrentTerm == s.serverState.CurrentTerm && s.currentRole == "leader" {
		if lr.ReplicationSuccessful && lr.AckLength >= s.peerdata.AckedLength[lr.NodeId] {
			s.peerdata.SentLength[lr.NodeId] = lr.AckLength
			s.peerdata.AckedLength[lr.NodeId] = lr.AckLength
			s.commitLogEntries()
		} else {
			s.peerdata.SentLength[lr.NodeId] = s.peerdata.SentLength[lr.NodeId] - 1
			s.replicateLog(lr.NodeId, lr.Port)
		}
	}
	return "replication successful"
}

func (s *Server) handleLogRequest(message string) string {
	fmt.Println("Got log request")
	s.electionModule.ResetElectionTimer <- struct{}{}
	logRequest, _ := model.ParseLogRequest(message)
	if logRequest.CurrentTerm > s.serverState.CurrentTerm {
		s.serverState.CurrentTerm = logRequest.CurrentTerm
		s.serverState.VotedFor = ""
	}
	if logRequest.CurrentTerm == s.serverState.CurrentTerm {
		if s.currentRole == "leader" {
			go s.electionTimer()
		}
		s.currentRole = "follower"
		s.leaderNodeId = logRequest.LeaderId
	}
	var logOk bool = false
	if len(s.Logs) >= logRequest.PrefixLength &&
		(logRequest.PrefixLength == 0 ||
			parseLogTerm(s.Logs[logRequest.PrefixLength-1]) == logRequest.PrefixTerm) {
		logOk = true
	}
	port, _ := strconv.Atoi(s.port)
	if s.serverState.CurrentTerm == logRequest.CurrentTerm && logOk {
		s.appendEntries(logRequest.PrefixLength, logRequest.CommitLength, logRequest.Suffix)
		ack := logRequest.PrefixLength + len(logRequest.Suffix)
		return model.NewLogResponse(s.serverState.Name, port, s.serverState.CurrentTerm, ack, true).String()
	} else {
		return model.NewLogResponse(s.serverState.Name, port, s.serverState.CurrentTerm, 0, false).String()
	}
}

func (s *Server) commitLogEntries() {
	allNodes, _ := logger.ListAllServers()
	aliveNodes := len(allNodes) - len(s.peerdata.SuspectedNodes)
	for i := s.serverState.CommitLength; i < len(s.Logs); i++ {
		var acks = 0
		for node := range allNodes {
			if s.peerdata.AckedLength[node] > s.serverState.CommitLength {
				acks = acks + 1
			}
		}
		if acks >= (aliveNodes+1)/2 || aliveNodes == 1 {
			log := s.Logs[i]
			command := strings.Split(log, "#")[0]
			s.db.PerformDbOperations(command)
			s.serverState.CommitLength = s.serverState.CommitLength + 1
			s.serverState.LogServerPersistedState()
		} else {
			break
		}
	}
}

func (s *Server) handleVoteRequest(message string) string {
	voteRequest, _ := model.ParseVoteRequest(message)
	if voteRequest.CandidateTerm > s.serverState.CurrentTerm {
		s.serverState.CurrentTerm = voteRequest.CandidateTerm
		s.currentRole = "follower"
		s.serverState.VotedFor = ""
		s.electionModule.ResetElectionTimer <- struct{}{}
	}
	var lastTerm = 0
	if len(s.Logs) > 0 {
		lastTerm = parseLogTerm(s.Logs[len(s.Logs)-1])
	}
	var logOk = false
	if voteRequest.CandidateLogTerm > lastTerm ||
		(voteRequest.CandidateLogTerm == lastTerm && voteRequest.CandidateLogLength >= len(s.Logs)) {
		logOk = true
	}

	if voteRequest.CandidateTerm == s.serverState.CurrentTerm && logOk && (s.serverState.VotedFor == "" || s.serverState.VotedFor == voteRequest.CandidateId) {
		s.serverState.VotedFor = voteRequest.CandidateId
		s.serverState.LogServerPersistedState()
		return model.NewVoteResponse(
			s.serverState.Name,
			s.serverState.CurrentTerm,
			true,
		).String()
	} else {
		return model.NewVoteResponse(s.serverState.Name, s.serverState.CurrentTerm, false).String()
	}
}

func (s *Server) handleVoteResponse(message string) {
	voteResponse, _ := model.ParseVoteResponse(message)
	if voteResponse.CurrentTerm > s.serverState.CurrentTerm {
		if s.currentRole != "leader" {
			s.electionModule.ResetElectionTimer <- struct{}{}
		}
		s.serverState.CurrentTerm = voteResponse.CurrentTerm
		s.currentRole = "follower"
		s.serverState.VotedFor = ""
	}
	if s.currentRole == "candidate" && voteResponse.CurrentTerm == s.serverState.CurrentTerm && voteResponse.VoteInFavor {
		s.peerdata.VotesReceived[voteResponse.NodeId] = true
		s.checkForElectionResult()
	}
}

func (s *Server) checkForElectionResult() {
	if s.currentRole == "leader" {
		return
	}
	var totalVotes = 0
	for server := range s.peerdata.VotesReceived {
		if s.peerdata.VotesReceived[server] {
			totalVotes += 1
		}
	}
	allNodes, _ := logger.ListAllServers()
	aliveNodes := len(allNodes) - len(s.peerdata.SuspectedNodes)

	if (totalVotes >= (aliveNodes+1)/2) || aliveNodes == 1 {
		fmt.Println("I won the election. New leader: ", s.serverState.Name, " Votes received: ", totalVotes)
		s.currentRole = "leader"
		s.leaderNodeId = s.serverState.Name
		s.peerdata.VotesReceived = make(map[string]bool)
		s.electionModule.ElectionTimeout.Stop()
		s.syncUp()
	}
}

func (s *Server) startElection() {
	s.serverState.CurrentTerm = s.serverState.CurrentTerm + 1
	s.currentRole = "candidate"
	s.serverState.VotedFor = s.serverState.Name
	s.peerdata.VotesReceived = map[string]bool{}
	s.peerdata.VotesReceived[s.serverState.Name] = true
	var lastTerm = 0
	if len(s.Logs) > 0 {
		lastTerm = parseLogTerm(s.Logs[len(s.Logs)-1])
	}

	voteRequest := model.NewVoteRequest(s.serverState.Name, s.serverState.CurrentTerm, len(s.Logs), lastTerm)
	allNodes, _ := logger.ListAllServers()
	for node, port := range allNodes {
		if node != s.serverState.Name {
			s.sendMessageToFollowerNode(voteRequest.String(), port)
		}
	}
	s.checkForElectionResult()
}

func (s *Server) electionTimer() {
	for {
		select {
		case <-s.electionModule.ElectionTimeout.C:
			fmt.Println("Timed out")
			if s.currentRole == "follower" {
				go s.startElection()
			} else {
				s.currentRole = "follower"
				s.electionModule.ResetElectionTimer <- struct{}{}
			}
		case <-s.electionModule.ResetElectionTimer:
			fmt.Println("Resetting election timer")
			s.electionModule.ElectionTimeout.Reset(time.Duration(s.electionModule.ElectionTimeoutInterval) * time.Millisecond)
		}
	}
}

func (s *Server) syncUp() {
	ticker := time.NewTicker(BroadcastPeriod * time.Millisecond)
	for t := range ticker.C {
		fmt.Println("sending heartbeat at: ", t)
		allServers, _ := logger.ListAllServers()
		for sname, sport := range allServers {
			if sname != s.serverState.Name {
				s.replicateLog(sname, sport)
			}
		}
	}
}

func main() {
	parseFlags()

	db, err := database.NewDatabase()
	if err != nil {
		fmt.Println("Error while creating db")
		return
	}

	rand.Seed(time.Now().UnixNano())
	electionTimeoutInterval := rand.Intn(int(ElectionMaxTimeout)-int(ElectionMinTimeout)) + int(ElectionMinTimeout)
	electionModule := model.NewElectionModule(electionTimeoutInterval)

	err = logger.AddServer(*serverName, *port)
	if err != nil {
		fmt.Println(err)
		return
	}

	s := Server{
		port:           *port,
		db:             db,
		Logs:           db.RebuildLogIfExists(*serverName),
		serverState:    model.GetExistingServerStateOrCreateNew(*serverName),
		currentRole:    "follower",
		leaderNodeId:   "",
		peerdata:       model.NewPeerData(),
		electionModule: electionModule,
	}
	s.serverState.LogServerPersistedState()
	go s.electionTimer()
	http.HandleFunc("/", s.handleConn)

	err = http.ListenAndServe(":"+*port, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func parseLogTerm(message string) int {
	split := strings.Split(message, "#")
	pTerm, _ := strconv.Atoi(split[1])
	return pTerm
}

func parseFlags() {
	flag.Parse()

	if *serverName == "" {
		log.Fatalf("Must provide serverName for the server")
	}

	if *port == "" {
		log.Fatalf("Must provide a port number for server to run")
	}
}

func (s *Server) handleResponse(res *http.Response, addr string) error {
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// Convert the request body to a string
	data := string(body)
	message := strings.TrimSpace(string(data))

	if message == "invalid command" || message == "replication successful" {
		return nil
	}
	fmt.Println(">", string(message))

	var response string = ""
	if strings.HasPrefix(message, "LogRequest") {
		response = s.handleLogRequest(message)
	}
	if strings.HasPrefix(message, "LogResponse") {
		response = s.handleLogResponse(message)
	}
	if strings.HasPrefix(message, "VoteRequest") {
		response = s.handleVoteRequest(message)
	}
	if strings.HasPrefix(message, "VoteResponse") {
		s.handleVoteResponse(message)
	}

	if s.currentRole == "leader" && response == "" {
		var err = s.db.ValidateCommand(message)
		if err != nil {
			response = err.Error()
		}
	}

	if response != "" {
		reqBody := []byte(response)
		_, err = http.Post(addr, "text/plain", bytes.NewBuffer(reqBody))
	}
	return err
}

func (s *Server) handleConn(w http.ResponseWriter, r *http.Request) {
	var response string

	switch r.Method {
	case http.MethodPost:
		// Read the request body
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		data := string(body)

		message := strings.TrimSpace(string(data))
		if message == "invalid command" || message == "replication successful" {
			return
		}
		fmt.Println(">", string(message))

		if strings.HasPrefix(message, "LogRequest") {
			response = s.handleLogRequest(message)
		}
		if strings.HasPrefix(message, "LogResponse") {
			response = s.handleLogResponse(message)
		}
		if strings.HasPrefix(message, "VoteRequest") {
			response = s.handleVoteRequest(message)
		}
		if strings.HasPrefix(message, "VoteResponse") {
			s.handleVoteResponse(message)
		}

		if s.currentRole == "leader" && response == "" {
			var err = s.db.ValidateCommand(message)
			if err != nil {
				response = err.Error()
			}

			if response == "" {
				logMessage := message + "#" + strconv.Itoa(s.serverState.CurrentTerm)
				s.peerdata.AckedLength[s.serverState.Name] = len(s.Logs)
				s.Logs = append(s.Logs, logMessage)
				currLogIdx := len(s.Logs) - 1
				err = s.db.LogCommand(logMessage, s.serverState.Name)
				if err != nil {
					response = "error while logging command"
				}

				allServers, _ := logger.ListAllServers()
				for sname, sport := range allServers {
					s.replicateLog(sname, sport)
				}

				for s.serverState.CommitLength <= currLogIdx {
					fmt.Println("Waiting for consensus: ")
				}
				response = "operation sucessful"
			}
		} else if s.currentRole != "leader" && response == "" {

			allServers, _ := logger.ListAllServers()
			fmt.Println("Current leader:", s.leaderNodeId)
			resp, err := http.Post("http://localhost:"+strconv.Itoa(allServers[s.leaderNodeId]),
				"text/plain", bytes.NewBuffer(body))

			if err != nil {
				http.Error(w, "Error redirecting request", http.StatusBadRequest)
				return
			}

			respData, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				http.Error(w, "Error reading request body", http.StatusBadRequest)
				return
			}

			response = string(respData)
		}

	case http.MethodGet:
		queryParams := r.URL.Query()
		key := queryParams.Get("key")
		fmt.Println(">", "GET", key)
		response = s.db.PerformGet(key)

	case http.MethodDelete:
		queryParams := r.URL.Query()
		key := queryParams.Get("key")

		fmt.Println(">", "DELETE", key)
		message := "DELETE " + key

		if s.currentRole == "leader" && response == "" {
			var err error

			if response == "" {
				logMessage := message + "#" + strconv.Itoa(s.serverState.CurrentTerm)
				s.peerdata.AckedLength[s.serverState.Name] = len(s.Logs)
				s.Logs = append(s.Logs, logMessage)
				currLogIdx := len(s.Logs) - 1
				err = s.db.LogCommand(logMessage, s.serverState.Name)
				if err != nil {
					response = "error while logging command"
				}

				allServers, _ := logger.ListAllServers()
				for sname, sport := range allServers {
					s.replicateLog(sname, sport)
				}

				for s.serverState.CommitLength <= currLogIdx {
					fmt.Println("Waiting for consensus: ")
				}
				response = "operation sucessful"
			}
		} else if s.currentRole != "leader" && response == "" {

			allServers, _ := logger.ListAllServers()
			fmt.Println("Current leader:", s.leaderNodeId)

			baseURL := "http://localhost:" + strconv.Itoa(allServers[s.leaderNodeId])
			parameters := url.Values{}
			parameters.Add("key", key)
			url := fmt.Sprintf("%s?%s", baseURL, parameters.Encode())

			req, err := http.NewRequest("DELETE", url, nil)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			client := &http.Client{}
			resp, err := client.Do(req)

			if err != nil {
				http.Error(w, "Error redirecting request", http.StatusBadRequest)
				return
			}

			respData, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				http.Error(w, "Error reading request body", http.StatusBadRequest)
				return
			}

			response = string(respData)
		}

	}

	if response != "" {
		w.Write([]byte(response + "\n"))
	}
}
