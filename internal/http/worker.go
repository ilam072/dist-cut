package http

import (
	"bufio"
	"bytes"
	"dist-cut/internal/parser"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type TaskRequest struct {
	Chunk  string `json:"chunk"`
	Fields string `json:"fields"`
	Delim  string `json:"delim"`
}

type TaskResponse struct {
	Result string `json:"result"`
	Err    string `json:"err"`
}

func ProcessChunk(chunk, fields, delim string) (string, error) {
	fs, err := parser.ParseFields(fields)
	if err != nil {
		return "", err
	}
	if delim == "" {
		delim = "\t"
	}
	var out bytes.Buffer
	r := bufio.NewReader(strings.NewReader(chunk))
	for {
		line, err := r.ReadString('\n')
		if line == "" && err == io.EOF {
			break
		}

		ended := false
		if strings.HasSuffix(line, "\n") {
			ended = true
			line = line[:len(line)-1]
		}
		parts := strings.Split(line, delim)
		sel := fs.SelectFields(parts)
		out.WriteString(strings.Join(sel, delim))
		if ended {
			out.WriteByte('\n')
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return out.String(), nil
}

func WorkerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req TaskRequest
	b, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(b, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"err":"%s"}`, err.Error())
		return
	}
	res, err := ProcessChunk(req.Chunk, req.Fields, req.Delim)
	resp := TaskResponse{Result: res}
	if err != nil {
		resp.Err = err.Error()
	}
	enc, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(enc)
}
