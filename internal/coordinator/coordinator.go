package coordinator

import (
	"bytes"
	worker "dist-cut/internal/http"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func Main(peers []string, listenAddr string, fields, delim string, replication, quorum int) error {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	lines := strings.Split(string(input), "\n")

	nodes := append([]string{listenAddr}, peers...)
	N := len(nodes)
	if N == 0 {
		return fmt.Errorf("no nodes available")
	}

	shardCount := N
	shards := make([]string, shardCount)
	for i, line := range lines {
		if i == len(lines)-1 && line == "" {
			break
		}
		shards[i%shardCount] += line
		if i != len(lines)-1 {
			shards[i%shardCount] += "\n"
		}
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	type result struct {
		idx int
		res string
		err error
	}

	results := make([]string, shardCount)

	for i := 0; i < shardCount; i++ {

		repNodes := make([]string, 0, replication)
		for r := 0; r < replication; r++ {
			repNodes = append(repNodes, nodes[(i+r)%N])
		}

		ch := make(chan result, len(repNodes))
		var wg sync.WaitGroup

		for _, node := range repNodes {
			wg.Add(1)

			go func(node string, idx int) {
				defer wg.Done()

				if node == listenAddr {
					res, err := worker.ProcessChunk(shards[idx], fields, delim)
					ch <- result{idx: idx, res: res, err: err}
					return
				}

				tr := worker.TaskRequest{
					Chunk:  shards[idx],
					Fields: fields,
					Delim:  delim,
				}

				jb, err := json.Marshal(tr)
				if err != nil {
					ch <- result{idx: idx, err: err}
					return
				}

				url := fmt.Sprintf("http://%s/process", node)
				resp, err := client.Post(url, "application/json", bytes.NewReader(jb))
				if err != nil {
					ch <- result{idx: idx, err: err}
					return
				}
				defer resp.Body.Close()

				b, err := io.ReadAll(resp.Body)
				if err != nil {
					ch <- result{idx: idx, err: err}
					return
				}

				var tres worker.TaskResponse
				if err := json.Unmarshal(b, &tres); err != nil {
					ch <- result{idx: idx, err: err}
					return
				}

				if tres.Err != "" {
					ch <- result{idx: idx, err: fmt.Errorf(tres.Err)}
					return
				}

				ch <- result{idx: idx, res: tres.Result}
			}(node, i)
		}

		go func() {
			wg.Wait()
			close(ch)
		}()

		successes := 0
		var chosen string
		errs := make([]string, 0)

		for r := range ch {
			if r.err == nil {
				successes++
				if chosen == "" {
					chosen = r.res
				}
			} else {
				errs = append(errs, r.err.Error())
			}

			if successes >= quorum {
				break
			}
		}

		if successes < quorum {
			return fmt.Errorf("shard %d failed quorum: got %d, errs: %v", i, successes, errs)
		}

		results[i] = chosen
	}

	var out bytes.Buffer
	for i := 0; i < shardCount; i++ {
		out.WriteString(results[i])
	}

	fmt.Print(out.String())
	return nil
}
