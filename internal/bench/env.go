package bench

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// Environment describes the machine and versions a run measured on.
type Environment struct {
	CPU         string `json:"cpu"`
	LogicalCPUs int    `json:"logical_cpus"`
	MemoryBytes int64  `json:"memory_bytes"`
	Kernel      string `json:"kernel"`
	Go          string `json:"go"`
	Docker      string `json:"docker,omitempty"`
	Commit      string `json:"commit,omitempty"`
}

// DetectEnvironment describes the machine as this process sees it. In the
// compose toolbox container that is Docker's VM: its CPUs and memory are
// what the whole stack shares. docker and commit come from the host.
func DetectEnvironment(docker, commit string) Environment {
	env := Environment{LogicalCPUs: runtime.NumCPU(), Go: runtime.Version(), Docker: docker, Commit: commit}
	env.CPU = procField("/proc/cpuinfo", "model name")
	if kb, err := strconv.ParseInt(strings.TrimSuffix(procField("/proc/meminfo", "MemTotal"), " kB"), 10, 64); err == nil {
		env.MemoryBytes = kb * 1024
	}
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		env.Kernel = strings.TrimSpace(string(b))
	}
	return env
}

// procField returns the value of the first "key: value" line of a /proc
// file, or "" if there is none.
func procField(path, key string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), ":")
		if ok && strings.TrimSpace(k) == key {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
