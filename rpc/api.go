package rpc

import (
	"errors"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/ava-labs/avalanchego/utils/logging"
)

type SuccessResponse struct {
	Success bool `json:"success"`
}

type Profile struct {
	File string `json:"file"`
}

type API struct {
	log         logging.Logger
	performance *Performance
}

func NewAPI(log logging.Logger) *API {
	return &API{log: log, performance: &Performance{}}
}

// StartCPUProfiler starts a cpu profile writing to the specified file
func (service *API) StartCPUProfiler(_ *http.Request, args *Profile, reply *SuccessResponse) error {
	reply.Success = true
	return service.performance.StartCPUProfiler(args.File)
}

// StopCPUProfiler stops the cpu profile
func (service *API) StopCPUProfiler(_ *http.Request, _ *struct{}, reply *SuccessResponse) error {
	service.log.Info("Admin: StopCPUProfiler called")
	reply.Success = true
	return service.performance.StopCPUProfiler()
}

// MemoryProfile runs a memory profile writing to the specified file
func (service *API) MemoryProfile(_ *http.Request, args *Profile, reply *SuccessResponse) error {
	service.log.Info("Admin: MemoryProfile called")
	reply.Success = true
	return service.performance.MemoryProfile(args.File)
}

// LockProfile runs a mutex profile writing to the specified file
func (service *API) LockProfile(_ *http.Request, args *Profile, reply *SuccessResponse) error {
	service.log.Info("Admin: LockProfile called")
	reply.Success = true
	return service.performance.LockProfile(args.File)
}

var (
	errCPUProfilerRunning    = errors.New("cpu profiler already running")
	errCPUProfilerNotRunning = errors.New("cpu profiler doesn't exist")
)

type Performance struct{ cpuProfileFile *os.File }

// StartCPUProfiler starts measuring the cpu utilization of this node
func (p *Performance) StartCPUProfiler(filePath string) error {
	if p.cpuProfileFile != nil {
		return errCPUProfilerRunning
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close() // Return the original error
		return err
	}
	runtime.SetMutexProfileFraction(1)

	p.cpuProfileFile = file
	return nil
}

// StopCPUProfiler stops measuring the cpu utilization of this node
func (p *Performance) StopCPUProfiler() error {
	if p.cpuProfileFile == nil {
		return errCPUProfilerNotRunning
	}

	pprof.StopCPUProfile()
	err := p.cpuProfileFile.Close()
	p.cpuProfileFile = nil
	return err
}

// MemoryProfile dumps the current memory utilization of this node
func (p *Performance) MemoryProfile(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(file); err != nil {
		_ = file.Close() // Return the original error
		return err
	}
	return file.Close()
}

// LockProfile dumps the current lock statistics of this node
func (p *Performance) LockProfile(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	profile := pprof.Lookup("mutex")
	if err := profile.WriteTo(file, 1); err != nil {
		_ = file.Close() // Return the original error
		return err
	}
	return file.Close()
}
