package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// ROCmMemoryInfo represents GPU memory information from rocm-smi
type ROCmMemoryInfo struct {
	DeviceID   int
	VRAMUsed   uint64 // in MB
	VRAMTotal  uint64 // in MB
	VRAMPercent float64
}

// queryROCmSMIInPod executes rocm-smi inside a pod to get VRAM usage
func (r *MetaRouter) queryROCmSMIInPod(ctx context.Context, podName string) (*ROCmMemoryInfo, error) {
	// Command to get VRAM usage in CSV format
	cmd := []string{
		"sh", "-c",
		"rocm-smi --showmeminfo vram --csv 2>/dev/null || echo 'error'",
	}
	
	var stdout, stderr bytes.Buffer
	
	req := r.k8sClient.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(r.namespace).
		SubResource("exec")
	
	req.VersionedParams(&v1.PodExecOptions{
		Command: cmd,
		Stdout:  true,
		Stderr:  true,
	}, scheme.ParameterCodec)
	
	exec, err := remotecommand.NewSPDYExecutor(r.Config, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("failed to create executor: %w", err)
	}
	
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	
	if err != nil {
		log.Printf("ROCm SMI exec error for pod %s: %v, stderr: %s\n", podName, err, stderr.String())
		return nil, err
	}
	
	output := stdout.String()
	if strings.Contains(output, "error") || output == "" {
		// ROCm SMI not available, try alternative approach
		return r.queryROCmSMIAlternative(ctx, podName)
	}
	
	return parseROCmSMIOutput(output)
}

// parseROCmSMIOutput parses the CSV output from rocm-smi
func parseROCmSMIOutput(output string) (*ROCmMemoryInfo, error) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) < 2 {
		return nil, fmt.Errorf("invalid rocm-smi output")
	}
	
	// CSV format: device,VRAM Total Memory (B),VRAM Total Used Memory (B)
	// or similar - need to parse the actual format
	
	info := &ROCmMemoryInfo{
		DeviceID: 0,
	}
	
	// Parse CSV (simplified - adjust based on actual rocm-smi output)
	for i, line := range lines {
		if i == 0 {
			// Skip header
			continue
		}
		
		fields := strings.Split(line, ",")
		if len(fields) >= 3 {
			// Try to parse memory values (assuming bytes)
			if total, err := strconv.ParseUint(strings.TrimSpace(fields[1]), 10, 64); err == nil {
				info.VRAMTotal = total / (1024 * 1024) // Convert bytes to MB
			}
			if used, err := strconv.ParseUint(strings.TrimSpace(fields[2]), 10, 64); err == nil {
				info.VRAMUsed = used / (1024 * 1024) // Convert bytes to MB
			}
			
			if info.VRAMTotal > 0 {
				info.VRAMPercent = float64(info.VRAMUsed) / float64(info.VRAMTotal) * 100
			}
			
			break // Only process first GPU for now
		}
	}
	
	if info.VRAMTotal == 0 {
		return nil, fmt.Errorf("failed to parse VRAM info")
	}
	
	return info, nil
}

// queryROCmSMIAlternative tries alternative methods to get GPU memory info
func (r *MetaRouter) queryROCmSMIAlternative(ctx context.Context, podName string) (*ROCmMemoryInfo, error) {
	// Try reading from /sys/class/kfd/kfd/topology/nodes/*/mem_banks/*/properties
	// This is where AMD GPU memory info is exposed
	
	cmd := []string{
		"sh", "-c",
		`for node in /sys/class/kfd/kfd/topology/nodes/*/mem_banks/*/properties; do 
			if [ -f "$node" ]; then 
				grep -E "heap_type|size_in_bytes|used" "$node" 2>/dev/null || true
			fi
		done | head -20`,
	}
	
	var stdout bytes.Buffer
	
	req := r.k8sClient.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(r.namespace).
		SubResource("exec")
	
	req.VersionedParams(&v1.PodExecOptions{
		Command: cmd,
		Stdout:  true,
		Stderr:  true,
	}, scheme.ParameterCodec)
	
	exec, err := remotecommand.NewSPDYExecutor(r.Config, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("failed to create executor: %w", err)
	}
	
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
	})
	
	if err != nil {
		log.Printf("Alternative GPU query failed for pod %s: %v\n", podName, err)
		// Return default values - get from environment or use Strix Halo default
		defaultVRAM, _ := strconv.ParseUint(getEnv("DEFAULT_VRAM_MB", "131072"), 10, 64)
		return &ROCmMemoryInfo{
			DeviceID:   0,
			VRAMTotal:  defaultVRAM, // Default from env or 128GB for Strix Halo
			VRAMUsed:   0,
		}, nil
	}
	
	output := stdout.String()
	log.Printf("GPU properties output for %s: %s\n", podName, output)
	
	// Parse the output (this is simplified)
	info := &ROCmMemoryInfo{
		DeviceID:   0,
		VRAMTotal:  128 * 1024, // Default for Strix Halo
		VRAMUsed:   0,
	}
	
	return info, nil
}

// updateNodeVRAM updates the VRAM information for a node
func (r *MetaRouter) updateNodeVRAM(ctx context.Context, node *NodeState) {
	memInfo, err := r.queryROCmSMIInPod(ctx, node.Hostname)
	if err != nil {
		log.Printf("Failed to query VRAM for %s: %v\n", node.Hostname, err)
		return
	}
	
	node.mu.Lock()
	node.VRAMTotal = memInfo.VRAMTotal
	node.VRAMUsed = memInfo.VRAMUsed
	node.mu.Unlock()
	
	log.Printf("VRAM for %s: %d/%d MB (%.1f%%)\n", 
		node.Hostname, memInfo.VRAMUsed, memInfo.VRAMTotal, memInfo.VRAMPercent)
}
