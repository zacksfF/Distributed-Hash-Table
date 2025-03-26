package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

/*
Hey, I just want to apologize if anything wasn’t clear earlier. I’m still learning 
how to make the best content possible, and I really appreciate your patience. What’s 
coming next is going to be great, and I’m excited to share it with you!
*/

// Simplified structure for vector clock - maps node ID to counter
type VectorClock map[int]int

// Clone returns a copy of the vector clock
func (vc VectorClock) Clone() VectorClock {
	clone := make(VectorClock)
	for k, v := range vc {
		clone[k] = v
	}
	return clone
}

// Increment increases the clock for the given node ID
func (vc VectorClock) Increment(nodeID int) {
	vc[nodeID]++
}

// Merge combines two vector clocks, taking the max of each position
func (vc VectorClock) Merge(other VectorClock) VectorClock {
	result := vc.Clone()
	for k, v := range other {
		if current, exists := result[k]; !exists || v > current {
			result[k] = v
		}
	}
	return result
}

// DataItem represents a stored value with its vector clock
type DataItem struct {
	Value       string
	VectorClock VectorClock
}

// Node represents a node in the Chord DHT
type Node struct {
	ID          int // Node ID
	Data        map[string]DataItem // Key-value store
	PartitionID int // Partition ID (0 for unified network)
	mu          sync.RWMutex // Mutex for thread-safe access
	vectorClock VectorClock // Vector clock for conflict resolution
}

// ChordDHT represents the entire DHT system
type ChordDHT struct {
	Nodes      []*Node // List of nodes in the DHT
	Partitions map[int][]int 
	mu         sync.RWMutex 
}

// NewChordDHT creates a new Chord DHT with the specified number of nodes
func NewChordDHT(numNodes int) *ChordDHT {
	dht := &ChordDHT{
		Nodes:      make([]*Node, numNodes),
		Partitions: make(map[int][]int),
	}

	// Initialize nodes
	nodeIDs := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		nodeIDs[i] = i
		dht.Nodes[i] = &Node{
			ID:          i,
			Data:        make(map[string]DataItem),
			PartitionID: 0,
			vectorClock: make(VectorClock),
		}
	}

	// All nodes start in partition 0 (unified network)
	dht.Partitions[0] = nodeIDs

	return dht
}

// Put stores a key-value pair in the DHT
func (dht *ChordDHT) Put(key, value string, nodeIdx int) error {

	// Check if the node index is valid
	dht.mu.RLock()
	node := dht.Nodes[nodeIdx]
	partitionID := node.PartitionID
	dht.mu.RUnlock()

	// Simple routing: find a node in the same partition
	targetNode := node
	for _, n := range dht.Nodes {
		if n.PartitionID == partitionID {
			targetNode = n
			break
		}
	}

	// Update the vector clock
	targetNode.mu.Lock()
	newClock := targetNode.vectorClock.Clone()
	newClock.Increment(targetNode.ID)

	// Update or insert the data
	dataItem := DataItem{
		Value:       value,
		VectorClock: newClock,
	}

	// Check for conflicts with existing data
	if existingItem, exists := targetNode.Data[key]; exists {
		// Merge vector clocks
		dataItem.VectorClock = existingItem.VectorClock.Merge(newClock)
	}

	// Store the data item
	targetNode.Data[key] = dataItem
	targetNode.vectorClock = targetNode.vectorClock.Merge(newClock)
	targetNode.mu.Unlock()

	// Replicate to other nodes in the same partition
	for _, n := range dht.Nodes {
		if n.ID != targetNode.ID && n.PartitionID == partitionID {
			n.mu.Lock()
			n.Data[key] = dataItem
			n.vectorClock = n.vectorClock.Merge(newClock)
			n.mu.Unlock()
		}
	}

	return nil
}

// Get retrieves a value for a key from the DHT
func (dht *ChordDHT) Get(key string, nodeIdx int) (string, error) {
	dht.mu.RLock()
	node := dht.Nodes[nodeIdx]
	partitionID := node.PartitionID
	dht.mu.RUnlock()

	// Try to get the value from this node
	node.mu.RLock()
	if item, exists := node.Data[key]; exists {
		node.mu.RUnlock()
		return item.Value, nil
	}
	node.mu.RUnlock()

	// Try other nodes in the same partition
	for _, n := range dht.Nodes {
		if n.ID != node.ID && n.PartitionID == partitionID {
			n.mu.RLock()
			if item, exists := n.Data[key]; exists {
				n.mu.RUnlock()
				return item.Value, nil
			}
			n.mu.RUnlock()
		}
	}

	return "", fmt.Errorf("key not found")
}


// CreatePartition divides the network into partitions
func (dht *ChordDHT) CreatePartition(nodeSets [][]int) error {
	dht.mu.Lock()
	defer dht.mu.Unlock()

	// Clear existing partitions
	dht.Partitions = make(map[int][]int)

	// Create new partitions
	for i, nodeSet := range nodeSets {
		partitionID := i + 1

		// Update nodes with new partition ID
		for _, nodeIdx := range nodeSet {
			if nodeIdx >= 0 && nodeIdx < len(dht.Nodes) {
				dht.Nodes[nodeIdx].mu.Lock()
				dht.Nodes[nodeIdx].PartitionID = partitionID
				dht.Nodes[nodeIdx].mu.Unlock()
			}
		}

		// Store partition information
		dht.Partitions[partitionID] = nodeSet
	}

	return nil
}

// HealPartitions reunifies the network
func (dht *ChordDHT) HealPartitions() error {
	dht.mu.Lock()
	defer dht.mu.Unlock()

	// Collect all keys and their values from all partitions
	allData := make(map[string][]DataItem)

	// Iterate through all nodes and collect data
	for _, node := range dht.Nodes {
		node.mu.RLock()
		for key, item := range node.Data {
			if _, exists := allData[key]; !exists {
				allData[key] = make([]DataItem, 0)
			}
			allData[key] = append(allData[key], item)
		}
		node.mu.RUnlock()
	}

	// Resolve conflicts and create a unified view
	resolvedData := make(map[string]DataItem)
	for key, items := range allData {
		if len(items) > 0 {
			// Start with the first item
			resolved := items[0]

			// Merge with other items
			for i := 1; i < len(items); i++ {
				// Merge vector clocks
				merged := resolved.VectorClock.Merge(items[i].VectorClock)

				// Use the value from the "latest" update
				// In a real system, you'd need a better conflict resolution strategy
				if len(items[i].Value) > len(resolved.Value) {
					resolved.Value = items[i].Value
				}

				resolved.VectorClock = merged
			}

			resolvedData[key] = resolved
		}
	}

	// Reunify all nodes - set to partition 0 and update data
	nodeIDs := make([]int, len(dht.Nodes))
	for i, node := range dht.Nodes {
		node.mu.Lock()

		// Reset partition ID
		node.PartitionID = 0
		nodeIDs[i] = node.ID

		// Update with resolved data
		node.Data = make(map[string]DataItem)
		for key, item := range resolvedData {
			node.Data[key] = item
		}

		// Merge all vector clocks
		for _, otherNode := range dht.Nodes {
			if otherNode.ID != node.ID {
				node.vectorClock = node.vectorClock.Merge(otherNode.vectorClock)
			}
		}
		// Unlock the node
		node.mu.Unlock()
	}

	// Reset partitions
	dht.Partitions = make(map[int][]int)
	dht.Partitions[0] = nodeIDs

	return nil
}

// PrintStatus displays the current status of the DHT
func (dht *ChordDHT) PrintStatus() {
	dht.mu.RLock()
	defer dht.mu.RUnlock()

	fmt.Println("=== DHT Status ===")

	// Print partitions
	fmt.Println("Partitions:")
	for partitionID, nodeIndices := range dht.Partitions {
		fmt.Printf("  Partition %d: Nodes %v\n", partitionID, nodeIndices)
	}

	// Print node data
	fmt.Println("\nNodes and Data:")
	for i, node := range dht.Nodes {
		node.mu.RLock()
		fmt.Printf("  Node %d (Partition: %d):\n", i, node.PartitionID)

		if len(node.Data) > 0 {
			fmt.Println("    Data:")
			for key, item := range node.Data {
				fmt.Printf("      %s: %s\n", key, item.Value)
			}
		} else {
			fmt.Println("    No data stored")
		}

		node.mu.RUnlock()
	}

	fmt.Println("=================")
}

func main() {
	// Initialize the DHT
	var dht *ChordDHT
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Simplified Chord DHT")
	fmt.Println("Type 'HELP' for available commands")

	// Command loop
	for {
		fmt.Print(">")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		args := strings.Fields(input)

		// Handle empty input
		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])

		// Use a switch statement for command handling
		switch command {
		case "START":
			if len(args) != 2 {
				fmt.Println("Usage: START <num_nodes>")
				continue
			}

			// Check if the argument is a valid integer
			numNodes, err := strconv.Atoi(args[1])
			if err != nil || numNodes <= 0 {
				fmt.Println("Number of nodes must be a positive integer")
				continue
			}

			dht = NewChordDHT(numNodes)
			fmt.Printf("Started DHT with %d nodes\n", numNodes)

		case "PUT":
			if dht == nil {
				fmt.Println("DHT not started. Use START <num_nodes> first.")
				continue
			}

			if len(args) < 3 {
				fmt.Println("Usage: PUT <key> <value>")
				continue
			}

			key := args[1]
			value := strings.Join(args[2:], " ")

			err := dht.Put(key, value, 0) // Start from the first node
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Successfully stored key '%s'\n", key)
			}

		case "GET":
			if dht == nil {
				fmt.Println("DHT not started. Use START <num_nodes> first.")
				continue
			}

			if len(args) != 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}

			key := args[1]
			value, err := dht.Get(key, 0) // Start from the first node

			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("%s\n", value)
			}

		case "PARTITION":
			if dht == nil {
				fmt.Println("DHT not started. Use START <num_nodes> first.")
				continue
			}

			if len(args) < 2 {
				fmt.Println("Usage: PARTITION <nodes_partition1> <nodes_partition2> ...")
				fmt.Println("Example: PARTITION 0,1 2,3,4")
				continue
			}

			nodeSets := make([][]int, len(args)-1)
			for i := 1; i < len(args); i++ {
				nodeIDs := strings.Split(args[i], ",")
				nodeSet := make([]int, len(nodeIDs))

				for j, idStr := range nodeIDs {
					id, err := strconv.Atoi(idStr)
					if err != nil {
						fmt.Printf("Invalid node ID: %s\n", idStr)
						continue
					}
					nodeSet[j] = id
				}

				nodeSets[i-1] = nodeSet
			}

			err := dht.CreatePartition(nodeSets)
			if err != nil {
				fmt.Printf("Error creating partition: %v\n", err)
			} else {
				fmt.Println("Network partitioned successfully")
			}

		case "HEAL":
			if dht == nil {
				fmt.Println("DHT not started. Use START <num_nodes> first.")
				continue
			}

			err := dht.HealPartitions()
			if err != nil {
				fmt.Printf("Error healing partitions: %v\n", err)
			} else {
				fmt.Println("Network healed successfully")
			}

		case "STATUS":
			if dht == nil {
				fmt.Println("DHT not started. Use START <num_nodes> first.")
				continue
			}

			dht.PrintStatus()

		case "HELP":
			fmt.Println("Available commands:")
			fmt.Println("  START <num_nodes>                   - Start DHT with the specified number of nodes")
			fmt.Println("  PUT <key> <value>                   - Store a key-value pair")
			fmt.Println("  GET <key>                           - Retrieve a value")
			fmt.Println("  PARTITION <nodes1> <nodes2> ...     - Create network partition")
			fmt.Println("  HEAL                                - Resolve network partitions")
			fmt.Println("  STATUS                              - Show system state")
			fmt.Println("  HELP                                - Show this help")
			fmt.Println("  EXIT                                - Exit the program")
			fmt.Println("")
			fmt.Println("Example sequence:")
			fmt.Println("  START 5")
			fmt.Println("  PUT key1 value1")
			fmt.Println("  PARTITION 0,1 2,3,4")
			fmt.Println("  PUT key2 value2")
			fmt.Println("  STATUS")
			fmt.Println("  HEAL")
			fmt.Println("  GET key1")

		case "EXIT", "QUIT":
			fmt.Println("Exiting...")
			return

		default:
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Type 'HELP' for available commands")
		}
	}
}
 