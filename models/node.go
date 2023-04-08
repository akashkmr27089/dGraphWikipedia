package models

type Node struct {
	Uid            string `json:"uid,omitempty"`
	Name           string `json:"name,omitempty"`
	Parent         *Node  `json:"parent,omitempty"`
	ReferenceCount int    `json:"reference_count"`
}
