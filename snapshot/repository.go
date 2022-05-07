package snapshot

type Object struct {
	Checksum    [32]byte
	Chunks      [][32]byte
	ContentType string
}
type Chunk struct {
	Checksum [32]byte
	Start    uint
	Length   uint
}
