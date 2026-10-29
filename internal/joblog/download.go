package joblog

// DownloadContentType identifies framed log downloads. Empty records keep idle
// connections alive; End confirms that the server finished reading the source.
const DownloadContentType = "application/x-ndjson"

type DownloadRecord struct {
	Data  []byte `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
	End   bool   `json:"end,omitempty"`
}
