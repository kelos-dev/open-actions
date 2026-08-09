package manifests

import (
	"embed"
	"io/fs"
)

//go:embed charts/open-actions
var charts embed.FS

func Chart() fs.FS {
	chart, err := fs.Sub(charts, "charts/open-actions")
	if err != nil {
		panic("load embedded Open Actions Helm chart: " + err.Error())
	}
	return chart
}
