//go:build extended
// +build extended

package lint_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/resty.v1"
	"zotregistry.io/zot/pkg/api"
	"zotregistry.io/zot/pkg/api/config"
	"zotregistry.io/zot/pkg/extensions/lint"
	"zotregistry.io/zot/pkg/test"
)

const (
	username               = "test"
	passphrase             = "test"
	ServerCert             = "../../test/data/server.cert"
	ServerKey              = "../../test/data/server.key"
	CACert                 = "../../test/data/ca.crt"
	AuthorizedNamespace    = "everyone/isallowed"
	UnauthorizedNamespace  = "fortknox/notallowed"
	ALICE                  = "alice"
	AuthorizationNamespace = "authz/image"
	AuthorizationAllRepos  = "**"
)

func TestVerifyMandatoryAnnotations(t *testing.T) {
	port := test.GetFreePort()
	baseURL := test.GetBaseURL(port)

	conf := config.New()
	conf.HTTP.Port = port

	ctlr := api.NewController(conf)
	dir := t.TempDir()

	err := test.CopyFiles("../../../test/data", dir)
	if err != nil {
		panic(err)
	}

	ctlr.Config.Storage.RootDirectory = dir

	go startServer(ctlr)
	defer stopServer(ctlr)
	test.WaitTillServerReady(baseURL)

	Convey("Mandatory annotations disabled", t, func() {
		resp, err := resty.R().SetBasicAuth(username, passphrase).
			Get(baseURL + "/v2/zot-test/manifests/0.0.1")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		manifestBlob := resp.Body()
		var manifest ispec.Manifest
		err = json.Unmarshal(manifestBlob, &manifest)
		So(err, ShouldBeNil)

		pass := lint.CheckMandatoryAnnotations(manifest, []string{}, false)
		So(pass, ShouldBeTrue)
	})

	Convey("Mandatory annotations enabled, but no list in config", t, func() {
		resp, err := resty.R().SetBasicAuth(username, passphrase).
			Get(baseURL + "/v2/zot-test/manifests/0.0.1")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		manifestBlob := resp.Body()
		var manifest ispec.Manifest
		err = json.Unmarshal(manifestBlob, &manifest)
		So(err, ShouldBeNil)

		pass := lint.CheckMandatoryAnnotations(manifest, []string{}, true)
		So(pass, ShouldBeTrue)
	})

	Convey("Mandatory annotations verification passing", t, func() {
		resp, err := resty.R().SetBasicAuth(username, passphrase).
			Get(baseURL + "/v2/zot-test/manifests/0.0.1")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		manifestBlob := resp.Body()
		var manifest ispec.Manifest
		err = json.Unmarshal(manifestBlob, &manifest)
		So(err, ShouldBeNil)

		manifest.Annotations = make(map[string]string)
		manifest.Annotations["annotation1"] = "test"
		manifest.Annotations["annotation2"] = "test2"
		manifest.Annotations["annotation3"] = "test string"

		pass := lint.CheckMandatoryAnnotations(manifest, []string{"annotation1", "annotation2", "annotation3"}, true)
		So(pass, ShouldBeTrue)
	})

	Convey("Mandatory annotations incomplete in manifest", t, func() {
		resp, err := resty.R().SetBasicAuth(username, passphrase).
			Get(baseURL + "/v2/zot-test/manifests/0.0.1")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		manifestBlob := resp.Body()
		var manifest ispec.Manifest
		err = json.Unmarshal(manifestBlob, &manifest)
		So(err, ShouldBeNil)

		manifest.Annotations = make(map[string]string)
		manifest.Annotations["annotation1"] = "test1"
		manifest.Annotations["annotation3"] = "test3"

		pass := lint.CheckMandatoryAnnotations(manifest, []string{"annotation1", "annotation2", "annotation3"}, true)
		So(pass, ShouldBeFalse)
	})

	Convey("Mandatory annotations verification passing - more annotations than the mandatory list", t, func() {
		resp, err := resty.R().SetBasicAuth(username, passphrase).
			Get(baseURL + "/v2/zot-test/manifests/0.0.1")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		manifestBlob := resp.Body()
		var manifest ispec.Manifest
		err = json.Unmarshal(manifestBlob, &manifest)
		So(err, ShouldBeNil)

		manifest.Annotations = make(map[string]string)
		manifest.Annotations["annotation1"] = "test1"
		manifest.Annotations["annotation2"] = "test2"
		manifest.Annotations["annotation3"] = "test3"

		pass := lint.CheckMandatoryAnnotations(manifest, []string{"annotation1", "annotation3"}, true)
		So(pass, ShouldBeTrue)
	})
}

func startServer(c *api.Controller) {
	// this blocks
	ctx := context.Background()
	if err := c.Run(ctx); err != nil {
		return
	}
}

func stopServer(c *api.Controller) {
	ctx := context.Background()
	_ = c.Server.Shutdown(ctx)
}
