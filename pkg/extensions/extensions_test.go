//go:build sync || metrics || mgmt
// +build sync metrics mgmt

package extensions_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/resty.v1"

	"zotregistry.io/zot/pkg/api"
	"zotregistry.io/zot/pkg/api/config"
	"zotregistry.io/zot/pkg/api/constants"
	"zotregistry.io/zot/pkg/extensions"
	extconf "zotregistry.io/zot/pkg/extensions/config"
	syncconf "zotregistry.io/zot/pkg/extensions/config/sync"
	"zotregistry.io/zot/pkg/test"
)

const (
	ServerCert = "../../test/data/server.cert"
	ServerKey  = "../../test/data/server.key"
)

func TestEnableExtension(t *testing.T) {
	Convey("Verify log if sync disabled in config", t, func() {
		globalDir := t.TempDir()
		port := test.GetFreePort()
		conf := config.New()
		falseValue := false

		syncConfig := &syncconf.Config{
			Enable:     &falseValue,
			Registries: []syncconf.RegistryConfig{},
		}

		// conf.Extensions.Sync.Enable = &falseValue
		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Sync = syncConfig
		conf.HTTP.Port = port

		logFile, err := os.CreateTemp(globalDir, "zot-log*.txt")
		So(err, ShouldBeNil)
		conf.Log.Level = "info"
		conf.Log.Output = logFile.Name()
		defer os.Remove(logFile.Name()) // cleanup

		ctlr := api.NewController(conf)
		ctlrManager := test.NewControllerManager(ctlr)

		defer ctlrManager.StopServer()

		ctlr.Config.Storage.RootDirectory = globalDir

		ctlrManager.StartAndWait(port)

		data, err := os.ReadFile(logFile.Name())
		So(err, ShouldBeNil)
		So(string(data), ShouldContainSubstring,
			"Sync registries config not provided or disabled, skipping sync")
	})
}

func TestMetricsExtension(t *testing.T) {
	Convey("Verify Metrics enabled for storage subpaths", t, func() {
		globalDir := t.TempDir()
		conf := config.New()
		port := test.GetFreePort()
		conf.HTTP.Port = port

		logFile, err := os.CreateTemp(globalDir, "zot-log*.txt")
		So(err, ShouldBeNil)
		defaultValue := true

		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Metrics = &extconf.MetricsConfig{
			BaseConfig: extconf.BaseConfig{Enable: &defaultValue},
			Prometheus: &extconf.PrometheusConfig{},
		}
		conf.Log.Level = "info"
		conf.Log.Output = logFile.Name()
		defer os.Remove(logFile.Name()) // cleanup

		ctlr := api.NewController(conf)
		ctlrManager := test.NewControllerManager(ctlr)

		subPaths := make(map[string]config.StorageConfig)
		subPaths["/a"] = config.StorageConfig{
			Dedupe:        false,
			RootDirectory: t.TempDir(),
		}

		ctlr.Config.Storage.RootDirectory = globalDir
		ctlr.Config.Storage.SubPaths = subPaths

		ctlrManager.StartAndWait(port)

		data, _ := os.ReadFile(logFile.Name())

		So(string(data), ShouldContainSubstring,
			"Prometheus instrumentation Path not set, changing to '/metrics'.")
	})
}

func TestMgmtExtension(t *testing.T) {
	globalDir := t.TempDir()
	conf := config.New()
	port := test.GetFreePort()
	conf.HTTP.Port = port
	baseURL := test.GetBaseURL(port)

	logFile, err := os.CreateTemp(globalDir, "zot-log*.txt")
	if err != nil {
		panic(err)
	}

	defaultValue := true

	Convey("Verify mgmt route enabled with htpasswd", t, func() {
		htpasswdPath := test.MakeHtpasswdFile()
		conf.HTTP.Auth.HTPasswd.Path = htpasswdPath

		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Mgmt = &extconf.MgmtConfig{
			BaseConfig: extconf.BaseConfig{
				Enable: &defaultValue,
			},
		}

		conf.Log.Output = logFile.Name()
		defer os.Remove(logFile.Name()) // cleanup

		ctlr := api.NewController(conf)

		subPaths := make(map[string]config.StorageConfig)
		subPaths["/a"] = config.StorageConfig{}

		ctlr.Config.Storage.RootDirectory = globalDir
		ctlr.Config.Storage.SubPaths = subPaths

		ctlrManager := test.NewControllerManager(ctlr)
		ctlrManager.StartAndWait(port)
		defer ctlrManager.StopServer()

		data, _ := os.ReadFile(logFile.Name())

		So(string(data), ShouldContainSubstring, "setting up mgmt routes")

		// without credentials
		resp, err := resty.R().Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp := extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd.Path, ShouldEqual, "")
		So(mgmtResp.HTTP.Auth.Bearer, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)

		// with credentials
		resp, err = resty.R().SetBasicAuth("test", "test").Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp = extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd.Path, ShouldEqual, "")
		So(mgmtResp.HTTP.Auth.Bearer, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)

		// with wrong credentials
		resp, err = resty.R().SetBasicAuth("test", "wrong").Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusUnauthorized)
	})

	Convey("Verify mgmt route enabled with ldap", t, func() {
		conf.HTTP.Auth.LDAP = &config.LDAPConfig{
			BindDN:  "binddn",
			BaseDN:  "basedn",
			Address: "ldapexample",
		}

		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Mgmt = &extconf.MgmtConfig{
			BaseConfig: extconf.BaseConfig{
				Enable: &defaultValue,
			},
		}

		conf.Log.Output = logFile.Name()
		defer os.Remove(logFile.Name()) // cleanup

		ctlr := api.NewController(conf)

		subPaths := make(map[string]config.StorageConfig)
		subPaths["/a"] = config.StorageConfig{}

		ctlr.Config.Storage.RootDirectory = globalDir
		ctlr.Config.Storage.SubPaths = subPaths

		ctlrManager := test.NewControllerManager(ctlr)
		ctlrManager.StartAndWait(port)
		defer ctlrManager.StopServer()

		data, _ := os.ReadFile(logFile.Name())

		So(string(data), ShouldContainSubstring, "setting up mgmt routes")

		// without credentials
		resp, err := resty.R().Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp := extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd.Path, ShouldEqual, "")
		// ldap is always nil, htpasswd should be populated when ldap is used
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldBeNil)
	})

	Convey("Verify mgmt route enabled with htpasswd + ldap", t, func() {
		htpasswdPath := test.MakeHtpasswdFile()
		conf.HTTP.Auth.HTPasswd.Path = htpasswdPath
		conf.HTTP.Auth.LDAP = &config.LDAPConfig{
			BindDN:  "binddn",
			BaseDN:  "basedn",
			Address: "ldapexample",
		}

		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Mgmt = &extconf.MgmtConfig{
			BaseConfig: extconf.BaseConfig{
				Enable: &defaultValue,
			},
		}

		conf.Log.Output = logFile.Name()
		defer os.Remove(logFile.Name()) // cleanup

		ctlr := api.NewController(conf)

		subPaths := make(map[string]config.StorageConfig)
		subPaths["/a"] = config.StorageConfig{}

		ctlr.Config.Storage.RootDirectory = globalDir
		ctlr.Config.Storage.SubPaths = subPaths

		ctlrManager := test.NewControllerManager(ctlr)
		ctlrManager.StartAndWait(port)
		defer ctlrManager.StopServer()

		data, _ := os.ReadFile(logFile.Name())

		So(string(data), ShouldContainSubstring, "setting up mgmt routes")

		// without credentials
		resp, err := resty.R().Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp := extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd.Path, ShouldEqual, "")
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldBeNil)

		// with credentials
		resp, err = resty.R().SetBasicAuth("test", "test").Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp = extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd.Path, ShouldEqual, "")
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldBeNil)
	})

	Convey("Verify mgmt route enabled with htpasswd + ldap + bearer", t, func() {
		htpasswdPath := test.MakeHtpasswdFile()
		conf.HTTP.Auth.HTPasswd.Path = htpasswdPath
		conf.HTTP.Auth.LDAP = &config.LDAPConfig{
			BindDN:  "binddn",
			BaseDN:  "basedn",
			Address: "ldapexample",
		}

		conf.HTTP.Auth.Bearer = &config.BearerConfig{
			Realm:   "realm",
			Service: "service",
		}

		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Mgmt = &extconf.MgmtConfig{
			BaseConfig: extconf.BaseConfig{
				Enable: &defaultValue,
			},
		}

		conf.Log.Output = logFile.Name()
		defer os.Remove(logFile.Name()) // cleanup

		ctlr := api.NewController(conf)

		subPaths := make(map[string]config.StorageConfig)
		subPaths["/a"] = config.StorageConfig{}

		ctlr.Config.Storage.RootDirectory = globalDir
		ctlr.Config.Storage.SubPaths = subPaths

		ctlrManager := test.NewControllerManager(ctlr)
		ctlrManager.StartAndWait(port)
		defer ctlrManager.StopServer()

		data, _ := os.ReadFile(logFile.Name())

		So(string(data), ShouldContainSubstring, "setting up mgmt routes")

		// without credentials
		resp, err := resty.R().Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp := extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd.Path, ShouldEqual, "")
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.Bearer.Realm, ShouldEqual, "realm")
		So(mgmtResp.HTTP.Auth.Bearer.Service, ShouldEqual, "service")

		// with credentials
		resp, err = resty.R().SetBasicAuth("test", "test").Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp = extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd.Path, ShouldEqual, "")
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.Bearer.Realm, ShouldEqual, "realm")
		So(mgmtResp.HTTP.Auth.Bearer.Service, ShouldEqual, "service")
	})

	Convey("Verify mgmt route enabled with ldap + bearer", t, func() {
		conf.HTTP.Auth.HTPasswd.Path = ""
		conf.HTTP.Auth.LDAP = &config.LDAPConfig{
			BindDN:  "binddn",
			BaseDN:  "basedn",
			Address: "ldapexample",
		}

		conf.HTTP.Auth.Bearer = &config.BearerConfig{
			Realm:   "realm",
			Service: "service",
		}

		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Mgmt = &extconf.MgmtConfig{
			BaseConfig: extconf.BaseConfig{
				Enable: &defaultValue,
			},
		}

		conf.Log.Output = logFile.Name()
		defer os.Remove(logFile.Name()) // cleanup

		ctlr := api.NewController(conf)

		subPaths := make(map[string]config.StorageConfig)
		subPaths["/a"] = config.StorageConfig{}

		ctlr.Config.Storage.RootDirectory = globalDir
		ctlr.Config.Storage.SubPaths = subPaths

		ctlrManager := test.NewControllerManager(ctlr)
		ctlrManager.StartAndWait(port)
		defer ctlrManager.StopServer()

		data, _ := os.ReadFile(logFile.Name())

		So(string(data), ShouldContainSubstring, "setting up mgmt routes")

		// without credentials
		resp, err := resty.R().Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp := extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd.Path, ShouldEqual, "")
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.Bearer.Realm, ShouldEqual, "realm")
		So(mgmtResp.HTTP.Auth.Bearer.Service, ShouldEqual, "service")
	})

	Convey("Verify mgmt route enabled with bearer", t, func() {
		conf.HTTP.Auth.HTPasswd.Path = ""
		conf.HTTP.Auth.LDAP = nil
		conf.HTTP.Auth.Bearer = &config.BearerConfig{
			Realm:   "realm",
			Service: "service",
		}

		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Mgmt = &extconf.MgmtConfig{
			BaseConfig: extconf.BaseConfig{
				Enable: &defaultValue,
			},
		}

		conf.Log.Output = logFile.Name()
		defer os.Remove(logFile.Name()) // cleanup

		ctlr := api.NewController(conf)

		subPaths := make(map[string]config.StorageConfig)
		subPaths["/a"] = config.StorageConfig{}

		ctlr.Config.Storage.RootDirectory = globalDir
		ctlr.Config.Storage.SubPaths = subPaths

		ctlrManager := test.NewControllerManager(ctlr)
		ctlrManager.StartAndWait(port)
		defer ctlrManager.StopServer()

		data, _ := os.ReadFile(logFile.Name())

		So(string(data), ShouldContainSubstring, "setting up mgmt routes")

		// without credentials
		resp, err := resty.R().Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp := extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.Bearer.Realm, ShouldEqual, "realm")
		So(mgmtResp.HTTP.Auth.Bearer.Service, ShouldEqual, "service")
	})

	Convey("Verify mgmt route enabled without any auth", t, func() {
		globalDir := t.TempDir()
		conf := config.New()
		port := test.GetFreePort()
		conf.HTTP.Port = port
		baseURL := test.GetBaseURL(port)

		logFile, err := os.CreateTemp(globalDir, "zot-log*.txt")
		So(err, ShouldBeNil)
		defaultValue := true

		conf.Commit = "v1.0.0"

		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Mgmt = &extconf.MgmtConfig{
			BaseConfig: extconf.BaseConfig{
				Enable: &defaultValue,
			},
		}

		conf.Log.Output = logFile.Name()
		defer os.Remove(logFile.Name()) // cleanup

		ctlr := api.NewController(conf)

		subPaths := make(map[string]config.StorageConfig)
		subPaths["/a"] = config.StorageConfig{}

		ctlr.Config.Storage.RootDirectory = globalDir
		ctlr.Config.Storage.SubPaths = subPaths

		ctlrManager := test.NewControllerManager(ctlr)
		ctlrManager.StartAndWait(port)
		defer ctlrManager.StopServer()

		resp, err := resty.R().Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp := extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.DistSpecVersion, ShouldResemble, conf.DistSpecVersion)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)

		data, _ := os.ReadFile(logFile.Name())
		So(string(data), ShouldContainSubstring, "setting up mgmt routes")
	})
}

func TestMgmtWithBearer(t *testing.T) {
	Convey("Make a new controller", t, func() {
		authorizedNamespace := "allowedrepo"
		unauthorizedNamespace := "notallowedrepo"
		authTestServer := test.MakeAuthTestServer(ServerKey, unauthorizedNamespace)
		defer authTestServer.Close()

		port := test.GetFreePort()
		baseURL := test.GetBaseURL(port)

		conf := config.New()
		conf.HTTP.Port = port

		aurl, err := url.Parse(authTestServer.URL)
		So(err, ShouldBeNil)

		conf.HTTP.Auth = &config.AuthConfig{
			Bearer: &config.BearerConfig{
				Cert:    ServerCert,
				Realm:   authTestServer.URL + "/auth/token",
				Service: aurl.Host,
			},
		}

		defaultValue := true

		conf.Extensions = &extconf.ExtensionConfig{}
		conf.Extensions.Mgmt = &extconf.MgmtConfig{
			BaseConfig: extconf.BaseConfig{
				Enable: &defaultValue,
			},
		}

		conf.Storage.RootDirectory = t.TempDir()

		ctlr := api.NewController(conf)

		cm := test.NewControllerManager(ctlr)
		cm.StartAndWait(port)
		defer cm.StopServer()

		resp, err := resty.R().Get(baseURL + "/v2/")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusUnauthorized)

		authorizationHeader := test.ParseBearerAuthHeader(resp.Header().Get("Www-Authenticate"))
		resp, err = resty.R().
			SetQueryParam("service", authorizationHeader.Service).
			SetQueryParam("scope", authorizationHeader.Scope).
			Get(authorizationHeader.Realm)
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)
		var goodToken test.AccessTokenResponse
		err = json.Unmarshal(resp.Body(), &goodToken)
		So(err, ShouldBeNil)

		resp, err = resty.R().
			SetHeader("Authorization", fmt.Sprintf("Bearer %s", goodToken.AccessToken)).
			Get(baseURL + "/v2/")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		resp, err = resty.R().SetHeader("Authorization",
			fmt.Sprintf("Bearer %s", goodToken.AccessToken)).Options(baseURL + "/v2/")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusNoContent)

		resp, err = resty.R().Post(baseURL + "/v2/" + authorizedNamespace + "/blobs/uploads/")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusUnauthorized)

		authorizationHeader = test.ParseBearerAuthHeader(resp.Header().Get("Www-Authenticate"))
		resp, err = resty.R().
			SetQueryParam("service", authorizationHeader.Service).
			SetQueryParam("scope", authorizationHeader.Scope).
			Get(authorizationHeader.Realm)
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)
		err = json.Unmarshal(resp.Body(), &goodToken)
		So(err, ShouldBeNil)

		resp, err = resty.R().
			SetHeader("Authorization", fmt.Sprintf("Bearer %s", goodToken.AccessToken)).
			Post(baseURL + "/v2/" + authorizedNamespace + "/blobs/uploads/")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusAccepted)

		resp, err = resty.R().
			Post(baseURL + "/v2/" + unauthorizedNamespace + "/blobs/uploads/")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusUnauthorized)

		authorizationHeader = test.ParseBearerAuthHeader(resp.Header().Get("Www-Authenticate"))
		resp, err = resty.R().
			SetQueryParam("service", authorizationHeader.Service).
			SetQueryParam("scope", authorizationHeader.Scope).
			Get(authorizationHeader.Realm)
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)
		var badToken test.AccessTokenResponse
		err = json.Unmarshal(resp.Body(), &badToken)
		So(err, ShouldBeNil)

		resp, err = resty.R().
			SetHeader("Authorization", fmt.Sprintf("Bearer %s", badToken.AccessToken)).
			Post(baseURL + "/v2/" + unauthorizedNamespace + "/blobs/uploads/")
		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusUnauthorized)

		// test mgmt route
		resp, err = resty.R().Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp := extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.DistSpecVersion, ShouldResemble, conf.DistSpecVersion)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.Bearer.Realm, ShouldEqual, conf.HTTP.Auth.Bearer.Realm)
		So(mgmtResp.HTTP.Auth.Bearer.Service, ShouldEqual, conf.HTTP.Auth.Bearer.Service)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)

		resp, err = resty.R().SetBasicAuth("", "").Get(baseURL + constants.FullMgmtPrefix)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		mgmtResp = extensions.StrippedConfig{}
		err = json.Unmarshal(resp.Body(), &mgmtResp)
		So(err, ShouldBeNil)
		So(mgmtResp.DistSpecVersion, ShouldResemble, conf.DistSpecVersion)
		So(mgmtResp.HTTP.Auth.Bearer, ShouldNotBeNil)
		So(mgmtResp.HTTP.Auth.Bearer.Realm, ShouldEqual, conf.HTTP.Auth.Bearer.Realm)
		So(mgmtResp.HTTP.Auth.Bearer.Service, ShouldEqual, conf.HTTP.Auth.Bearer.Service)
		So(mgmtResp.HTTP.Auth.HTPasswd, ShouldBeNil)
		So(mgmtResp.HTTP.Auth.LDAP, ShouldBeNil)
	})
}
