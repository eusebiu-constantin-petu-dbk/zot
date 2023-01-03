//go:build sync && scrub && metrics && search
// +build sync,scrub,metrics,search

package api_test

import (
	"net/http"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"zotregistry.io/zot/pkg/api"
	"zotregistry.io/zot/pkg/api/config"
	"zotregistry.io/zot/pkg/test"
)

func TestOpenIDAuthMiddleware(t *testing.T) {
	port := test.GetFreePort()
	baseURL := test.GetBaseURL(port)

	conf := config.New()
	conf.HTTP.Port = port

	htpasswdPath := test.MakeHtpasswdFile()
	defer os.Remove(htpasswdPath)

	// mockOIDCServer, err := mockoidc.Run()
	mockOIDCServer, err := mockOIDCRun()
	if err != nil {
		panic(err)
	}

	defer func() {
		err := mockOIDCServer.Shutdown()
		if err != nil {
			panic(err)
		}
	}()

	// user := ZotMockUser{
	// 	Subject: "123", // should stay that way to pass token verifier
	// 	Email:   "test@test.com",
	// }
	// mockOIDCServer.QueueUser(user)

	mockOIDCConfig := mockOIDCServer.Config()
	conf.HTTP.Auth = &config.AuthConfig{
		HTPasswd: config.AuthHTPasswd{
			Path: htpasswdPath,
		},
		OpenID: &config.OpenIDConfig{
			Providers: map[string]config.OpenIDProviderConfig{
				"dex": {
					ClientID:     mockOIDCConfig.ClientID,
					ClientSecret: mockOIDCConfig.ClientSecret,
					KeyPath:      "",
					Issuer:       mockOIDCConfig.Issuer,
					Scopes:       []string{"openid", "email"},
				},
			},
			SecureCookieEncryptionKey: "test1234test1234",
			SecureCookieHashKey:       "test1234test1234",
			APIKeys:                   true,
			SessionCookieStorePath:    "",
		},
	}
	conf.HTTP.AccessControl = &config.AccessControlConfig{}

	ctlr := api.NewController(conf)
	dir := t.TempDir()

	err = test.CopyFiles("../../test/data", dir)
	if err != nil {
		panic(err)
	}
	ctlr.Config.Storage.RootDirectory = dir

	cm := test.NewControllerManager(ctlr)

	cm.StartServer()
	defer cm.StopServer()
	test.WaitTillServerReady(baseURL)

	rh := api.NewRouteHandler(ctlr)

	defer os.Remove("user.db")

	Convey("test",t,  func(){
		oidmdw := api.NewOpenIDAuthMiddleware(ctlr)
		So(oidmdw, ShouldNotEqual, nil)

		basicmdw := api.NewBasicAuthMiddleware(ctlr)
		So(basicmdw, ShouldNotEqual, nil)

		Convey("SetNext for basicmdw", func ()  {
			basicmdw.SetNext(oidmdw)
			nextmdw := basicmdw.GetNext()
			So(nextmdw, ShouldEqual, oidmdw)
		})
		Convey("SetNext for oidmdw", func ()  {
			oidmdw.SetNext(basicmdw)
			nextmdw := oidmdw.GetNext()
			So(nextmdw, ShouldEqual, basicmdw)
		})
		Convey("OpenID MdwFunc", func(){

			nextHandler := http.HandlerFunc(rh.ListRepositories)
			mdwFunc := oidmdw.MdwFunc(false)

			mdwFunc(nextHandler)
		})


	})
}