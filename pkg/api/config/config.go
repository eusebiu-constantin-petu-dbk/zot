package config

import (
	"fmt"
	"time"

	glob "github.com/bmatcuk/doublestar/v4"
	"github.com/getlantern/deepcopy"
	distspec "github.com/opencontainers/distribution-spec/specs-go"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"zotregistry.io/zot/errors"
	extconf "zotregistry.io/zot/pkg/extensions/config"
	"zotregistry.io/zot/pkg/storage"
)

var (
	Commit     string // nolint: gochecknoglobals
	BinaryType string // nolint: gochecknoglobals
	GoVersion  string // nolint: gochecknoglobals
)

type StorageConfig struct {
	RootDirectory string
	GC            bool
	Dedupe        bool
	Commit        bool
	GCDelay       time.Duration
	GCInterval    time.Duration
	StorageDriver map[string]interface{} `mapstructure:",omitempty"`
}

type TLSConfig struct {
	Cert   string
	Key    string
	CACert string
}

type AuthHTPasswd struct {
	Path string
}

type AuthConfig struct {
	FailDelay int
	HTPasswd  AuthHTPasswd
	LDAP      *LDAPConfig
	Bearer    *BearerConfig
}

type BearerConfig struct {
	Realm   string
	Service string
	Cert    string
}

type MethodRatelimitConfig struct {
	Method string
	Rate   int
}

type RatelimitConfig struct {
	Rate    *int                    // requests per second
	Methods []MethodRatelimitConfig `mapstructure:",omitempty"`
}

type HTTPConfig struct {
	Address          string
	Port             string
	AllowOrigin      string // comma separated
	TLS              *TLSConfig
	Auth             *AuthConfig
	RawAccessControl map[string]interface{} `mapstructure:"accessControl,omitempty"`
	Realm            string
	AllowReadAccess  bool             `mapstructure:",omitempty"`
	ReadOnly         bool             `mapstructure:",omitempty"`
	Ratelimit        *RatelimitConfig `mapstructure:",omitempty"`
}

type LDAPConfig struct {
	Port          int
	Insecure      bool
	StartTLS      bool // if !Insecure, then StartTLS or LDAPs
	SkipVerify    bool
	SubtreeSearch bool
	Address       string
	BindDN        string
	BindPassword  string
	BaseDN        string
	UserAttribute string
	CACert        string
}

type LogConfig struct {
	Level  string
	Output string
	Audit  string
}

type GlobalStorageConfig struct {
	Dedupe        bool
	GC            bool
	Commit        bool
	GCDelay       time.Duration
	GCInterval    time.Duration
	RootDirectory string
	StorageDriver map[string]interface{} `mapstructure:",omitempty"`
	SubPaths      map[string]StorageConfig
}

type AccessControlConfig struct {
	Repositories Repositories
	AdminPolicy  Policy
}

type Repositories map[string]PolicyGroup

type PolicyGroup struct {
	Policies      []Policy
	DefaultPolicy []string
}

type Policy struct {
	Users   []string
	Actions []string
}

type Config struct {
	DistSpecVersion string `json:"distSpecVersion" mapstructure:"distSpecVersion"`
	GoVersion       string
	Commit          string
	BinaryType      string
	AccessControl   *AccessControlConfig
	Storage         GlobalStorageConfig
	HTTP            HTTPConfig
	Log             *LogConfig
	Extensions      *extconf.ExtensionConfig
}

func New() *Config {
	return &Config{
		DistSpecVersion: distspec.Version,
		GoVersion:       GoVersion,
		Commit:          Commit,
		BinaryType:      BinaryType,
		Storage:         GlobalStorageConfig{GC: true, GCDelay: storage.DefaultGCDelay, Dedupe: true},
		HTTP:            HTTPConfig{Address: "127.0.0.1", Port: "8080", Auth: &AuthConfig{FailDelay: 0}},
		Log:             &LogConfig{Level: "debug"},
	}
}

// Sanitize makes a sanitized copy of the config removing any secrets.
func (c *Config) Sanitize() *Config {
	sanitizedConfig := &Config{}
	if err := deepcopy.Copy(sanitizedConfig, c); err != nil {
		panic(err)
	}

	if c.HTTP.Auth != nil && c.HTTP.Auth.LDAP != nil && c.HTTP.Auth.LDAP.BindPassword != "" {
		sanitizedConfig.HTTP.Auth.LDAP = &LDAPConfig{}

		if err := deepcopy.Copy(sanitizedConfig.HTTP.Auth.LDAP, c.HTTP.Auth.LDAP); err != nil {
			panic(err)
		}

		sanitizedConfig.HTTP.Auth.LDAP.BindPassword = "******"
	}

	return sanitizedConfig
}

// LoadAccessControlConfig populates config.AccessControl struct with values from config.
func (c *Config) LoadAccessControlConfig(viperInstance *viper.Viper) error {
	if c.HTTP.RawAccessControl == nil {
		return nil
	}

	c.AccessControl = &AccessControlConfig{}
	c.AccessControl.Repositories = make(map[string]PolicyGroup)

	for policy := range c.HTTP.RawAccessControl {
		var policies []Policy

		var policyGroup PolicyGroup

		if policy == "adminpolicy" {
			adminPolicy := viperInstance.GetStringMapStringSlice("http::accessControl::adminPolicy")
			c.AccessControl.AdminPolicy.Actions = adminPolicy["actions"]
			c.AccessControl.AdminPolicy.Users = adminPolicy["users"]

			continue
		}

		err := viperInstance.UnmarshalKey(fmt.Sprintf("http::accessControl::%s::policies", policy), &policies)
		if err != nil {
			return err
		}

		defaultPolicy := viperInstance.GetStringSlice(fmt.Sprintf("http::accessControl::%s::defaultPolicy", policy))
		policyGroup.Policies = policies
		policyGroup.DefaultPolicy = defaultPolicy
		c.AccessControl.Repositories[policy] = policyGroup
	}

	return nil
}

func (c *Config) Validate() error {
	if err := validateGC(c); err != nil {
		return err
	}

	if err := validateLDAP(c); err != nil {
		return err
	}

	if err := validateSync(c); err != nil {
		return err
	}

	// check authorization config, it should have basic auth enabled or ldap
	if c.HTTP.RawAccessControl != nil {
		if c.HTTP.Auth == nil || (c.HTTP.Auth.HTPasswd.Path == "" && c.HTTP.Auth.LDAP == nil) {
			// checking for default policy only authorization config: no users, no policies but default policy
			err := checkForDefaultPolicyConfig(c)
			if err != nil {
				return err
			}
		}
	}

	if len(c.Storage.StorageDriver) != 0 {
		// enforce s3 driver in case of using storage driver
		if c.Storage.StorageDriver["name"] != storage.S3StorageDriverName {
			log.Error().Err(errors.ErrBadConfig).Msgf("unsupported storage driver: %s", c.Storage.StorageDriver["name"])

			return errors.ErrBadConfig
		}

		// enforce filesystem storage in case sync feature is enabled
		if c.Extensions != nil && c.Extensions.Sync != nil {
			log.Error().Err(errors.ErrBadConfig).Msg("sync supports only filesystem storage")

			return errors.ErrBadConfig
		}
	}

	// enforce s3 driver on subpaths in case of using storage driver
	if c.Storage.SubPaths != nil {
		if len(c.Storage.SubPaths) > 0 {
			subPaths := c.Storage.SubPaths

			for route, storageConfig := range subPaths {
				if len(storageConfig.StorageDriver) != 0 {
					if storageConfig.StorageDriver["name"] != storage.S3StorageDriverName {
						log.Error().Err(errors.ErrBadConfig).Str("subpath",
							route).Msgf("unsupported storage driver: %s", storageConfig.StorageDriver["name"])

						return errors.ErrBadConfig
					}
				}
			}
		}
	}

	// check glob patterns in authz config are compilable
	if c.AccessControl != nil {
		for pattern := range c.AccessControl.Repositories {
			ok := glob.ValidatePattern(pattern)
			if !ok {
				log.Info().Msg("asa")
				log.Error().Err(glob.ErrBadPattern).Str("pattern", pattern).Msg("authorization pattern could not be compiled")

				return glob.ErrBadPattern
			}
		}
	}

	updateDistSpecVersion(c)

	return nil
}

func updateDistSpecVersion(config *Config) {
	if config.DistSpecVersion == distspec.Version {
		return
	}

	log.Warn().
		Msgf("config dist-spec version: %s differs from version actually used: %s",
			config.DistSpecVersion, distspec.Version)

	config.DistSpecVersion = distspec.Version
}

func checkForDefaultPolicyConfig(config *Config) error {
	if !isDefaultPolicyConfig(config) {
		log.Error().Err(errors.ErrBadConfig).
			Msg("access control config requires httpasswd, ldap authentication " +
				"or using only 'defaultPolicy' policies")
		return errors.ErrBadConfig
	}

	return nil
}

func isDefaultPolicyConfig(cfg *Config) bool {
	adminPolicy := cfg.AccessControl.AdminPolicy

	log.Info().Msg("checking if default authorization is possible")

	if len(adminPolicy.Actions)+len(adminPolicy.Users) > 0 {
		log.Info().Msg("admin policy detected, default authorization disabled")

		return false
	}

	for _, repository := range cfg.AccessControl.Repositories {
		for _, policy := range repository.Policies {
			if len(policy.Actions)+len(policy.Users) > 0 {
				log.Info().Interface("repository", repository).
					Msg("repository with non-empty policy detected, default authorization disabled")

				return false
			}
		}
	}

	log.Info().Msg("default authorization detected")

	return true
}

func validateLDAP(config *Config) error {
	// LDAP mandatory configuration
	if config.HTTP.Auth != nil && config.HTTP.Auth.LDAP != nil {
		ldap := config.HTTP.Auth.LDAP
		if ldap.UserAttribute == "" {
			log.Error().Str("userAttribute", ldap.UserAttribute).
				Msg("invalid LDAP configuration, missing mandatory key: userAttribute")

			return errors.ErrLDAPConfig
		}

		if ldap.Address == "" {
			log.Error().Str("address", ldap.Address).
				Msg("invalid LDAP configuration, missing mandatory key: address")

			return errors.ErrLDAPConfig
		}

		if ldap.BaseDN == "" {
			log.Error().Str("basedn", ldap.BaseDN).
				Msg("invalid LDAP configuration, missing mandatory key: basedn")

			return errors.ErrLDAPConfig
		}
	}

	return nil
}

func validateGC(config *Config) error {
	// enforce GC params
	if config.Storage.GCDelay < 0 {
		log.Error().Err(errors.ErrBadConfig).
			Msgf("invalid garbage-collect delay %v specified", config.Storage.GCDelay)

		return errors.ErrBadConfig
	}

	if config.Storage.GCInterval < 0 {
		log.Error().Err(errors.ErrBadConfig).
			Msgf("invalid garbage-collect interval %v specified", config.Storage.GCInterval)

		return errors.ErrBadConfig
	}

	if !config.Storage.GC {
		if config.Storage.GCDelay != 0 {
			log.Warn().Err(errors.ErrBadConfig).
				Msg("garbage-collect delay specified without enabling garbage-collect, will be ignored")
		}

		if config.Storage.GCInterval != 0 {
			log.Warn().Err(errors.ErrBadConfig).
				Msg("periodic garbage-collect interval specified without enabling garbage-collect, will be ignored")
		}
	}

	return nil
}

func validateSync(config *Config) error {
	// check glob patterns in sync config are compilable
	if config.Extensions != nil && config.Extensions.Sync != nil {
		for id, regCfg := range config.Extensions.Sync.Registries {
			// check retry options are configured for sync
			if regCfg.MaxRetries != nil && regCfg.RetryDelay == nil {
				log.Error().Err(errors.ErrBadConfig).Msgf("extensions.sync.registries[%d].retryDelay"+
					" is required when using extensions.sync.registries[%d].maxRetries", id, id)

				return errors.ErrBadConfig
			}

			if regCfg.Content != nil {
				for _, content := range regCfg.Content {
					ok := glob.ValidatePattern(content.Prefix)
					if !ok {
						log.Error().Err(glob.ErrBadPattern).Str("pattern", content.Prefix).Msg("sync pattern could not be compiled")

						return glob.ErrBadPattern
					}
				}
			}
		}
	}

	return nil
}
