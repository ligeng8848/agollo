package remote

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	crypt "github.com/bketelsen/crypt/config"
	"github.com/shima-park/agollo"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

var (
	ErrUnsupportedProvider = errors.New("This configuration manager is not supported")

	_ viperConfigManager = apolloConfigManager{}
	// getConfigManager方法每次返回新对象导致缓存无效，
	// 这里通过endpoint作为key复一个对象
	// key: endpoint+appid value: agollo.Agollo
	agolloMap sync.Map
)

var (
	// apollod的appid
	appID string
	// 默认为properties，apollo默认配置文件格式
	defaultConfigType = "properties"
	// 默认创建Agollo的Option
	defaultAgolloOptions = []agollo.Option{
		agollo.AutoFetchOnCacheMiss(),
		agollo.FailTolerantOnBackupExists(),
	}
)

func SetAppID(appid string) {
	appID = appid
}

func SetConfigType(ct string) {
	defaultConfigType = ct
}

func SetAgolloOptions(opts ...agollo.Option) {
	defaultAgolloOptions = opts
}

type viperConfigManager interface {
	Get(key string) ([]byte, error)
	GetMultipleNamespaces(keys []string) ([]byte, error)
	Watch(key string, stop chan bool) <-chan *viper.RemoteResponse
	WatchMultipleNamespaces(keys []string, stop chan bool) <-chan *viper.RemoteResponse
}

type apolloConfigManager struct {
	agollo agollo.Agollo
}

func newApolloConfigManager(appid, endpoint string, opts []agollo.Option) (*apolloConfigManager, error) {
	if appid == "" {
		return nil, errors.New("The appid is not set")
	}

	ag, err := newAgollo(appid, endpoint, opts)
	if err != nil {
		return nil, err
	}

	return &apolloConfigManager{
		agollo: ag,
	}, nil

}

func newAgollo(appid, endpoint string, opts []agollo.Option) (agollo.Agollo, error) {
	i, found := agolloMap.Load(endpoint + "/" + appid)
	if !found {
		ag, err := agollo.New(
			endpoint,
			appid,
			opts...,
		)
		if err != nil {
			return nil, err
		}

		// 监听并同步apollo配置
		ag.Start()

		agolloMap.Store(endpoint+"/"+appid, ag)

		return ag, nil
	}
	return i.(agollo.Agollo), nil
}

func (cm apolloConfigManager) Get(namespace string) ([]byte, error) {
	configs := cm.agollo.GetNameSpace(namespace)
	return marshalConfigs(getConfigType(namespace), configs)
}

func (cm apolloConfigManager) GetMultipleNamespaces(namespaces []string) ([]byte, error) {
	newConfigs := make(map[string]interface{})
	var configType string
	var b []byte
	for _, namespace := range namespaces {
		configs := cm.agollo.GetNameSpace(namespace)
		configType = getConfigType(namespace)
		// 根据类型进行合并
		switch configType {
		case "json", "yml", "yaml", "xml":
			content := configs["content"]
			if content != nil {
				var tempConfig map[string]interface{}
				err := unmarshalConfig(configType, []byte(content.(string)), &tempConfig)
				if err != nil {
					return nil, err
				}
				// 执行合并
				viper.MergeConfigMap(tempConfig)
				mergedConfigs := viper.AllSettings()
				b, err = marshalConfig(configType, mergedConfigs)
				if err != nil {
					return nil, err
				}
				newConfigs["content"] = string(b)
			}
		case "properties":
			for k, v := range configs {
				newConfigs[k] = v
			}
		}
	}

	return marshalConfigs(configType, newConfigs)
}

func unmarshalConfig(configType string, data []byte, config *map[string]interface{}) error {
	switch configType {
	case "json":
		return json.Unmarshal(data, config)
	case "yml", "yaml":
		return yaml.Unmarshal(data, config)
	case "xml":
		return xml.Unmarshal(data, config)
	default:
		return fmt.Errorf("unsupported config type: %s", configType)
	}
}

func marshalConfig(configType string, config map[string]interface{}) ([]byte, error) {
	switch configType {
	case "json":
		return json.Marshal(config)
	case "yml", "yaml":
		return yaml.Marshal(config)
	case "xml":
		return xml.Marshal(config)
	default:
		return nil, fmt.Errorf("unsupported config type: %s", configType)
	}
}

func marshalConfigs(configType string, configs map[string]interface{}) ([]byte, error) {
	var bts []byte
	var err error
	switch configType {
	case "json", "yml", "yaml", "xml":
		content := configs["content"]
		if content != nil {
			bts = []byte(content.(string))
		}
	case "properties":
		bts, err = marshalProperties(configs)
	}
	return bts, err
}

func (cm apolloConfigManager) Watch(namespace string, stop chan bool) <-chan *viper.RemoteResponse {
	resp := make(chan *viper.RemoteResponse)
	backendResp := cm.agollo.WatchNamespace(namespace, stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case r := <-backendResp:
				if r.Error != nil {
					resp <- &viper.RemoteResponse{
						Value: nil,
						Error: r.Error,
					}
					continue
				}

				configType := getConfigType(namespace)
				value, err := marshalConfigs(configType, r.NewValue)

				resp <- &viper.RemoteResponse{Value: value, Error: err}
			}
		}
	}()
	return resp
}
func (cm apolloConfigManager) WatchMultipleNamespaces(namespaces []string, stop chan bool) <-chan *viper.RemoteResponse {
	combinedResp := make(chan *viper.RemoteResponse)

	// 创建一个监听每个namespace变化的goroutine
	for _, namespace := range namespaces {
		go func(ns string) {
			backendResp := cm.agollo.WatchNamespace(ns, stop)
			for {
				select {
				case <-stop:
					return
				case r := <-backendResp:
					if r.Error != nil {
						combinedResp <- &viper.RemoteResponse{
							Value: nil,
							Error: r.Error,
						}
						continue
					}

					configType := getConfigType(ns)
					var mergedConfigs map[string]interface{}
					switch configType {
					case "json", "yml", "yaml", "xml":
						content := r.NewValue["content"]
						if content != nil {
							var tempConfig map[string]interface{}
							err := unmarshalConfig(configType, []byte(content.(string)), &tempConfig)
							if err != nil {
								combinedResp <- &viper.RemoteResponse{Value: nil, Error: err}
								continue
							}
							viper.MergeConfigMap(tempConfig)
							mergedConfigs = viper.AllSettings()
						}
					case "properties":
						mergedConfigs = r.NewValue
					}

					value, err := marshalConfigs(configType, mergedConfigs)
					combinedResp <- &viper.RemoteResponse{Value: value, Error: err}
				}
			}
		}(namespace)
	}

	return combinedResp
}

type configProvider struct {
}

func (rc configProvider) Get(rp viper.RemoteProvider) (io.Reader, error) {
	cmt, err := getConfigManager(rp)
	if err != nil {
		return nil, err
	}
	namespaces := strings.Split(rp.Path(), ",")
	var b []byte
	if len(namespaces) == 1 {
		switch cm := cmt.(type) {
		case viperConfigManager:
			b, err = cm.Get(namespaces[0])
		case crypt.ConfigManager:
			b, err = cm.Get(namespaces[0])
		}
	} else if len(namespaces) > 1 {
		switch cm := cmt.(type) {
		case viperConfigManager:
			b, err = cm.GetMultipleNamespaces(namespaces)
			if err != nil {
				return nil, err
			}
		}
	}

	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

func (rc configProvider) Watch(rp viper.RemoteProvider) (io.Reader, error) {
	cmt, err := getConfigManager(rp)
	if err != nil {
		return nil, err
	}
	namespaces := strings.Split(rp.Path(), ",")
	var resp []byte
	if len(namespaces) == 1 {
		switch cm := cmt.(type) {
		case viperConfigManager:
			resp, err = cm.Get(namespaces[0])
		case crypt.ConfigManager:
			resp, err = cm.Get(namespaces[0])
		}
	} else if len(namespaces) > 1 {
		switch cm := cmt.(type) {
		case viperConfigManager:
			resp, err = cm.GetMultipleNamespaces(namespaces)
		}
	}

	if err != nil {
		return nil, err
	}

	return bytes.NewReader(resp), nil
}

func (rc configProvider) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	cmt, err := getConfigManager(rp)
	if err != nil {
		return nil, nil
	}
	namespaces := strings.Split(rp.Path(), ",")
	if len(namespaces) > 1 {
		switch cm := cmt.(type) {
		case viperConfigManager:
			quitwc := make(chan bool)
			viperResponsCh := cm.WatchMultipleNamespaces(namespaces, quitwc)
			return viperResponsCh, quitwc
		}
	}
	switch cm := cmt.(type) {
	case viperConfigManager:
		quitwc := make(chan bool)
		viperResponsCh := cm.Watch(rp.Path(), quitwc)
		return viperResponsCh, quitwc
	default:
		ccm := cm.(crypt.ConfigManager)
		quit := make(chan bool)
		quitwc := make(chan bool)
		viperResponsCh := make(chan *viper.RemoteResponse)
		cryptoResponseCh := ccm.Watch(rp.Path(), quit)
		// need this function to convert the Channel response form crypt.Response to viper.Response
		go func(cr <-chan *crypt.Response, vr chan<- *viper.RemoteResponse, quitwc <-chan bool, quit chan<- bool) {
			for {
				select {
				case <-quitwc:
					quit <- true
					return
				case resp := <-cr:
					vr <- &viper.RemoteResponse{
						Error: resp.Error,
						Value: resp.Value,
					}

				}

			}
		}(cryptoResponseCh, viperResponsCh, quitwc, quit)

		return viperResponsCh, quitwc
	}
}

func getConfigManager(rp viper.RemoteProvider) (interface{}, error) {
	if rp.SecretKeyring() != "" {
		kr, err := os.Open(rp.SecretKeyring())
		if err != nil {
			return nil, err
		}
		defer kr.Close()

		switch rp.Provider() {
		case "etcd":
			return crypt.NewEtcdConfigManager([]string{rp.Endpoint()}, kr)
		case "consul":
			return crypt.NewConsulConfigManager([]string{rp.Endpoint()}, kr)
		case "apollo":
			return nil, errors.New("The Apollo configuration manager is not encrypted")
		default:
			return nil, ErrUnsupportedProvider
		}
	} else {
		switch rp.Provider() {
		case "etcd":
			return crypt.NewStandardEtcdConfigManager([]string{rp.Endpoint()})
		case "consul":
			return crypt.NewStandardConsulConfigManager([]string{rp.Endpoint()})
		case "apollo":
			return newApolloConfigManager(appID, rp.Endpoint(), defaultAgolloOptions)
		default:
			return nil, ErrUnsupportedProvider
		}
	}
}

// 配置文件有多种格式，例如：properties、xml、yml、yaml、json等。同样Namespace也具有这些格式。在Portal UI中可以看到“application”的Namespace上有一个“properties”标签，表明“application”是properties格式的。
// 如果使用Http接口直接调用时，对应的namespace参数需要传入namespace的名字加上后缀名，如datasources.json。
func getConfigType(namespace string) string {
	ext := filepath.Ext(namespace)

	if len(ext) > 1 {
		fileExt := ext[1:]
		// 还是要判断一下碰到，TEST.Namespace1
		// 会把Namespace1作为文件扩展名
		for _, e := range viper.SupportedExts {
			if e == fileExt {
				return fileExt
			}
		}
	}

	return defaultConfigType
}

func init() {
	viper.SupportedRemoteProviders = append(
		viper.SupportedRemoteProviders,
		"apollo",
	)
	viper.RemoteConfig = &configProvider{}
}
