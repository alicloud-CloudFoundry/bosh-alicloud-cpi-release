/*
 * Copyright (C) 2017-2019 Alibaba Group Holding Limited
 */
package alicloud

import (
	"bosh-alicloud-cpi/registry"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	bosherr "github.com/cloudfoundry/bosh-utils/errors"
	boshlog "github.com/cloudfoundry/bosh-utils/logger"
	boshsys "github.com/cloudfoundry/bosh-utils/system"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/endpoints"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/location"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/slb"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type InnerType string

type CloudConfigJson struct {
	Root CloudConfig `json:"cloud"`
}

type CloudConfig struct {
	Plugin     string `json:"plugin"`
	Properties Config `json:"properties"`
}

type Config struct {
	OpenApi  OpenApi        `json:"alicloud"`
	Registry RegistryConfig `json:"registry"`
	Agent    AgentConfig    `json:"agent"`
}

const (
	UseForceStop = true

	WaitTimeout  = time.Duration(600) * time.Second
	WaitInterval = time.Duration(5) * time.Second

	DefaultEipWaitSeconds       = 120
	DefaultSlbWeight            = 100
	BoshCPI                     = "Bosh-Alicloud-Cpi"
	BoshCPIVersion              = "1.0"
	DefaultSlbServerGroupPort   = 33333
	DefaultSlbServerGroupWeight = 100
)

type OpenApi struct {
	Region           string `json:"region"`
	AvailabilityZone string `json:"availability_zone"`
	AccessEndpoint   string `json:"access_endpoint"`
	AccessKeyId      string `json:"access_key_id"`
	AccessKeySecret  string `json:"access_key_secret"`
	SecurityToken    string `json:"security_token"`
	Encrypted        *bool  `json:"encrypted,omitempty"`
	KmsKeyId         string `json:"kms_key_id"`
	EcsEndpoint      string `json:"ecs_endpoint"`
	SlbEndpoint      string `json:"slb_endpoint"`
	OssEndpoint      string `json:"oss_endpoint"`
	LocationEndpoint string `json:"location_endpoint"`
}

type RegistryConfig struct {
	User     string      `json:"user"`
	Password string      `json:"password"`
	Protocol string      `json:"protocol"`
	Host     string      `json:"host"`
	Port     json.Number `json:"port"`
}

type AgentConfig struct {
	Ntp       []string        `json:"ntp"`
	Mbus      string          `json:"mbus"`
	Blobstore BlobstoreConfig `json:"blobstore"`
}

type BlobstoreConfig struct {
	Provider string                 `json:"provider"`
	Options  map[string]interface{} `json:"options"`
}

func (c Config) Validate() error {
	if c.OpenApi.GetRegion("") == "" {
		return fmt.Errorf("region can't be empty")
	}

	_, err := c.Registry.Port.Int64()
	if err != nil {
		return fmt.Errorf("bad registry.port %s", c.Registry.Port.String())
	}

	//TODO: validate more
	return nil
}

func (a OpenApi) GetRegion(region string) string {
	if region != "" {
		return region
	}
	return a.Region
}

func (a OpenApi) GetAvailabilityZone() string {
	return a.AvailabilityZone
}

func (a RegistryConfig) IsEmpty() bool {
	if a.Host == "" {
		return true
	} else {
		return false
	}
}

func NewConfigFromFile(configFile string, fs boshsys.FileSystem) (Config, error) {
	var config Config

	if configFile == "" {
		return config, bosherr.Errorf("Must provide a config file")
	}

	bytes, err := fs.ReadFile(configFile)
	if err != nil {
		return config, bosherr.WrapErrorf(err, "Reading config file '%s'", configFile)
	}

	return NewConfigFromBytes(bytes)
}

func NewConfigFromBytes(bytes []byte) (Config, error) {
	var ccs CloudConfigJson
	var config Config

	err := json.Unmarshal(bytes, &ccs)
	if err != nil {
		return config, bosherr.WrapError(err, "unmarshal config json failed")
	}

	config = ccs.Root.Properties

	err = config.Validate()
	if err != nil {
		return config, bosherr.WrapError(err, "validate config failed")
	}

	return config, nil
}

func (a RegistryConfig) ToInstanceUserData() string {
	endpoint := a.GetEndpoint()
	json := fmt.Sprintf(`{"registry":{"endpoint":"%s"}}`, endpoint)
	return json
}

func (a RegistryConfig) GetEndpoint() string {
	port, _ := a.Port.Int64()
	return fmt.Sprintf("%s://%s:%s@%s:%d", a.Protocol, a.User, a.Password, a.Host, port)
}

func (a BlobstoreConfig) AsRegistrySettings() registry.BlobstoreSettings {
	return registry.BlobstoreSettings{
		Provider: a.Provider,
		Options:  a.Options,
	}
}

func (c Config) NewEcsClient(region string) (*ecs.Client, error) {
	var mutex = sync.RWMutex{}
	mutex.Lock()
	defer mutex.Unlock()

	endpoint := strings.TrimSpace(c.OpenApi.EcsEndpoint)
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("ECS_ENDPOINT"))
	}
	if endpoint != "" {
		endpoints.AddEndpointMapping(c.OpenApi.Region, "ecs", endpoint)
	}
	client, err := ecs.NewClientWithOptions(c.OpenApi.GetRegion(region), getSdkConfig().WithTimeout(time.Duration(90)*time.Second), c.getAuthCredential(true))
	client.AppendUserAgent(BoshCPI, BoshCPIVersion)
	if err != nil {
		return nil, bosherr.WrapErrorf(err, "Initiating ECS Client in '%s' got an error.", c.OpenApi.GetRegion(region))
	}
	return client, nil
}

func (c Config) NewSlbClient(region string) (*slb.Client, error) {
	var mutex = sync.RWMutex{}
	mutex.Lock()
	defer mutex.Unlock()

	endpoint := strings.TrimSpace(c.OpenApi.SlbEndpoint)
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("SLB_ENDPOINT"))
	}
	if endpoint != "" {
		endpoints.AddEndpointMapping(c.OpenApi.Region, "slb", endpoint)
	}
	client, err := slb.NewClientWithOptions(c.OpenApi.GetRegion(region), getSdkConfig().WithTimeout(time.Duration(30)*time.Second), c.getAuthCredential(true))
	client.AppendUserAgent(BoshCPI, BoshCPIVersion)
	if err != nil {
		return nil, bosherr.WrapErrorf(err, "Initiating SLB Client in '%s' got an error.", c.OpenApi.GetRegion(region))
	}
	return client, nil
}

func (c Config) GetRegistryClient(logger boshlog.Logger) RegistryManager {
	//if !c.Registry.IsEmpty() {
	//	return c.GetHttpRegistryClient(logger)
	//} else {
	return NewRegistryManager(c, logger)
	//}
}

func (c Config) NewOssClient(region string) (*oss.Client, error) {
	var mutex = sync.RWMutex{}
	mutex.Lock()
	defer mutex.Unlock()

	endpoint := strings.TrimSpace(c.OpenApi.OssEndpoint)
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("OSS_ENDPOINT"))
	}
	schma := "https"

	if endpoint == "" {
		if strings.TrimSpace(region) == "" {
			region = c.OpenApi.Region
		}
		endpointItem, _ := c.describeEndpointForService("oss")
		if endpointItem != nil && len(endpointItem.Endpoint) > 0 {
			if len(endpointItem.Protocols.Protocols) > 0 {
				// HTTP or HTTPS
				schma = strings.ToLower(endpointItem.Protocols.Protocols[0])
				for _, p := range endpointItem.Protocols.Protocols {
					if strings.ToLower(p) == "https" {
						schma = strings.ToLower(p)
						break
					}
				}
			}
			endpoint = endpointItem.Endpoint
		} else {
			endpoint = fmt.Sprintf("oss-%s-internal.aliyuncs.com", c.OpenApi.Region)
		}
	}
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = fmt.Sprintf("%s://%s", schma, endpoint)
	}

	clientOptions := []oss.ClientOption{oss.UserAgent(BoshCPI + "/" + BoshCPIVersion),
		oss.SecurityToken(c.OpenApi.SecurityToken)}
	ossClient, err := oss.New(endpoint, c.OpenApi.AccessKeyId, c.OpenApi.AccessKeySecret, clientOptions...)
	if err != nil {
		return nil, bosherr.WrapErrorf(err, "Initiating OSS Client in '%s' got an error.", c.OpenApi.GetRegion(region))
	}
	return ossClient, nil
}

func (c Config) GetHttpRegistryClient(logger boshlog.Logger) registry.Client {
	r := c.Registry

	port, _ := r.Port.Int64()
	clientOptions := registry.ClientOptions{
		Protocol: r.Protocol,
		Host:     r.Host,
		Port:     int(port),
		Username: r.User,
		Password: r.Password,
	}

	client := registry.NewHTTPClient(clientOptions, logger)
	return client
}

func (c Config) getAuthCredential(stsSupported bool) auth.Credential {
	if stsSupported {
		return credentials.NewStsTokenCredential(c.OpenApi.AccessKeyId, c.OpenApi.AccessKeySecret, c.OpenApi.SecurityToken)
	}

	return credentials.NewAccessKeyCredential(c.OpenApi.AccessKeyId, c.OpenApi.AccessKeySecret)
}

func (c Config) GetInstanceRegion(instanceId string) (region string, err error) {
	client, err := c.NewEcsClient("")
	if err != nil {
		return
	}

	args := ecs.CreateDescribeInstanceAttributeRequest()
	args.InstanceId = instanceId

	invoker := NewInvoker()
	err = invoker.Run(func() error {
		inst, err := client.DescribeInstanceAttribute(args)
		if err != nil {
			return bosherr.WrapErrorf(err, "Describe Instance %s Attribute in '%s' got an error.", instanceId, c.OpenApi.GetRegion(region))
		}
		if inst != nil {
			region = inst.RegionId
		}
		return nil
	})
	return
}

func (c Config) GetCrossRegions() (regions []string, err error) {
	regionMap := make(map[string]string)
	regionstr := os.Getenv("CROSS_REGIONS")
	if len(strings.TrimSpace(regionstr)) > 0 {
		for _, r := range strings.Split(strings.TrimSpace(regionstr), ",") {
			r = strings.TrimSpace(r)
			if r == c.OpenApi.GetRegion("") {
				continue
			}
			if _, ok := regionMap[r]; ok {
				continue
			}
			regions = append(regions, r)
			regionMap[r] = r
		}
	}

	client, err := c.NewEcsClient("")
	if err != nil {
		return
	}

	invoker := NewInvoker()
	err = invoker.Run(func() error {
		resp, err := client.DescribeRegions(ecs.CreateDescribeRegionsRequest())
		if err != nil {
			return bosherr.WrapErrorf(err, "Describe Regions got an error.")
		}
		if resp != nil && len(resp.Regions.Region) > 0 {
			for _, r := range resp.Regions.Region {
				if r.RegionId == c.OpenApi.GetRegion("") {
					continue
				}
				if strings.HasPrefix(r.RegionId, "cn-") {
					if _, ok := regionMap[r.RegionId]; ok {
						continue
					}
					regions = append(regions, r.RegionId)
					regionMap[r.RegionId] = r.RegionId
				}
			}
		}
		return nil
	})
	return
}

func (c Config) describeEndpointForService(serviceCode string) (*location.DescribeEndpointResponse, error) {
	args := location.CreateDescribeEndpointRequest()
	args.ServiceCode = serviceCode
	args.Id = c.OpenApi.Region
	args.Domain = strings.TrimSpace(c.OpenApi.LocationEndpoint)
	if args.Domain == "" {
		args.Domain = strings.TrimSpace(os.Getenv("LOCATION_ENDPOINT"))
		if args.Domain == "" {
			args.Domain = "location-readonly.aliyuncs.com"
		}
	}

	locationClient, err := location.NewClientWithOptions(c.OpenApi.Region, getSdkConfig(), c.getAuthCredential(true))
	locationClient.AppendUserAgent(BoshCPI, BoshCPIVersion)
	if err != nil {
		return nil, fmt.Errorf("Unable to initialize the location client: %#v", err)

	}
	endpointsResponse, err := locationClient.DescribeEndpoint(args)
	if err != nil {
		return nil, fmt.Errorf("Describe %s endpoint using region: %#v got an error: %#v.", serviceCode, c.OpenApi.Region, err)
	}
	return endpointsResponse, nil
}
